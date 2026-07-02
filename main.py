# main.py
# ------------------------------------------------------------
# ✅ 권장 설치 (Koyeb/Windows 공통)
#   python -m pip install -U discord.py tzdata aiohttp
#
# ✅ 실행
#   python main.py
#
# ✅ 디스코드에서 사용(권장 순서)
# 1) !설치
# 2) !로그채널설정 #study-log        (채널 멘션으로 입력 권장)
# 3) (선택) !정산채널설정 #ranking   (자동 주간정산이 나갈 채널 지정)
# 4) (선택) !패널복구               (패널 메시지ID가 날아갔을 때, 현재 채널에서 찾아 재등록)
#
# ✅ 자동 기능
# - (1) 일요일 KST 12:00 자동 주간정산
#       📌 안내 메시지 → 랭킹 출력 → 초기화 완료 메시지
#       (정산 결과는 로그 채널에도 함께 남김)
# - (2) 현황판 조건부 갱신: 활동(work/break) 있으면 1분, 없으면 5분
# - (3) 버튼(출근/휴식/복귀/퇴근) 누를 때마다 즉시 현황판 업데이트
#
# ✅ 안정성 패치 핵심
# - Koyeb Health 서버를 Discord 로그인/ready보다 먼저 기동 (setup_hook)
# - Deadlock 제거: 락 잡은 상태에서 재락(save_now) 호출 금지
# - Interaction 응답 표준화: 버튼 콜백 시작 즉시 defer + 예외(Unknown interaction) 방어
# - 응답 후 작업 분리: 로그 전송/현황판 수정은 create_task로 분리
#
# ✅ 이벤트 소싱(로그=진짜 데이터) 옵션
# - 로그 채널에 [STUDYLOG] 라인으로 모든 이벤트를 기록
# - !리플레이 : "마지막 weekly_reset 이후부터" 로그만 읽어 상태를 재구성
#   (로그 채널 권한이 있어야 합니다: 읽기 메시지 기록/읽기)
# ------------------------------------------------------------

import os
import json
import asyncio
import hashlib
import uuid
from datetime import datetime, timedelta, date, timezone, time
from typing import Dict, Any, Optional, Tuple, List

import discord
from discord.ext import commands, tasks

import aiohttp
from aiohttp import web

from zoneinfo import ZoneInfo


# ------------------------------------------------------------
# ✅ 토큰 입력란 (요청대로 빈칸 유지)
#    실제 운영은 환경변수 DISCORD_TOKEN 사용 권장
# ------------------------------------------------------------
TOKEN = ""

DATA_FILE = "study_data.json"
LOG_PREFIX = "[STUDYLOG]"

BOOT_ID = str(uuid.uuid4())[:8]

# ✅ KST (Windows에서 tzdata 없으면 실패할 수 있어 안전장치 포함)
try:
    KST = ZoneInfo("Asia/Seoul")
except Exception:
    KST = timezone(timedelta(hours=9), name="KST")


# ------------------------------------------------------------
# ✅ 디스코드 봇 기본 설정
# ------------------------------------------------------------
INTENTS = discord.Intents.default()
INTENTS.message_content = True  # 명령어를 쓸 거라면 필요
INTENTS.voice_states = True


def now_kst() -> datetime:
    return datetime.now(tz=KST)


def dt_to_iso(dt: datetime) -> str:
    return dt.astimezone(KST).isoformat()


def iso_to_dt(iso_str: Optional[str]) -> Optional[datetime]:
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except Exception:
        return None


def week_start_kst(d: date) -> date:
    # 월요일 시작
    return d - timedelta(days=d.weekday())


def fmt_hhmm(seconds: int) -> str:
    seconds = max(int(seconds), 0)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h}시간 {m}분"


TIER_LABELS = {
    "unranked": "⚪ 언랭",
    "bronze": "🥉 브론즈",
    "silver": "🥈 실버",
    "gold": "🥇 골드",
    "diamond": "💎 다이아",
    "challenger": "🏆 챌린저",
}


def tier_key_from_weekly(weekly_sec: int) -> str:
    hours = weekly_sec / 3600.0
    if hours < 10:
        return "unranked"
    if hours < 20:
        return "bronze"
    if hours < 30:
        return "silver"
    if hours < 40:
        return "gold"
    if hours < 52:
        return "diamond"
    return "challenger"


def tier_from_weekly(weekly_sec: int) -> str:
    return TIER_LABELS[tier_key_from_weekly(weekly_sec)]


def status_label(status: str) -> str:
    if status == "work":
        return "공부 중"
    if status == "break":
        return "휴식 중"
    return "대기 중"


def safe_str(v: Any) -> str:
    return str(v).replace("\n", " ").replace(";", ",").strip()


# ------------------------------------------------------------
# ✅ 데이터 저장소(Deadlock-free)
# - store.lock 잡은 상태에서 save_now() 호출 금지 (재락 위험)
# - 락을 이미 잡았으면 save_now_locked()만 호출
# ------------------------------------------------------------
class DataStore:
    def __init__(self, path: str):
        self.path = path
        self.lock = asyncio.Lock()
        self.data: Dict[str, Any] = {"version": 2, "guilds": {}}

    def _ensure_file(self):
        if not os.path.exists(self.path):
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)

    def _load_sync(self):
        self._ensure_file()
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
        except Exception as e:
            backup = f"{self.path}.corrupt.{datetime.now().strftime('%Y%m%d%H%M%S')}"
            try:
                if os.path.exists(self.path):
                    os.replace(self.path, backup)
                    print(f"[DATA] Failed to load {self.path}; moved corrupt file to {backup}: {e}")
            except Exception as backup_error:
                print(f"[DATA] Failed to load {self.path} and could not back it up: {backup_error}")
            self.data = {"version": 2, "guilds": {}}

        if "guilds" not in self.data:
            self.data["guilds"] = {}

    def _atomic_save_sync(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    async def load_once(self):
        async with self.lock:
            self._load_sync()

    async def save_now(self):
        async with self.lock:
            self._atomic_save_sync()

    def save_now_locked(self):
        self._atomic_save_sync()


store = DataStore(DATA_FILE)

# aiohttp 세션(keepalive/ping용)
http_session: Optional[aiohttp.ClientSession] = None


# ------------------------------------------------------------
# ✅ 길드/유저 구조 보장
# ------------------------------------------------------------
def ensure_guild(data: Dict[str, Any], guild_id: int) -> Dict[str, Any]:
    gid = str(guild_id)
    g = data["guilds"].get(gid)

    if not g:
        today = now_kst().date()
        g = {
            "week_start": week_start_kst(today).isoformat(),
            "panel": {"channel_id": None, "message_id": None},
            "log_channel_id": None,
            "settlement_channel_id": None,
            "last_settlement_week_start": None,
            "users": {},
            "dashboard_hash": None,
            # ✅ 이벤트 소싱 최적화: 마지막 weekly_reset 로그 메시지 ID
            "last_weekly_reset_log_id": None,
            "monitored_voice_channel_ids": [],
            "call_alert_last_at": None,
            "break_alerts": {},
            "long_session_alerts": {},
            "midnight_alerts": {},
        }
        data["guilds"][gid] = g

    # 기본 키 보정
    g.setdefault("panel", {"channel_id": None, "message_id": None})
    g.setdefault("log_channel_id", None)
    g.setdefault("settlement_channel_id", None)
    g.setdefault("last_settlement_week_start", None)
    g.setdefault("users", {})
    g.setdefault("week_start", week_start_kst(now_kst().date()).isoformat())
    g.setdefault("dashboard_hash", None)
    g.setdefault("last_weekly_reset_log_id", None)
    g.setdefault("monitored_voice_channel_ids", [])
    g.setdefault("call_alert_last_at", None)
    g.setdefault("break_alerts", {})
    g.setdefault("long_session_alerts", {})
    g.setdefault("midnight_alerts", {})

    return g


def ensure_week_current(guild_data: Dict[str, Any]) -> bool:
    """현재 주와 저장된 주가 다른지 확인한다. 초기화는 주간 정산에서만 수행한다."""
    today = now_kst().date()
    current = week_start_kst(today).isoformat()
    return guild_data.get("week_start") != current


def ensure_user(guild_data: Dict[str, Any], member: discord.Member) -> Dict[str, Any]:
    uid = str(member.id)
    users = guild_data["users"]
    u = users.get(uid)
    if not u:
        u = {
            "name": member.display_name,
            "status": "off",
            "start_time": None,
            "break_start": None,
            "total_break_today": 0,
            "weekly_total_sec": 0,
            "streak": 0,
            "last_work_date": None,
            "daily_sec": {},
            "daily_break_sec": {},
            "lifetime_total_sec": 0,
            "best_streak": 0,
            "tier_counts": {k: 0 for k in TIER_LABELS},
        }
        users[uid] = u
    else:
        u["name"] = member.display_name
        u.setdefault("status", "off")
        u.setdefault("start_time", None)
        u.setdefault("break_start", None)
        u.setdefault("total_break_today", 0)
        u.setdefault("weekly_total_sec", 0)
        u.setdefault("streak", 0)
        u.setdefault("last_work_date", None)
        u.setdefault("daily_sec", {})
        u.setdefault("daily_break_sec", {})
        u.setdefault("lifetime_total_sec", sum(int(v) for v in u.get("daily_sec", {}).values()))
        u.setdefault("best_streak", int(u.get("streak", 0)))
        counts = u.setdefault("tier_counts", {})
        for k in TIER_LABELS:
            counts.setdefault(k, 0)
    return u


# ------------------------------------------------------------
# ✅ 계산 로직
# ------------------------------------------------------------
def calc_effective_study_sec(user: Dict[str, Any], now: datetime) -> int:
    start = iso_to_dt(user.get("start_time"))
    if not start:
        return 0

    total_break = int(user.get("total_break_today", 0))

    if user.get("status") == "break":
        bs = iso_to_dt(user.get("break_start"))
        if bs:
            total_break += int((now - bs).total_seconds())

    total = int((now - start).total_seconds()) - total_break
    return max(total, 0)


def current_break_sec(user: Dict[str, Any], now: datetime) -> int:
    total_break = int(user.get("total_break_today", 0))
    if user.get("status") == "break":
        bs = iso_to_dt(user.get("break_start"))
        if bs:
            total_break += max(int((now - bs).total_seconds()), 0)
    return max(total_break, 0)


def add_recorded_study_sec(user: Dict[str, Any], day_s: str, seconds: int):
    seconds = max(int(seconds), 0)
    if seconds <= 0:
        return
    daily = user.setdefault("daily_sec", {})
    daily[day_s] = int(daily.get(day_s, 0)) + seconds
    user["lifetime_total_sec"] = int(user.get("lifetime_total_sec", 0)) + seconds


def add_recorded_break_sec(user: Dict[str, Any], day_s: str, seconds: int):
    seconds = max(int(seconds), 0)
    if seconds <= 0:
        return
    daily = user.setdefault("daily_break_sec", {})
    daily[day_s] = int(daily.get(day_s, 0)) + seconds


def current_session_sec_for_day(user: Dict[str, Any], now: datetime, day_s: str) -> int:
    start = iso_to_dt(user.get("start_time"))
    if not start or start.date().isoformat() != day_s:
        return 0
    return calc_effective_study_sec(user, now)


def current_break_sec_for_day(user: Dict[str, Any], now: datetime, day_s: str) -> int:
    start = iso_to_dt(user.get("start_time"))
    if not start or start.date().isoformat() != day_s:
        return 0
    return current_break_sec(user, now)


def rollover_active_sessions(guild_data: Dict[str, Any], now: datetime):
    """자정이 지난 활성 세션을 전날 기록으로 확정하고 현재 상태를 새 날짜로 이어간다."""
    today = now.date()
    for u in guild_data.get("users", {}).values():
        if u.get("status") not in ("work", "break"):
            continue
        start = iso_to_dt(u.get("start_time"))
        if not start:
            continue

        while start.date() < today:
            boundary = datetime.combine(start.date() + timedelta(days=1), time.min, tzinfo=KST)
            add_recorded_study_sec(u, start.date().isoformat(), calc_effective_study_sec(u, boundary))
            add_recorded_break_sec(u, start.date().isoformat(), current_break_sec(u, boundary))

            boundary_iso = dt_to_iso(boundary)
            u["start_time"] = boundary_iso
            u["total_break_today"] = 0
            if u.get("status") == "break":
                u["break_start"] = boundary_iso
            else:
                u["break_start"] = None
            start = boundary


def week_daily_seconds(user: Dict[str, Any], now: datetime) -> List[Tuple[str, int]]:
    daily = user.get("daily_sec", {})
    start = week_start_kst(now.date())
    out: List[Tuple[str, int]] = []
    today_s = now.date().isoformat()
    for i in range(7):
        d = start + timedelta(days=i)
        day_s = d.isoformat()
        sec = int(daily.get(day_s, 0))
        if day_s == today_s:
            sec += current_session_sec_for_day(user, now, day_s)
        out.append((day_s, sec))
    return out


def month_total_seconds(user: Dict[str, Any], now: datetime) -> int:
    prefix = now.strftime("%Y-%m")
    total = sum(int(sec) for day, sec in user.get("daily_sec", {}).items() if day.startswith(prefix))
    total += current_session_sec_for_day(user, now, now.date().isoformat())
    return total


def lifetime_total_seconds(user: Dict[str, Any], now: datetime) -> int:
    return int(user.get("lifetime_total_sec", 0)) + current_session_sec_for_day(user, now, now.date().isoformat())


WEEKDAY_LABELS = ["월", "화", "수", "목", "금", "토", "일"]


def build_today_summary_text(user: Dict[str, Any], display_name: str, now: datetime) -> str:
    today_s = now.date().isoformat()
    study_sec = int(user.get("daily_sec", {}).get(today_s, 0)) + current_session_sec_for_day(user, now, today_s)
    break_sec = int(user.get("daily_break_sec", {}).get(today_s, 0)) + current_break_sec_for_day(user, now, today_s)
    start = iso_to_dt(user.get("start_time"))
    start_text = start.strftime("%H:%M") if start and start.date() == now.date() else "-"
    current_session = current_session_sec_for_day(user, now, today_s)

    return (
        f"**이름:** {display_name}\n"
        f"**현재 상태:** {status_label(user.get('status', 'off'))}\n"
        f"**오늘 공부:** {fmt_hhmm(study_sec)}\n"
        f"**오늘 휴식:** {fmt_hhmm(break_sec)}\n"
        f"**오늘 시작:** {start_text}\n"
        f"**현재 세션:** {fmt_hhmm(current_session)}"
    )


def build_weekly_info_text(user: Dict[str, Any], display_name: str, now: datetime) -> str:
    rows = week_daily_seconds(user, now)
    total = sum(sec for _, sec in rows)
    lines = []
    for idx, (day_s, sec) in enumerate(rows):
        lines.append(f"{WEEKDAY_LABELS[idx]}: {fmt_hhmm(sec)}")

    return (
        f"**{display_name}님의 주간 정보**\n"
        f"**이번 주 총합:** {fmt_hhmm(total)}\n"
        f"**현재 티어:** {tier_from_weekly(total)}\n"
        + "\n".join(lines)
    )


def build_total_info_text(user: Dict[str, Any], display_name: str, now: datetime) -> str:
    counts = user.setdefault("tier_counts", {})
    for k in TIER_LABELS:
        counts.setdefault(k, 0)
    count_text = " / ".join(f"{TIER_LABELS[k]} {int(counts.get(k, 0))}회" for k in TIER_LABELS)

    return (
        f"**{display_name}님의 통합 정보**\n"
        f"**이번달 총 시간:** {fmt_hhmm(month_total_seconds(user, now))}\n"
        f"**전체 누적 시간:** {fmt_hhmm(lifetime_total_seconds(user, now))}\n"
        f"**현재 연속 출근:** 🔥 {int(user.get('streak', 0))}일\n"
        f"**최고 연속 출근:** 🏅 {int(user.get('best_streak', 0))}일\n"
        f"**누적 랭킹 횟수:**\n{count_text}"
    )


def has_any_activity(guild_data: Dict[str, Any]) -> bool:
    for u in guild_data.get("users", {}).values():
        if u.get("status") in ("work", "break"):
            return True
    return False


# ------------------------------------------------------------
# ✅ 로그(이벤트 소싱) - 문자열 포맷
# ------------------------------------------------------------
def make_log(action: str, member: discord.Member, ts: datetime, **fields) -> str:
    base = {
        "action": action,
        "uid": str(member.id),
        "name": safe_str(member.display_name),
        "ts": dt_to_iso(ts),
    }
    for k, v in fields.items():
        base[k] = safe_str(v)
    parts = [f"{k}={base[k]}" for k in base]
    return f"{LOG_PREFIX} " + "; ".join(parts)


def make_system_log(action: str, ts: datetime, **fields) -> str:
    base = {
        "action": action,
        "uid": "SYSTEM",
        "name": "SYSTEM",
        "ts": dt_to_iso(ts),
    }
    for k, v in fields.items():
        base[k] = safe_str(v)
    parts = [f"{k}={base[k]}" for k in base]
    return f"{LOG_PREFIX} " + "; ".join(parts)


def parse_log_line(line: str) -> Optional[Dict[str, str]]:
    """[STUDYLOG] k=v; k=v 형태 파싱"""
    if not line.startswith(LOG_PREFIX):
        return None
    try:
        payload = line[len(LOG_PREFIX):].strip()
        parts = [p.strip() for p in payload.split(";")]
        out: Dict[str, str] = {}
        for p in parts:
            if "=" not in p:
                continue
            k, v = p.split("=", 1)
            out[k.strip()] = v.strip()
        return out if out.get("action") else None
    except Exception:
        return None


async def send_to_channel(channel: Optional[discord.TextChannel], content: str):
    if not channel:
        return
    try:
        await channel.send(content)
    except Exception:
        pass


async def send_log_text(guild: discord.Guild, guild_data: Dict[str, Any], text: str) -> Optional[int]:
    """로그 채널에 기록하고, 메시지 ID를 반환(가능하면)."""
    ch_id = guild_data.get("log_channel_id")
    if not ch_id:
        return None
    ch = guild.get_channel(int(ch_id))
    if not isinstance(ch, discord.TextChannel):
        return None
    try:
        msg = await ch.send(text)
        return msg.id
    except Exception:
        return None


async def send_settlement_message_both(
    guild: discord.Guild,
    guild_data: Dict[str, Any],
    settlement_channel: discord.TextChannel,
    content: str
):
    # 정산 채널
    await send_to_channel(settlement_channel, content)

    # 로그 채널(중복 방지)
    log_id = guild_data.get("log_channel_id")
    if log_id and int(log_id) != settlement_channel.id:
        log_ch = guild.get_channel(int(log_id))
        if isinstance(log_ch, discord.TextChannel):
            await send_to_channel(log_ch, content)


async def send_alert_text(guild: discord.Guild, guild_data: Dict[str, Any], content: str):
    channel: Optional[discord.TextChannel] = None

    log_id = guild_data.get("log_channel_id")
    if log_id:
        ch = guild.get_channel(int(log_id))
        if isinstance(ch, discord.TextChannel):
            channel = ch

    if not channel:
        panel = guild_data.get("panel", {})
        ch_id = panel.get("channel_id")
        if ch_id:
            ch = guild.get_channel(int(ch_id))
            if isinstance(ch, discord.TextChannel):
                channel = ch

    if not channel:
        channel = get_settlement_channel(guild, guild_data)

    await send_to_channel(channel, content)


# ------------------------------------------------------------
# ✅ 대시보드(현황판) - edit 최소화(해시 비교)
# ------------------------------------------------------------
def build_dashboard_text(guild_data: Dict[str, Any]) -> str:
    now = now_kst()
    work_lines: List[str] = []
    break_lines: List[str] = []

    for u in guild_data.get("users", {}).values():
        st = u.get("status", "off")
        name = u.get("name", "알 수 없음")
        if st == "work":
            sec = calc_effective_study_sec(u, now)
            # ✅ 줄바꿈 적용
            work_lines.append(f"🟢 {name} ({fmt_hhmm(sec)}째)")
        elif st == "break":
            break_lines.append(f"🟡 {name} (휴식 중)")

    lines = work_lines + break_lines
    if not lines:
        return "지금 공부 중인 사람이 없습니다.\n\n버튼으로 출근해서 스터디를 시작해 보세요."
    return "\n".join(lines)


def dashboard_hash(description: str) -> str:
    return hashlib.sha256(description.encode("utf-8")).hexdigest()


def build_dashboard_embed(
    guild: discord.Guild,
    guild_data: Dict[str, Any],
    last_actor: Optional[discord.Member] = None
) -> discord.Embed:
    now = now_kst()
    desc = build_dashboard_text(guild_data)

    embed = discord.Embed(
        title="📅 스터디 현황판",
        description=desc,
        color=discord.Color.blurple(),
        timestamp=now
    )

    if last_actor:
        u = ensure_user(guild_data, last_actor)
        embed.set_footer(
            text=f"최근 조작: {u.get('name', last_actor.display_name)} · 내 상태: {status_label(u.get('status','off'))} · 기준시간: KST"
        )
    else:
        embed.set_footer(text="상태 확인: [📌 오늘 요약]/[📅 주간 정보]/[🏅 통합 정보] 버튼 · 기준시간: KST")

    return embed


async def fetch_panel_message(guild: discord.Guild, guild_data: Dict[str, Any]) -> Optional[discord.Message]:
    panel = guild_data.get("panel", {})
    ch_id = panel.get("channel_id")
    msg_id = panel.get("message_id")
    if not ch_id or not msg_id:
        return None

    ch = guild.get_channel(int(ch_id))
    if not isinstance(ch, discord.TextChannel):
        return None

    try:
        return await ch.fetch_message(int(msg_id))
    except Exception:
        return None


async def update_dashboard(
    guild: discord.Guild,
    guild_data: Dict[str, Any],
    last_actor: Optional[discord.Member] = None,
    force: bool = False
):
    """현황판 임베드 갱신 (해시 동일하면 edit 생략)"""
    msg = await fetch_panel_message(guild, guild_data)
    if not msg:
        return

    desc = build_dashboard_text(guild_data)
    h = dashboard_hash(desc)
    if (not force) and guild_data.get("dashboard_hash") == h:
        return

    guild_data["dashboard_hash"] = h
    embed = build_dashboard_embed(guild, guild_data, last_actor=last_actor)

    try:
        # ✅ persistent view 재부착 (재시작 후 버튼 먹통 방지)
        await msg.edit(embed=embed, view=StudyView())
    except Exception:
        pass


# ------------------------------------------------------------
# ✅ 권한 체크(관리자)
# ------------------------------------------------------------
def is_admin_member(member: discord.Member) -> bool:
    perms = member.guild_permissions
    return perms.administrator or perms.manage_guild


def is_admin_ctx(ctx: commands.Context) -> bool:
    return bool(ctx.guild and isinstance(ctx.author, discord.Member) and is_admin_member(ctx.author))


# ------------------------------------------------------------
# ✅ 채널 파서
# ------------------------------------------------------------
def resolve_text_channel(guild: discord.Guild, raw: str) -> Optional[discord.TextChannel]:
    raw = raw.strip()

    # <#id>
    if raw.startswith("<#") and raw.endswith(">"):
        cid = raw[2:-1]
        if cid.isdigit():
            ch = guild.get_channel(int(cid))
            return ch if isinstance(ch, discord.TextChannel) else None

    # 숫자 ID
    if raw.isdigit():
        ch = guild.get_channel(int(raw))
        return ch if isinstance(ch, discord.TextChannel) else None

    # 이름
    name = raw.lstrip("#")
    for ch in guild.text_channels:
        if ch.name == name:
            return ch

    return None


def resolve_voice_channel(guild: discord.Guild, raw: str) -> Optional[discord.VoiceChannel]:
    raw = raw.strip()
    if raw.startswith("<#") and raw.endswith(">"):
        cid = raw[2:-1]
        if cid.isdigit():
            ch = guild.get_channel(int(cid))
            return ch if isinstance(ch, discord.VoiceChannel) else None
    if raw.isdigit():
        ch = guild.get_channel(int(raw))
        return ch if isinstance(ch, discord.VoiceChannel) else None
    name = raw.lstrip("#")
    for ch in guild.voice_channels:
        if ch.name == name:
            return ch
    return None


def resolve_member_target(guild: discord.Guild, raw: str) -> Optional[discord.Member]:
    raw = raw.strip()
    if raw.startswith("<@") and raw.endswith(">"):
        uid = raw[2:-1].lstrip("!")
        if uid.isdigit():
            member = guild.get_member(int(uid))
            if member:
                return member
    if raw.isdigit():
        member = guild.get_member(int(raw))
        if member:
            return member

    lowered = raw.lower()
    for member in guild.members:
        if member.bot:
            continue
        names = [member.name, member.display_name, str(member)]
        if any(name.lower() == lowered for name in names):
            return member
    return None


def get_settlement_channel(guild: discord.Guild, guild_data: Dict[str, Any]) -> Optional[discord.TextChannel]:
    # 1) 지정
    cid = guild_data.get("settlement_channel_id")
    if cid:
        ch = guild.get_channel(int(cid))
        if isinstance(ch, discord.TextChannel):
            return ch

    # 2) 패널 채널
    panel = guild_data.get("panel", {})
    if panel.get("channel_id"):
        ch = guild.get_channel(int(panel["channel_id"]))
        if isinstance(ch, discord.TextChannel):
            return ch

    # 3) 로그 채널
    log_id = guild_data.get("log_channel_id")
    if log_id:
        ch = guild.get_channel(int(log_id))
        if isinstance(ch, discord.TextChannel):
            return ch

    # 4) fallback
    return guild.text_channels[0] if guild.text_channels else None


# ------------------------------------------------------------
# ✅ 주간정산 메시지 생성/실행
# ------------------------------------------------------------
def build_weekly_ranking_lines(guild_data: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    users = list(guild_data.get("users", {}).values())
    users.sort(key=lambda u: int(u.get("weekly_total_sec", 0)), reverse=True)

    if not users or all(int(u.get("weekly_total_sec", 0)) == 0 for u in users):
        return ("이번 주 누적 기록이 없습니다. (초기화 완료)", None)

    top_sec = max(int(users[0].get("weekly_total_sec", 0)), 1)

    lines: List[str] = []
    rank = 1
    for u in users:
        sec = int(u.get("weekly_total_sec", 0))
        if sec <= 0:
            continue
        bar_len = max(int((sec / top_sec) * 20), 1)
        lines.append(f"{rank}등 {u.get('name','?')} {'■'*bar_len} ({sec/3600:.1f}시간)")
        rank += 1
        if rank > 20:
            break

    ranking_msg = "**📊 이번 주 스터디 랭킹**\n" + "\n".join(lines)
    reset_msg = "✅ 주간 정산이 완료되어 이번 주 누적 시간이 초기화되었습니다."
    return ranking_msg, reset_msg


def roll_active_sessions_into_weekly(guild_data: Dict[str, Any], now: datetime):
    """정산 직전에 진행 중인 세션을 이번 주 기록에 반영하고, 세션은 정산 시점부터 이어가게 한다."""
    now_iso = dt_to_iso(now)
    for u in guild_data.get("users", {}).values():
        if u.get("status") not in ("work", "break"):
            continue

        studied_sec = calc_effective_study_sec(u, now)
        break_sec = current_break_sec(u, now)
        today_s = now.date().isoformat()
        add_recorded_study_sec(u, today_s, studied_sec)
        add_recorded_break_sec(u, today_s, break_sec)
        u["weekly_total_sec"] = int(u.get("weekly_total_sec", 0)) + studied_sec
        u["start_time"] = now_iso
        u["total_break_today"] = 0

        if u.get("status") == "break":
            u["break_start"] = now_iso
        else:
            u["break_start"] = None


async def run_weekly_settlement(
    guild: discord.Guild,
    guild_data: Dict[str, Any],
    settlement_channel: discord.TextChannel
):
    """안내 → 랭킹 → 초기화, 그리고 weekly_reset 로그도 남김"""
    now = now_kst()
    rollover_active_sessions(guild_data, now)
    roll_active_sessions_into_weekly(guild_data, now)

    announce = "📌 **이번 주 종료!** 지금부터 주간정산을 시작합니다."
    await send_settlement_message_both(guild, guild_data, settlement_channel, announce)

    ranking_msg, reset_msg = build_weekly_ranking_lines(guild_data)
    await send_settlement_message_both(guild, guild_data, settlement_channel, ranking_msg)
    if reset_msg:
        await send_settlement_message_both(guild, guild_data, settlement_channel, reset_msg)

    # 티어 횟수 기록 후 초기화
    for u in guild_data.get("users", {}).values():
        tier_key = tier_key_from_weekly(int(u.get("weekly_total_sec", 0)))
        counts = u.setdefault("tier_counts", {})
        for k in TIER_LABELS:
            counts.setdefault(k, 0)
        counts[tier_key] = int(counts.get(tier_key, 0)) + 1
        u["weekly_total_sec"] = 0
    guild_data["week_start"] = week_start_kst(now.date()).isoformat()

    # ✅ weekly_reset을 "로그"로도 남기고, 그 메시지 ID를 저장(이벤트 소싱 최적화 기준점)
    reset_log = make_system_log("weekly_reset", now_kst(), week_start=guild_data.get("week_start"))
    msg_id = await send_log_text(guild, guild_data, reset_log)
    if msg_id:
        guild_data["last_weekly_reset_log_id"] = msg_id


# ------------------------------------------------------------
# ✅ 응답 후 작업(로그/대시보드) 분리
# ------------------------------------------------------------
def schedule_after_response(coro):
    try:
        asyncio.create_task(coro)
    except Exception:
        pass


async def safe_defer(interaction: discord.Interaction, *, ephemeral: bool = False, thinking: bool = False) -> bool:
    """
    ✅ Unknown interaction(10062) / already acknowledged(40060) 방어
    - 성공하면 True
    - 실패하면 False (이 경우 followup도 실패할 수 있어 그냥 종료하는 게 안전)
    """
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral, thinking=thinking)
        return True
    except discord.NotFound:
        return False
    except discord.HTTPException as e:
        # 이미 ack 된 경우(40060)는 True 취급하고 진행
        if getattr(e, "code", None) == 40060:
            return True
        return False


# ------------------------------------------------------------
# ✅ 버튼 UI(View) - persistent
# ------------------------------------------------------------
class StudyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="▶ 출근", style=discord.ButtonStyle.success, custom_id="study:checkin")
    async def checkin(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        if not await safe_defer(interaction, ephemeral=True):
            return

        now = now_kst()
        log_text = None

        async with store.lock:
            g = ensure_guild(store.data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)

            if u.get("status") == "work":
                await interaction.followup.send("이미 출근(공부 중) 상태입니다.", ephemeral=True)
                return
            if u.get("status") == "break":
                await interaction.followup.send("현재 휴식 중입니다. 휴식/복귀로 복귀하거나 퇴근하세요.", ephemeral=True)
                return

            u["status"] = "work"
            u["start_time"] = dt_to_iso(now)
            u["break_start"] = None
            u["total_break_today"] = 0

            store.save_now_locked()

            log_text = make_log("checkin", interaction.user, now)

        await interaction.followup.send("✅ 출근 완료!", ephemeral=True)

        async def after():
            async with store.lock:
                g2 = ensure_guild(store.data, interaction.guild.id)
            if log_text:
                await send_log_text(interaction.guild, g2, log_text)
            await update_dashboard(interaction.guild, g2, last_actor=interaction.user, force=True)

        schedule_after_response(after())

    @discord.ui.button(label="⏸ 휴식/복귀", style=discord.ButtonStyle.secondary, custom_id="study:toggle_break")
    async def toggle_break(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        if not await safe_defer(interaction, ephemeral=True):
            return

        now = now_kst()
        log_text = None
        reply = ""

        async with store.lock:
            g = ensure_guild(store.data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)
            rollover_active_sessions(g, now)

            st = u.get("status", "off")
            if st == "off":
                await interaction.followup.send("출근 후에 사용할 수 있습니다. 먼저 [▶ 출근]을 눌러주세요.", ephemeral=True)
                return

            if st == "work":
                u["status"] = "break"
                u["break_start"] = dt_to_iso(now)
                store.save_now_locked()

                log_text = make_log("break_start", interaction.user, now)
                reply = "⏸ 휴식 시작!"

            elif st == "break":
                bs = iso_to_dt(u.get("break_start"))
                delta = int((now - bs).total_seconds()) if bs else 0

                u["total_break_today"] = int(u.get("total_break_today", 0)) + max(delta, 0)
                u["status"] = "work"
                u["break_start"] = None
                store.save_now_locked()

                log_text = make_log("break_end", interaction.user, now, break_sec=delta, total_break_today=u.get("total_break_today", 0))
                reply = f"▶ 복귀 완료! (휴식 {fmt_hhmm(delta)})"
            else:
                reply = "알 수 없는 상태입니다."

        await interaction.followup.send(reply, ephemeral=True)

        async def after():
            async with store.lock:
                g2 = ensure_guild(store.data, interaction.guild.id)
            if log_text:
                await send_log_text(interaction.guild, g2, log_text)
            await update_dashboard(interaction.guild, g2, last_actor=interaction.user, force=True)

        schedule_after_response(after())

    @discord.ui.button(label="⏹ 퇴근", style=discord.ButtonStyle.danger, custom_id="study:checkout")
    async def checkout(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        if not await safe_defer(interaction, thinking=True):
            return

        now = now_kst()
        studied_sec = 0
        tier = "⚪ 언랭"
        streak = 0
        weekly_total_after = 0
        log_text = None

        async with store.lock:
            g = ensure_guild(store.data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)
            rollover_active_sessions(g, now)

            st = u.get("status", "off")
            if st == "off":
                await interaction.followup.send("현재 대기 중입니다. 출근하지 않은 상태에서는 퇴근할 수 없습니다.", ephemeral=True)
                return

            # 휴식 중 퇴근: 휴식 반영
            if st == "break":
                bs = iso_to_dt(u.get("break_start"))
                if bs:
                    delta = int((now - bs).total_seconds())
                    u["total_break_today"] = int(u.get("total_break_today", 0)) + max(delta, 0)
                u["break_start"] = None

            studied_sec = calc_effective_study_sec(u, now)
            break_sec = current_break_sec(u, now)
            today_s = now.date().isoformat()
            add_recorded_study_sec(u, today_s, studied_sec)
            add_recorded_break_sec(u, today_s, break_sec)
            u["weekly_total_sec"] = int(u.get("weekly_total_sec", 0)) + studied_sec
            weekly_total_after = int(u.get("weekly_total_sec", 0))

            yday_s = (now.date() - timedelta(days=1)).isoformat()
            last = u.get("last_work_date")

            if last == yday_s:
                u["streak"] = int(u.get("streak", 0)) + 1
            elif last == today_s:
                u["streak"] = int(u.get("streak", 0))
            else:
                u["streak"] = 1

            u["last_work_date"] = today_s
            streak = int(u.get("streak", 0))
            u["best_streak"] = max(int(u.get("best_streak", 0)), streak)
            tier = tier_from_weekly(weekly_total_after)

            # 종료 처리
            u["status"] = "off"
            u["start_time"] = None
            u["break_start"] = None
            u["total_break_today"] = 0

            store.save_now_locked()

            log_text = make_log(
                "checkout",
                interaction.user,
                now,
                studied_sec=studied_sec,
                weekly_total_sec=weekly_total_after,
                streak=streak,
                tier=tier
            )

        # ✅ 이름(멘션) 포함 요청 반영
        msg = f"{interaction.user.mention} 수고하셨습니다! 오늘 {fmt_hhmm(studied_sec)} 공부함. (현재 티어: {tier} / 🔥 {streak}일 연속)"
        await interaction.followup.send(msg)

        async def after():
            async with store.lock:
                g2 = ensure_guild(store.data, interaction.guild.id)
            if log_text:
                await send_log_text(interaction.guild, g2, log_text)
            await update_dashboard(interaction.guild, g2, last_actor=interaction.user, force=True)

        schedule_after_response(after())

    @discord.ui.button(label="📌 오늘 요약", style=discord.ButtonStyle.secondary, custom_id="study:today_summary")
    async def today_summary(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return
        if not await safe_defer(interaction, ephemeral=True):
            return

        now = now_kst()
        async with store.lock:
            g = ensure_guild(store.data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)
            rollover_active_sessions(g, now)
            text = build_today_summary_text(u, interaction.user.display_name, now)
            store.save_now_locked()

        await interaction.followup.send(text, ephemeral=True)

    @discord.ui.button(label="📅 주간 정보", style=discord.ButtonStyle.secondary, custom_id="study:weekly_info")
    async def weekly_info(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return
        if not await safe_defer(interaction, ephemeral=True):
            return

        now = now_kst()
        async with store.lock:
            g = ensure_guild(store.data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)
            rollover_active_sessions(g, now)
            text = build_weekly_info_text(u, interaction.user.display_name, now)
            store.save_now_locked()

        await interaction.followup.send(text, ephemeral=True)

    @discord.ui.button(label="🏅 통합 정보", style=discord.ButtonStyle.secondary, custom_id="study:total_info")
    async def total_info(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return
        if not await safe_defer(interaction, ephemeral=True):
            return

        now = now_kst()
        async with store.lock:
            g = ensure_guild(store.data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)
            rollover_active_sessions(g, now)
            text = build_total_info_text(u, interaction.user.display_name, now)
            store.save_now_locked()

        await interaction.followup.send(text, ephemeral=True)


# ------------------------------------------------------------
# ✅ Koyeb Health Check 서버 (/health)
# - Discord 로그인보다 먼저 떠야 "Starting 고착"이 줄어듭니다.
# ------------------------------------------------------------
async def health_check(request: web.Request):
    return web.Response(text="OK", status=200)


async def start_web_server():
    app = web.Application()
    app.router.add_get("/health", health_check)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "8000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[BOOT {BOOT_ID}] ✅ Health server listening on 0.0.0.0:{port}/health")


async def ping_self():
    """
    KOYEB_URL=https://xxxx.koyeb.app/health
    - Free 수면을 100% 막아주진 못하지만, 재시작/라우팅 유지에 도움 되는 경우가 많습니다.
    """
    await bot.wait_until_ready()

    url = os.getenv("KOYEB_URL", "").strip()
    if not url:
        print(f"[BOOT {BOOT_ID}] ⚠ KOYEB_URL 미설정: self ping 비활성")
        return

    global http_session
    if http_session is None or http_session.closed:
        http_session = aiohttp.ClientSession()

    ok = 0
    fail = 0

    while not bot.is_closed():
        try:
            async with http_session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                _ = await r.text()
            ok += 1
        except Exception:
            fail += 1

        # 너무 시끄럽지 않게 20회마다만 출력
        if (ok + fail) % 20 == 0:
            print(f"[BOOT {BOOT_ID}] [PING] ok={ok} fail={fail} url={url}")

        await asyncio.sleep(180)


# ------------------------------------------------------------
# ✅ Bot 클래스(핵심: setup_hook에서 선기동)
# ------------------------------------------------------------
class MyBot(commands.Bot):
    async def setup_hook(self):
        # 1) 파일 로드(가장 먼저)
        await store.load_once()

        # 2) persistent view 등록
        self.add_view(StudyView())

        # 3) 자동 태스크 시작
        if not auto_dashboard_refresh.is_running():
            auto_dashboard_refresh.start()
        if not auto_weekly_settlement.is_running():
            auto_weekly_settlement.start()
        if not study_safety_alerts.is_running():
            study_safety_alerts.start()

        # 5) self ping
        self.loop.create_task(ping_self())

        print(f"[BOOT {BOOT_ID}] ✅ setup_hook 완료")


bot = MyBot(command_prefix="!", intents=INTENTS)


# ------------------------------------------------------------
# ✅ 명령어: !설치 (현황판)
# ------------------------------------------------------------
@bot.command(name="설치")
async def install_panel(ctx: commands.Context):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    async with store.lock:
        g = ensure_guild(store.data, ctx.guild.id)
        ensure_week_current(g)

        # 이미 등록된 패널이 살아있으면 그대로 사용
        old = await fetch_panel_message(ctx.guild, g)
        if old:
            try:
                await ctx.send("이미 이 서버에 현황판이 설치되어 있습니다. (기존 메시지를 사용 중)")
            except Exception:
                pass
            return

        embed = build_dashboard_embed(ctx.guild, g)

        try:
            msg = await ctx.send(embed=embed, view=StudyView())
        except discord.Forbidden:
            try:
                await ctx.send("봇에 메시지/임베드/버튼 권한이 없습니다. 채널 권한을 확인해 주세요.")
            except Exception:
                pass
            return

        g["panel"]["channel_id"] = msg.channel.id
        g["panel"]["message_id"] = msg.id
        g["dashboard_hash"] = dashboard_hash(build_dashboard_text(g))

        store.save_now_locked()

    try:
        await ctx.send("✅ 스터디 현황판을 설치했습니다!")
    except Exception:
        pass


# ------------------------------------------------------------
# ✅ 명령어: !패널복구
# - “현재 채널의 마지막 봇 메시지 중 현황판을 찾아서 panel.message_id 재등록”
# ------------------------------------------------------------
@bot.command(name="패널복구")
async def recover_panel(ctx: commands.Context):
    if not ctx.guild or not isinstance(ctx.channel, discord.TextChannel):
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    target: Optional[discord.Message] = None

    # 최근 메시지에서 봇이 보낸 "📅 스터디 현황판" 임베드를 찾음
    async for msg in ctx.channel.history(limit=100):
        if msg.author.id != bot.user.id:
            continue
        if not msg.embeds:
            continue
        emb = msg.embeds[0]
        if (emb.title or "") == "📅 스터디 현황판":
            target = msg
            break

    if not target:
        await ctx.send("현재 채널에서 현황판 메시지를 찾지 못했습니다. `!설치`로 다시 설치하세요.")
        return

    async with store.lock:
        g = ensure_guild(store.data, ctx.guild.id)
        g["panel"]["channel_id"] = target.channel.id
        g["panel"]["message_id"] = target.id
        # 해시 갱신 및 저장
        g["dashboard_hash"] = None
        store.save_now_locked()

    await ctx.send(f"✅ 패널 복구 완료: 메시지 ID `{target.id}` 재등록")
    # 즉시 갱신
    async with store.lock:
        g2 = ensure_guild(store.data, ctx.guild.id)
    await update_dashboard(ctx.guild, g2, last_actor=None, force=True)


# ------------------------------------------------------------
# ✅ 로그/정산 채널 설정
# ------------------------------------------------------------
@bot.command(name="로그채널설정")
async def set_log_channel(ctx: commands.Context, channel_arg: str):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    ch = resolve_text_channel(ctx.guild, channel_arg)
    if not ch:
        await ctx.send("채널을 찾지 못했습니다. `!로그채널설정 #채널`처럼 채널 멘션으로 입력해 주세요.")
        return

    async with store.lock:
        g = ensure_guild(store.data, ctx.guild.id)
        g["log_channel_id"] = ch.id
        store.save_now_locked()

    await ctx.send(f"✅ 로그 채널이 설정되었습니다: {ch.mention}\n이제 출근/휴식/복귀/퇴근/정산 이벤트가 모두 기록됩니다.")


@bot.command(name="정산채널설정")
async def set_settlement_channel(ctx: commands.Context, channel_arg: str):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    ch = resolve_text_channel(ctx.guild, channel_arg)
    if not ch:
        await ctx.send("채널을 찾지 못했습니다. `!정산채널설정 #채널`처럼 채널 멘션으로 입력해 주세요.")
        return

    async with store.lock:
        g = ensure_guild(store.data, ctx.guild.id)
        g["settlement_channel_id"] = ch.id
        store.save_now_locked()

    await ctx.send(f"✅ 자동 주간정산 채널이 설정되었습니다: {ch.mention}\n(월요일 00:00 KST에 이 채널로 자동 출력)")


# ------------------------------------------------------------
# ✅ 음성방 텍스트 알림 설정/호출
# ------------------------------------------------------------
@bot.command(name="음성알림방설정")
async def set_voice_alert_channel(ctx: commands.Context, *, channel_arg: str):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    ch = resolve_voice_channel(ctx.guild, channel_arg)
    if not ch:
        await ctx.send("음성채널을 찾지 못했습니다. 음성채널명 또는 채널 멘션으로 입력해주세요.")
        return

    async with store.lock:
        g = ensure_guild(store.data, ctx.guild.id)
        ids = g.setdefault("monitored_voice_channel_ids", [])
        if ch.id not in ids:
            ids.append(ch.id)
        store.save_now_locked()

    await ctx.send(f"✅ 음성방 알림 대상에 추가했습니다: **{ch.name}**")


@bot.command(name="음성알림방해제")
async def unset_voice_alert_channel(ctx: commands.Context, *, channel_arg: str):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    ch = resolve_voice_channel(ctx.guild, channel_arg)
    if not ch:
        await ctx.send("음성채널을 찾지 못했습니다.")
        return

    async with store.lock:
        g = ensure_guild(store.data, ctx.guild.id)
        ids = g.setdefault("monitored_voice_channel_ids", [])
        g["monitored_voice_channel_ids"] = [cid for cid in ids if int(cid) != ch.id]
        store.save_now_locked()

    await ctx.send(f"✅ 음성방 알림 대상에서 제거했습니다: **{ch.name}**")


@bot.command(name="호출")
async def call_voice_room(ctx: commands.Context, *, target_arg: str):
    if not ctx.guild or not isinstance(ctx.author, discord.Member):
        return

    member_target = resolve_member_target(ctx.guild, target_arg)
    channel_target = resolve_voice_channel(ctx.guild, target_arg) if not member_target else None
    if not member_target and not channel_target:
        await ctx.send("호출할 대상을 찾지 못했습니다. 유저 멘션, 유저명, 음성채널명 중 하나로 입력해주세요.")
        return

    now = now_kst()
    async with store.lock:
        g = ensure_guild(store.data, ctx.guild.id)
        last = iso_to_dt(g.get("call_alert_last_at"))
        if last and (now - last).total_seconds() < 300:
            remain = 300 - int((now - last).total_seconds())
            await ctx.send(f"호출은 5분마다 사용할 수 있습니다. {remain // 60}분 {remain % 60}초 뒤에 다시 시도해주세요.")
            return
        g["call_alert_last_at"] = dt_to_iso(now)
        store.save_now_locked()

    source = ctx.author.voice.channel.name if ctx.author.voice and ctx.author.voice.channel else ctx.channel.name
    if member_target:
        target_place = member_target.voice.channel.name if member_target.voice and member_target.voice.channel else "현재 음성채널 없음"
        content = f"📣 **개인 호출**: **{source}**에서 {member_target.mention}님을 호출했습니다."
        content += f"\n대상 위치: **{target_place}**"
    else:
        mentions = " ".join(m.mention for m in channel_target.members if not m.bot)
        content = f"📣 **방 호출**: **{source}**에서 **{channel_target.name}** 방을 호출했습니다."
        if mentions:
            content += f"\n{mentions}"

    async with store.lock:
        g2 = ensure_guild(store.data, ctx.guild.id)
    await send_alert_text(ctx.guild, g2, content)
    await ctx.send("✅ 호출 알림을 보냈습니다.")


# ------------------------------------------------------------
# ✅ 관리자 명령: !시간정정 @유저 [시간]
# ------------------------------------------------------------
@bot.command(name="시간정정")
async def adjust_time(ctx: commands.Context, member: discord.Member, hours: str):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    try:
        h = float(hours)
    except ValueError:
        await ctx.send("시간은 숫자로 입력해주세요. 예) 2, -1.5")
        return

    delta_sec = int(h * 3600)

    async with store.lock:
        g = ensure_guild(store.data, ctx.guild.id)
        ensure_week_current(g)
        u = ensure_user(g, member)

        u["weekly_total_sec"] = max(int(u.get("weekly_total_sec", 0)) + delta_sec, 0)
        store.save_now_locked()
        current = fmt_hhmm(int(u.get("weekly_total_sec", 0)))

    await ctx.send(
        f"✅ 시간 정정 완료: {member.display_name} / {fmt_hhmm(abs(delta_sec))} ({'추가' if delta_sec >= 0 else '차감'})\n"
        f"현재 주간 누적: {current}"
    )

    # 대시보드 갱신
    async with store.lock:
        g2 = ensure_guild(store.data, ctx.guild.id)
    await update_dashboard(ctx.guild, g2, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None, force=True)


# ------------------------------------------------------------
# ✅ 관리자 명령: !주간정산 (수동)
# ------------------------------------------------------------
@bot.command(name="주간정산")
async def weekly_settlement_cmd(ctx: commands.Context):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    async with store.lock:
        g = ensure_guild(store.data, ctx.guild.id)
        ch = get_settlement_channel(ctx.guild, g)

    if not ch:
        await ctx.send("정산 메시지를 보낼 채널을 찾지 못했습니다.")
        return

    await ctx.send("📌 수동 주간정산을 시작합니다...")

    async with store.lock:
        g_live = ensure_guild(store.data, ctx.guild.id)
    await run_weekly_settlement(ctx.guild, g_live, ch)

    async with store.lock:
        g_save = ensure_guild(store.data, ctx.guild.id)
        g_save["last_settlement_week_start"] = g_save.get("week_start")
        store.save_now_locked()

    async with store.lock:
        g2 = ensure_guild(store.data, ctx.guild.id)
    await update_dashboard(ctx.guild, g2, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None, force=True)


# ------------------------------------------------------------
# ✅ 이벤트 소싱: !리플레이
# - 로그 채널의 메시지를 읽어 상태를 재구성
# - “마지막 weekly_reset 이후부터”만 읽도록 최적화
# ------------------------------------------------------------
@bot.command(name="리플레이")
async def replay_from_logs(ctx: commands.Context):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    async with store.lock:
        g = ensure_guild(store.data, ctx.guild.id)
        log_id = g.get("log_channel_id")
        last_reset_id = g.get("last_weekly_reset_log_id")

    if not log_id:
        await ctx.send("먼저 `!로그채널설정 #채널`로 로그 채널을 지정해 주세요.")
        return

    log_ch = ctx.guild.get_channel(int(log_id))
    if not isinstance(log_ch, discord.TextChannel):
        await ctx.send("로그 채널을 찾지 못했습니다. 채널 삭제/권한을 확인해 주세요.")
        return

    # 1) 현재 상태를 리셋(유저는 남기고 상태만 초기화)
    async with store.lock:
        g = ensure_guild(store.data, ctx.guild.id)
        for uid, u in g.get("users", {}).items():
            u["status"] = "off"
            u["start_time"] = None
            u["break_start"] = None
            u["total_break_today"] = 0
        store.save_now_locked()

    # 2) 로그를 읽어 이벤트 적용
    applied = 0
    scanned = 0

    # after 기준: 마지막 weekly_reset 메시지 이후만
    after_obj = discord.Object(id=int(last_reset_id)) if last_reset_id else None

    async for msg in log_ch.history(limit=2000, oldest_first=True, after=after_obj):
        scanned += 1
        if not msg.content.startswith(LOG_PREFIX):
            continue

        evt = parse_log_line(msg.content)
        if not evt:
            continue

        action = evt.get("action")
        uid = evt.get("uid")
        ts = iso_to_dt(evt.get("ts"))
        if not action or not uid or not ts:
            continue

        # SYSTEM weekly_reset이면 기준점 갱신
        if uid == "SYSTEM" and action == "weekly_reset":
            async with store.lock:
                g = ensure_guild(store.data, ctx.guild.id)
                g["last_weekly_reset_log_id"] = msg.id
                # weekly_reset 이후 주간 누적은 이미 0이라는 전제로 진행
                store.save_now_locked()
            continue

        member = ctx.guild.get_member(int(uid))
        if not member:
            continue

        async with store.lock:
            g = ensure_guild(store.data, ctx.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, member)

            if action == "checkin":
                # 출근
                u["status"] = "work"
                u["start_time"] = evt.get("ts")
                u["break_start"] = None
                u["total_break_today"] = 0
                applied += 1

            elif action == "break_start":
                if u.get("status") == "work":
                    u["status"] = "break"
                    u["break_start"] = evt.get("ts")
                    applied += 1

            elif action == "break_end":
                if u.get("status") == "break":
                    bs = iso_to_dt(u.get("break_start"))
                    now_dt = ts
                    delta = int((now_dt - bs).total_seconds()) if bs else 0
                    u["total_break_today"] = int(u.get("total_break_today", 0)) + max(delta, 0)
                    u["status"] = "work"
                    u["break_start"] = None
                    applied += 1

            elif action == "checkout":
                # 퇴근: 로그에 studied_sec가 있으면 그걸 주간 누적에 반영
                try:
                    studied_sec = int(float(evt.get("studied_sec", "0")))
                except Exception:
                    studied_sec = 0

                u["weekly_total_sec"] = int(u.get("weekly_total_sec", 0)) + max(studied_sec, 0)

                # 스트릭/last_work_date는 로그에 있으면 반영
                streak_s = evt.get("streak")
                tier_s = evt.get("tier")
                if streak_s and streak_s.isdigit():
                    u["streak"] = int(streak_s)
                u["last_work_date"] = ts.date().isoformat()

                # 상태 종료
                u["status"] = "off"
                u["start_time"] = None
                u["break_start"] = None
                u["total_break_today"] = 0
                applied += 1

            store.save_now_locked()

    # 3) 대시보드 갱신
    async with store.lock:
        g2 = ensure_guild(store.data, ctx.guild.id)
    await update_dashboard(ctx.guild, g2, last_actor=None, force=True)

    await ctx.send(f"✅ 리플레이 완료: scanned={scanned}, applied={applied}\n(기준: last_weekly_reset_log_id={last_reset_id})")


# ------------------------------------------------------------
# ✅ 음성방 입장/깜빡/휴식 과다 알림
# ------------------------------------------------------------
async def delayed_checkin_reminder(guild_id: int, member_id: int, channel_id: int):
    await asyncio.sleep(300)
    guild = bot.get_guild(guild_id)
    if not guild:
        return
    member = guild.get_member(member_id)
    if not member or member.bot:
        return
    if not member.voice or not member.voice.channel or member.voice.channel.id != channel_id:
        return

    async with store.lock:
        g = ensure_guild(store.data, guild.id)
        u = ensure_user(g, member)
        if u.get("status") in ("work", "break"):
            return

    channel_name = member.voice.channel.name
    await send_alert_text(guild, g, f"⏰ {member.mention}님, **{channel_name}**에 들어와 있지만 아직 출근하지 않았습니다. 현황판에서 [▶ 출근]을 눌러주세요.")


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot or not member.guild:
        return
    if before.channel == after.channel or not after.channel:
        return

    async with store.lock:
        g = ensure_guild(store.data, member.guild.id)
        monitored = {int(cid) for cid in g.get("monitored_voice_channel_ids", [])}

    if after.channel.id not in monitored:
        return

    await send_alert_text(member.guild, g, f"🔔 **{after.channel.name}** 방에 {member.mention}님이 들어왔습니다.")
    schedule_after_response(delayed_checkin_reminder(member.guild.id, member.id, after.channel.id))


@tasks.loop(seconds=300)
async def study_safety_alerts():
    if not bot.is_ready():
        return

    now = now_kst()
    for guild in bot.guilds:
        alerts: List[str] = []
        async with store.lock:
            g = ensure_guild(store.data, guild.id)
            rollover_active_sessions(g, now)
            break_alerts = g.setdefault("break_alerts", {})
            long_alerts = g.setdefault("long_session_alerts", {})
            midnight_alerts = g.setdefault("midnight_alerts", {})

            for uid, u in g.get("users", {}).items():
                status = u.get("status", "off")
                mention = f"<@{uid}>"

                if status == "break":
                    bs = iso_to_dt(u.get("break_start"))
                    if bs:
                        mins = int((now - bs).total_seconds() // 60)
                        level = 60 if mins >= 60 else 30 if mins >= 30 else 0
                        prev = int(break_alerts.get(uid, 0))
                        if level and level > prev:
                            break_alerts[uid] = level
                            alerts.append(f"☕ {mention}님이 {mins}분째 휴식 중입니다.")
                else:
                    break_alerts.pop(uid, None)

                if status in ("work", "break"):
                    start = iso_to_dt(u.get("start_time"))
                    if start:
                        active_hours = (now - start).total_seconds() / 3600
                        if active_hours >= 12 and long_alerts.get(uid) != u.get("start_time"):
                            long_alerts[uid] = u.get("start_time")
                            alerts.append(f"⏹ {mention}님, 세션이 12시간 이상 이어지고 있습니다. 퇴근을 깜빡한 것은 아닌지 확인해주세요.")

                    today_s = now.date().isoformat()
                    if now.hour == 23 and now.minute >= 50 and midnight_alerts.get(uid) != today_s:
                        midnight_alerts[uid] = today_s
                        alerts.append(f"🌙 {mention}님, 곧 하루가 넘어갑니다. 계속 공부 중이면 기록은 자동으로 내일 세션으로 이어집니다.")
                else:
                    long_alerts.pop(uid, None)

            store.save_now_locked()

        for content in alerts:
            async with store.lock:
                g2 = ensure_guild(store.data, guild.id)
            await send_alert_text(guild, g2, content)


# ------------------------------------------------------------
# ✅ 자동 주간정산: 월요일 00:00(KST)
# ------------------------------------------------------------
@tasks.loop(time=time(hour=0, minute=0, tzinfo=KST))
async def auto_weekly_settlement():
    if not bot.is_ready():
        return
    # 월요일 00:00에 지난주를 닫고 새 주를 시작
    if now_kst().weekday() != 0:
        return

    for guild in bot.guilds:
        # 데이터/채널만 확보하고 락 해제
        async with store.lock:
            g = ensure_guild(store.data, guild.id)
            ensure_week_current(g)

            current_ws = week_start_kst(now_kst().date()).isoformat()
            if g.get("last_settlement_week_start") == current_ws:
                continue

            ch = get_settlement_channel(guild, g)
            if not ch:
                continue

        # 정산 실행(네트워크)
        async with store.lock:
            g_live = ensure_guild(store.data, guild.id)
        await run_weekly_settlement(guild, g_live, ch)

        # 정산 완료 기록 저장
        async with store.lock:
            g_save = ensure_guild(store.data, guild.id)
            g_save["last_settlement_week_start"] = g_save.get("week_start")
            store.save_now_locked()

        # 대시보드 갱신
        async with store.lock:
            g2 = ensure_guild(store.data, guild.id)
        await update_dashboard(guild, g2, last_actor=None, force=True)


# ------------------------------------------------------------
# ✅ 현황판 조건부 갱신: 활동 있으면 1분, 없으면 5분
# ------------------------------------------------------------
@tasks.loop(seconds=60)
async def auto_dashboard_refresh():
    if not bot.is_ready():
        return

    # 1) 활동 여부 판단(락 짧게)
    async with store.lock:
        any_active = False
        for guild in bot.guilds:
            g = ensure_guild(store.data, guild.id)
            ensure_week_current(g)
            if has_any_activity(g):
                any_active = True

    # 2) 길드별 대시보드 갱신
    for guild in bot.guilds:
        async with store.lock:
            g = ensure_guild(store.data, guild.id)
        await update_dashboard(guild, g, last_actor=None, force=False)

    # 3) interval 조절
    target_seconds = 60 if any_active else 300
    try:
        auto_dashboard_refresh.change_interval(seconds=target_seconds)
    except Exception:
        pass

    # 4) 저장(해시값 갱신 등이 있을 수 있어 반영)
    async with store.lock:
        store.save_now_locked()


# ------------------------------------------------------------
# ✅ on_ready: 재시작 시 패널 1회 복구 갱신
# ------------------------------------------------------------
@bot.event
async def on_ready():
    # 재시작 시 패널이 있으면 1회 강제 갱신
    for guild in bot.guilds:
        async with store.lock:
            g = ensure_guild(store.data, guild.id)
            ensure_week_current(g)
        await update_dashboard(guild, g, last_actor=None, force=True)

    async with store.lock:
        store.save_now_locked()

    print(f"[BOOT {BOOT_ID}] ✅ 로그인 완료: {bot.user} (서버 {len(bot.guilds)}개)")


# ------------------------------------------------------------
# ✅ graceful close
# ------------------------------------------------------------
async def close_http_session():
    global http_session
    if http_session and not http_session.closed:
        await http_session.close()


# ------------------------------------------------------------
# 실행
# ------------------------------------------------------------
async def main():
    token = TOKEN.strip() or os.getenv("DISCORD_TOKEN", "").strip() or os.getenv("token", "").strip()
    if not token:
        print("⚠ TOKEN이 비어 있습니다. main.py 상단 TOKEN 또는 환경변수 DISCORD_TOKEN을 설정하세요.")
        raise SystemExit(0)

    await start_web_server()
    try:
        await bot.start(token)
    finally:
        await close_http_session()
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
