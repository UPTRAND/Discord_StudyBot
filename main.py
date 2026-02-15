# main.py
# ------------------------------------------------------------
# ✅ 권장 설치 (Koyeb/Windows 공통)
#   python -m pip install -U discord.py tzdata aiohttp
#
# ✅ 실행
#   python main.py
#
# ✅ 디스코드에서 사용
# 1) !설치
# 2) !로그채널설정 #study-log        (채널 멘션으로 입력 권장)
# 3) (선택) !정산채널설정 #ranking   (자동 주간정산이 나갈 채널 지정)
#
# ✅ 자동 기능
# - (1) 일요일 KST 12:00 자동 주간정산
#       📌 안내 메시지 → 랭킹 출력 → 초기화 완료 메시지
#       (정산 결과는 로그 채널에도 함께 남김)
# - (2) 현황판 조건부 갱신: 활동(work/break) 있으면 1분, 없으면 5분
# - (3) 버튼(출근/휴식/복귀/퇴근) 누를 때마다 즉시 현황판 업데이트
#
# ✅ 이번 패치 핵심(요청사항)
# - Deadlock 제거: 락을 잡은 상태에서 save_now() 같은 “재락” 호출 금지
# - defer 표준화: 모든 버튼 콜백 시작에 interaction.response.defer(...), 실패해도 안전(safe_defer)
# - 응답 후 작업 분리: 로그 전송/현황판 수정은 asyncio.create_task로 응답 이후 처리
#
# ✅ 추가 안정화(이번 문제 해결)
# - 패널 메시지 갱신: fetch_message 대신 get_partial_message(...).edit(...) 사용
#   → Read Message History 권한 의존 ↓, 재시작 후에도 안정적으로 edit 가능
# - 대시보드 해시: "분 단위 키" 포함 → 시간이 흘러도 갱신이 눈에 보이도록
# - msg.edit에 timeout 적용 → 네트워크 지연이 이벤트 루프를 오래 점유하지 않도록
# ------------------------------------------------------------

import os
import json
import asyncio
import hashlib
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

# ✅ KST (Windows에서 tzdata 없으면 실패할 수 있어 안전장치 포함)
try:
    KST = ZoneInfo("Asia/Seoul")
except Exception:
    KST = timezone(timedelta(hours=9), name="KST")

INTENTS = discord.Intents.default()
INTENTS.message_content = True

bot = commands.Bot(command_prefix="!", intents=INTENTS)


# ------------------------------------------------------------
# ✅ 유틸 (시간/포맷)
# ------------------------------------------------------------
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


def tier_from_weekly(weekly_sec: int) -> str:
    hours = weekly_sec / 3600.0
    if hours < 10:
        return "🥉 브론즈"
    if hours < 20:
        return "🥈 실버"
    if hours < 40:
        return "🥇 골드"
    return "🏆 챌린저"


def status_label(status: str) -> str:
    if status == "work":
        return "공부 중"
    if status == "break":
        return "휴식 중"
    return "대기 중"


def safe_str(s: Any) -> str:
    return str(s).replace("\n", " ").replace(";", ",").strip()


# ------------------------------------------------------------
# ✅ 데이터 저장소 (Deadlock-free)
# - store.lock을 잡은 상태에서 save_now()를 부르면 재락으로 데드락 위험
# - 따라서:
#   1) 일반 저장: await store.save_now() (락 내부에서 호출 금지)
#   2) 락 이미 잡은 상태: store.save_now_locked()
# ------------------------------------------------------------
class DataStore:
    def __init__(self, path: str):
        self.path = path
        self.lock = asyncio.Lock()
        self.data: Dict[str, Any] = {"version": 1, "guilds": {}}

    def _ensure_file(self):
        if not os.path.exists(self.path):
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)

    def _load_sync(self):
        self._ensure_file()
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
        except Exception:
            self.data = {"version": 1, "guilds": {}}

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
        # ✅ 락 밖에서 호출하는 일반 저장
        async with self.lock:
            self._atomic_save_sync()

    def save_now_locked(self):
        # ✅ 이미 store.lock을 잡은 상태에서 호출
        self._atomic_save_sync()


store = DataStore(DATA_FILE)

# aiohttp 세션(재사용 최적화)
http_session: Optional[aiohttp.ClientSession] = None


# ------------------------------------------------------------
# ✅ 길드/유저 구조 보장 + 주 변경 처리
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
            # ✅ 최적화: 마지막 임베드 해시(동일하면 edit 생략)
            "dashboard_hash": None,
        }
        data["guilds"][gid] = g
    else:
        g.setdefault("panel", {"channel_id": None, "message_id": None})
        g.setdefault("log_channel_id", None)
        g.setdefault("settlement_channel_id", None)
        g.setdefault("last_settlement_week_start", None)
        g.setdefault("users", {})
        g.setdefault("week_start", week_start_kst(now_kst().date()).isoformat())
        g.setdefault("dashboard_hash", None)

    return g


def ensure_week_current(guild_data: Dict[str, Any]) -> bool:
    today = now_kst().date()
    current = week_start_kst(today).isoformat()
    if guild_data.get("week_start") != current:
        guild_data["week_start"] = current
        for u in guild_data["users"].values():
            u["weekly_total_sec"] = 0
        return True
    return False


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
        }
        users[uid] = u
    else:
        u["name"] = member.display_name
    return u


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


def has_any_activity(guild_data: Dict[str, Any]) -> bool:
    for u in guild_data.get("users", {}).values():
        if u.get("status") in ("work", "break"):
            return True
    return False


# ------------------------------------------------------------
# ✅ 로그 채널 전송 (응답 후 task에서 호출 권장)
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


async def send_to_channel(channel: Optional[discord.TextChannel], content: str):
    if not channel:
        return
    try:
        await channel.send(content)
    except Exception:
        pass


async def send_log_text(guild: discord.Guild, guild_data: Dict[str, Any], text: str):
    ch_id = guild_data.get("log_channel_id")
    if not ch_id:
        return
    ch = guild.get_channel(int(ch_id))
    if isinstance(ch, discord.TextChannel):
        await send_to_channel(ch, text)


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


# ------------------------------------------------------------
# ✅ 상호작용 안전 defer/followup
# - 404(10062): interaction 만료
# - 400(40060): 이미 ack됨 (중복 실행/레이스 등)
# ------------------------------------------------------------
async def safe_defer(interaction: discord.Interaction, *, ephemeral: bool = True, thinking: bool = False) -> bool:
    try:
        if interaction.response.is_done():
            return False
        await interaction.response.defer(ephemeral=ephemeral, thinking=thinking)
        return True
    except discord.HTTPException:
        return False


async def safe_followup(interaction: discord.Interaction, content: str, *, ephemeral: bool = False):
    try:
        await interaction.followup.send(content, ephemeral=ephemeral)
    except Exception:
        # interaction이 만료됐으면 사용자에겐 실패 표시가 뜰 수 있으나, 서버 로직은 계속 진행
        pass


# ------------------------------------------------------------
# ✅ 대시보드(현황판) - edit 최소화(해시 비교)
# ------------------------------------------------------------
def build_dashboard_text(guild_data: Dict[str, Any]) -> str:
    now = now_kst()
    work_lines: List[str] = []
    break_lines: List[str] = []

    for u in guild_data["users"].values():
        st = u.get("status", "off")
        name = u.get("name", "알 수 없음")
        if st == "work":
            sec = calc_effective_study_sec(u, now)
            work_lines.append(f"🟢 {name} ({fmt_hhmm(sec)}째)")
        elif st == "break":
            break_lines.append(f"🟡 {name} (휴식 중)")

    lines = work_lines + break_lines
    if not lines:
        return "지금 공부 중인 사람이 없습니다.\n\n버튼으로 출근해서 스터디를 시작해 보세요."
    return " | ".join(lines)


def dashboard_hash(description: str) -> str:
    # ✅ 분 단위 키를 섞어서 "경과시간" 갱신이 눈에 보이도록
    minute_key = now_kst().strftime("%Y-%m-%d %H:%M")
    payload = f"{minute_key}\n{description}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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
        embed.set_footer(text="상태 확인: [📊 내 정보] 버튼 · 기준시간: KST")

    return embed


async def fetch_panel_message(guild: discord.Guild, guild_data: Dict[str, Any]) -> Optional[discord.PartialMessage]:
    panel = guild_data.get("panel", {})
    ch_id = panel.get("channel_id")
    msg_id = panel.get("message_id")
    if not ch_id or not msg_id:
        return None

    ch = guild.get_channel(int(ch_id))
    if not isinstance(ch, discord.TextChannel):
        return None

    # ✅ fetch_message 대신 PartialMessage로 edit (Read Message History 의존 ↓)
    try:
        return ch.get_partial_message(int(msg_id))
    except Exception:
        return None


async def update_dashboard(
    guild: discord.Guild,
    guild_data: Dict[str, Any],
    last_actor: Optional[discord.Member] = None,
    force: bool = False
):
    """
    ✅ 최적화 포인트
    - 대시보드 description을 만들고 해시 비교
    - 동일하면 msg.edit 생략(디스코드 API 호출 감소)
    - edit은 timeout을 걸어 이벤트 루프 점유를 방지
    """
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
        await asyncio.wait_for(msg.edit(embed=embed, view=StudyView()), timeout=8)
    except Exception:
        # 여기서 조용히 실패하면 "가만히 있는" 것처럼 보이므로, 원인 파악이 필요하면 print를 살리세요.
        # print(f"[update_dashboard] failed: {type(e).__name__}: {e}")
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
# ✅ 주간정산 메시지 생성
# ------------------------------------------------------------
def build_weekly_ranking_lines(guild_data: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    users = list(guild_data["users"].values())
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


async def run_weekly_settlement(guild: discord.Guild, guild_data: Dict[str, Any], settlement_channel: discord.TextChannel):
    """
    ✅ 안내 → 랭킹 → 초기화 메시지
    ✅ 정산 채널 + 로그 채널에 모두 남김
    """
    ensure_week_current(guild_data)

    announce = "📌 **이번 주 종료!** 지금부터 주간정산을 시작합니다."
    await send_settlement_message_both(guild, guild_data, settlement_channel, announce)

    ranking_msg, reset_msg = build_weekly_ranking_lines(guild_data)
    await send_settlement_message_both(guild, guild_data, settlement_channel, ranking_msg)
    if reset_msg:
        await send_settlement_message_both(guild, guild_data, settlement_channel, reset_msg)

    # 초기화
    for u in guild_data["users"].values():
        u["weekly_total_sec"] = 0


# ------------------------------------------------------------
# ✅ 응답 후 작업(로그/대시보드)을 안전하게 수행하는 헬퍼
# ------------------------------------------------------------
def schedule_after_response(coro):
    try:
        asyncio.create_task(coro)
    except Exception:
        pass


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

        # ✅ 3초 제한 회피: 먼저 defer (실패해도 진행)
        deferred = await safe_defer(interaction, ephemeral=True)

        now = now_kst()
        need_after = {"log": None, "update": False}

        async with store.lock:
            data = store.data
            g = ensure_guild(data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)

            if u.get("status") == "work":
                if deferred:
                    await safe_followup(interaction, "이미 출근(공부 중) 상태입니다.", ephemeral=True)
                return
            if u.get("status") == "break":
                if deferred:
                    await safe_followup(interaction, "현재 휴식 중입니다. 휴식/복귀 버튼으로 복귀하거나 퇴근하세요.", ephemeral=True)
                return

            u["status"] = "work"
            u["start_time"] = dt_to_iso(now)
            u["break_start"] = None
            u["total_break_today"] = 0

            store.save_now_locked()

            need_after["log"] = make_log("checkin", interaction.user, now)
            need_after["update"] = True

        if deferred:
            await safe_followup(interaction, "✅ 출근 완료!", ephemeral=True)

        async def after():
            async with store.lock:
                g2 = ensure_guild(store.data, interaction.guild.id)
            if need_after["log"]:
                await send_log_text(interaction.guild, g2, need_after["log"])
            await update_dashboard(interaction.guild, g2, last_actor=interaction.user, force=True)

        schedule_after_response(after())

    @discord.ui.button(label="⏸ 휴식/복귀", style=discord.ButtonStyle.secondary, custom_id="study:toggle_break")
    async def toggle_break(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        deferred = await safe_defer(interaction, ephemeral=True)

        now = now_kst()
        need_after = {"log": None, "update": False}
        reply = ""

        async with store.lock:
            data = store.data
            g = ensure_guild(data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)

            st = u.get("status", "off")
            if st == "off":
                if deferred:
                    await safe_followup(interaction, "출근 후에 사용할 수 있습니다. 먼저 [▶ 출근]을 눌러주세요.", ephemeral=True)
                return

            if st == "work":
                u["status"] = "break"
                u["break_start"] = dt_to_iso(now)
                store.save_now_locked()

                need_after["log"] = make_log("break_start", interaction.user, now)
                need_after["update"] = True
                reply = "⏸ 휴식 시작!"

            elif st == "break":
                bs = iso_to_dt(u.get("break_start"))
                delta = int((now - bs).total_seconds()) if bs else 0

                u["total_break_today"] = int(u.get("total_break_today", 0)) + max(delta, 0)
                u["status"] = "work"
                u["break_start"] = None
                store.save_now_locked()

                need_after["log"] = make_log("break_end", interaction.user, now, break_sec=delta, total_break_today=u.get("total_break_today", 0))
                need_after["update"] = True
                reply = f"▶ 복귀 완료! (휴식 {fmt_hhmm(delta)})"
            else:
                reply = "알 수 없는 상태입니다."

        if deferred:
            await safe_followup(interaction, reply, ephemeral=True)

        async def after():
            async with store.lock:
                g2 = ensure_guild(store.data, interaction.guild.id)
            if need_after["log"]:
                await send_log_text(interaction.guild, g2, need_after["log"])
            if need_after["update"]:
                await update_dashboard(interaction.guild, g2, last_actor=interaction.user, force=True)

        schedule_after_response(after())

    @discord.ui.button(label="⏹ 퇴근", style=discord.ButtonStyle.danger, custom_id="study:checkout")
    async def checkout(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        # ✅ 공개 메시지로 마무리할 수 있으니 thinking=True
        deferred = await safe_defer(interaction, ephemeral=False, thinking=True)

        now = now_kst()
        studied_sec = 0
        tier = "🥉 브론즈"
        streak = 0
        weekly_total_after = 0
        log_text = None
        should_reply_error = False
        reply_error = ""

        async with store.lock:
            data = store.data
            g = ensure_guild(data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)

            st = u.get("status", "off")
            if st == "off":
                should_reply_error = True
                reply_error = "현재 대기 중입니다. 출근하지 않은 상태에서는 퇴근할 수 없습니다."
            else:
                # 휴식 중 퇴근: 휴식 시간 반영
                if st == "break":
                    bs = iso_to_dt(u.get("break_start"))
                    if bs:
                        delta = int((now - bs).total_seconds())
                        u["total_break_today"] = int(u.get("total_break_today", 0)) + max(delta, 0)
                    u["break_start"] = None

                studied_sec = calc_effective_study_sec(u, now)
                u["weekly_total_sec"] = int(u.get("weekly_total_sec", 0)) + studied_sec
                weekly_total_after = int(u.get("weekly_total_sec", 0))

                today_s = now.date().isoformat()
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

        if should_reply_error:
            if deferred:
                await safe_followup(interaction, reply_error, ephemeral=True)
            return

        # ✅ 응답(공개)
        msg = f"{interaction.user.mention} 수고하셨습니다! 오늘 {fmt_hhmm(studied_sec)} 공부함. (현재 티어: {tier} / 🔥 {streak}일 연속)"
        if deferred:
            await safe_followup(interaction, msg, ephemeral=False)

        async def after():
            async with store.lock:
                g2 = ensure_guild(store.data, interaction.guild.id)
            if log_text:
                await send_log_text(interaction.guild, g2, log_text)
            await update_dashboard(interaction.guild, g2, last_actor=interaction.user, force=True)

        schedule_after_response(after())

    @discord.ui.button(label="📊 내 정보", style=discord.ButtonStyle.secondary, custom_id="study:myinfo")
    async def myinfo(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        deferred = await safe_defer(interaction, ephemeral=True)

        now = now_kst()
        text = ""

        async with store.lock:
            data = store.data
            g = ensure_guild(data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)

            weekly_sec = int(u.get("weekly_total_sec", 0))
            tier = tier_from_weekly(weekly_sec)
            streak = int(u.get("streak", 0))
            st = status_label(u.get("status", "off"))

            current_session = 0
            if u.get("status") in ("work", "break"):
                current_session = calc_effective_study_sec(u, now)

            text = (
                f"**이름:** {u.get('name', interaction.user.display_name)}\n"
                f"**현재 상태:** {st}\n"
                f"**이번 주 누적:** {fmt_hhmm(weekly_sec)}\n"
                f"**현재 티어:** {tier}\n"
                f"**연속 출근:** 🔥 {streak}일\n"
            )
            if current_session > 0:
                text += f"**현재 세션 실공부:** {fmt_hhmm(current_session)}\n"

        if deferred:
            await safe_followup(interaction, text, ephemeral=True)


# ------------------------------------------------------------
# ✅ 명령어: !설치 (현황판)
# ------------------------------------------------------------
@bot.command(name="설치")
async def install_panel(ctx: commands.Context):
    if not ctx.guild:
        return

    async with store.lock:
        data = store.data
        g = ensure_guild(data, ctx.guild.id)
        ensure_week_current(g)

        # ✅ fetch_message 대신 PartialMessage로도 존재 여부를 확인할 수 있으나
        #    메시지가 삭제되었을 수도 있으니 "편집 시도"보단 기존 값으로 판단하지 않고,
        #    여기서는 기존 패널이 기록되어 있으면 안내만 합니다.
        if g.get("panel", {}).get("channel_id") and g.get("panel", {}).get("message_id"):
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
                await ctx.send("봇에 메시지 보내기/임베드/버튼 권한이 없습니다. 채널 권한을 확인해 주세요.")
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
        data = store.data
        g = ensure_guild(data, ctx.guild.id)
        g["log_channel_id"] = ch.id
        store.save_now_locked()

    await ctx.send(f"✅ 로그 채널이 설정되었습니다: {ch.mention}\n이제 출근/휴식/복귀/퇴근 이벤트가 모두 기록됩니다.")


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
        data = store.data
        g = ensure_guild(data, ctx.guild.id)
        g["settlement_channel_id"] = ch.id
        store.save_now_locked()

    await ctx.send(f"✅ 자동 주간정산 채널이 설정되었습니다: {ch.mention}\n(일요일 12:00 KST에 이 채널로 자동 출력)")


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
    current = "0시간 0분"

    async with store.lock:
        data = store.data
        g = ensure_guild(data, ctx.guild.id)
        ensure_week_current(g)
        u = ensure_user(g, member)

        u["weekly_total_sec"] = max(int(u.get("weekly_total_sec", 0)) + delta_sec, 0)
        store.save_now_locked()
        current = fmt_hhmm(int(u.get("weekly_total_sec", 0)))

    await ctx.send(
        f"✅ 시간 정정 완료: {member.display_name} / {fmt_hhmm(abs(delta_sec))} ({'추가' if delta_sec >= 0 else '차감'})\n"
        f"현재 주간 누적: {current}"
    )

    async def after():
        async with store.lock:
            g2 = ensure_guild(store.data, ctx.guild.id)
        await update_dashboard(ctx.guild, g2, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None, force=True)

    schedule_after_response(after())


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

    ch: Optional[discord.TextChannel]
    async with store.lock:
        data = store.data
        g = ensure_guild(data, ctx.guild.id)
        ch = get_settlement_channel(ctx.guild, g)

    if not ch:
        await ctx.send("정산 메시지를 보낼 채널을 찾지 못했습니다.")
        return

    await ctx.send("📌 수동 주간정산을 시작합니다...")

    async with store.lock:
        g = ensure_guild(store.data, ctx.guild.id)
        await run_weekly_settlement(ctx.guild, g, ch)
        g["last_settlement_week_start"] = g.get("week_start")
        store.save_now_locked()

    async def after():
        async with store.lock:
            g2 = ensure_guild(store.data, ctx.guild.id)
        await update_dashboard(ctx.guild, g2, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None, force=True)

    schedule_after_response(after())


# ------------------------------------------------------------
# ✅ 자동 주간정산: 일요일 12:00(KST)
# - tasks.loop(time=...)는 매일 해당 시각에 호출되므로, 일요일만 필터
# ------------------------------------------------------------
@tasks.loop(time=time(hour=12, minute=0, tzinfo=KST))
async def auto_weekly_settlement():
    if not bot.is_ready():
        return

    if now_kst().weekday() != 6:
        return

    for guild in bot.guilds:
        async with store.lock:
            data = store.data
            g = ensure_guild(data, guild.id)
            ensure_week_current(g)

            ws = g.get("week_start")
            if g.get("last_settlement_week_start") == ws:
                continue

            ch = get_settlement_channel(guild, g)
            if not ch:
                continue

        async with store.lock:
            g_live = ensure_guild(store.data, guild.id)
        await run_weekly_settlement(guild, g_live, ch)

        async with store.lock:
            g_save = ensure_guild(store.data, guild.id)
            g_save["last_settlement_week_start"] = g_save.get("week_start")
            store.save_now_locked()

        async with store.lock:
            g2 = ensure_guild(store.data, guild.id)
        await update_dashboard(guild, g2, last_actor=None, force=True)


@auto_weekly_settlement.before_loop
async def before_auto_weekly_settlement():
    await bot.wait_until_ready()


# ------------------------------------------------------------
# ✅ 현황판 조건부 갱신: 활동 있으면 1분, 없으면 5분
# - change_interval 대신 "내부 sleep 방식"으로 안정화(권장)
# ------------------------------------------------------------
async def dashboard_refresh_worker():
    await bot.wait_until_ready()

    while not bot.is_closed():
        try:
            # 1) 활동 여부 확인(락 짧게)
            async with store.lock:
                data = store.data
                for guild in bot.guilds:
                    g = ensure_guild(data, guild.id)
                    ensure_week_current(g)
                any_active = any(has_any_activity(ensure_guild(data, guild.id)) for guild in bot.guilds)

            # 2) 갱신 간격
            interval = 60 if any_active else 300

            # 3) 길드별 업데이트(락 짧게)
            for guild in bot.guilds:
                async with store.lock:
                    g = ensure_guild(store.data, guild.id)
                await update_dashboard(guild, g, last_actor=None, force=False)

            # 4) 저장(해시 등)
            async with store.lock:
                store.save_now_locked()

            await asyncio.sleep(interval)

        except Exception:
            await asyncio.sleep(10)


# ------------------------------------------------------------
# ✅ Koyeb Health Check 서버 (/health)
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


async def ping_self():
    """
    KOYEB_URL=https://xxxx.koyeb.app/health
    """
    await bot.wait_until_ready()

    url = os.getenv("KOYEB_URL", "").strip()
    if not url:
        return

    global http_session
    if http_session is None or http_session.closed:
        http_session = aiohttp.ClientSession()

    while not bot.is_closed():
        try:
            await http_session.get(url, timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass
        await asyncio.sleep(180)


# ------------------------------------------------------------
# ✅ on_ready: 초기화/복구/태스크 시작
# ------------------------------------------------------------
@bot.event
async def on_ready():
    bot.add_view(StudyView())  # persistent view

    # 웹서버/자가핑
    bot.loop.create_task(start_web_server())
    bot.loop.create_task(ping_self())

    # 데이터 1회 로드
    await store.load_once()

    # 자동 기능 시작
    if not auto_weekly_settlement.is_running():
        auto_weekly_settlement.start()

    # ✅ 안정화된 현황판 워커 시작(중복 방지)
    if not hasattr(bot, "_dash_worker_started"):
        bot._dash_worker_started = True
        bot.loop.create_task(dashboard_refresh_worker())

    # 재시작 시 패널 갱신(1회)
    for guild in bot.guilds:
        async with store.lock:
            g = ensure_guild(store.data, guild.id)
            ensure_week_current(g)
        await update_dashboard(guild, g, last_actor=None, force=True)

    async with store.lock:
        store.save_now_locked()

    print(f"✅ 로그인 완료: {bot.user} (서버 {len(bot.guilds)}개)")


@bot.event
async def on_close():
    global http_session
    if http_session and not http_session.closed:
        await http_session.close()


# ------------------------------------------------------------
# 실행
# ------------------------------------------------------------
if __name__ == "__main__":
    token = TOKEN.strip() or os.getenv("DISCORD_TOKEN", "").strip()
    if not token:
        print("⚠ TOKEN이 비어 있습니다. main.py 상단 TOKEN 또는 환경변수 DISCORD_TOKEN을 설정하세요.")
    else:
        bot.run(token)
