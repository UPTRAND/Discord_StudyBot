# main.py
# ------------------------------------------------------------
# ✅ 권장 설치 (Koyeb/Windows 공통)
#   python -m pip install -U discord.py tzdata aiohttp
#
# ✅ 실행
#   python main.py
#
# ✅ 디스코드에서 사용(최소)
# 1) !로그채널설정 #study-log     (반드시 먼저)
# 2) !설치                       (현황판 설치)
# 3) (선택) !정산채널설정 #ranking
#
# ✅ 이벤트 소싱(Event Sourcing) 구조
# - 유저 상태/누적/스트릭은 "오직 로그 채널 메시지"로만 저장됨
# - 재시작 시 로그를 다시 읽어서 100% 복구(replay)
# - study_data.json은 설정(panel/log/settlement)만 저장
#
# ✅ 이번 추가 사항
# 1) !패널복구 : “현재 채널의 마지막 봇 메시지 중 현황판을 찾아서 panel.message_id 재등록”
# 2) 현황판 멤버 표시 줄바꿈 적용
# 3) 로그 replay 최적화: “마지막 weekly_reset 이후부터”만 읽기 (없으면 이번 주 시작부터)
# ------------------------------------------------------------

import os
import json
import asyncio
import hashlib
from datetime import datetime, timedelta, date, timezone, time
from typing import Dict, Any, Optional, List, Tuple

import discord
from discord.ext import commands, tasks

import aiohttp
from aiohttp import web

from zoneinfo import ZoneInfo


# ------------------------------------------------------------
# ✅ 토큰 입력란 (요청대로 빈칸 유지)
# ------------------------------------------------------------
TOKEN = ""

DATA_FILE = "study_data.json"     # ✅ 설정만 저장(로그가 진짜 데이터)
LOG_PREFIX = "[STUDYLOG]"

try:
    KST = ZoneInfo("Asia/Seoul")
except Exception:
    KST = timezone(timedelta(hours=9), name="KST")

INTENTS = discord.Intents.default()
INTENTS.message_content = True

bot = commands.Bot(command_prefix="!", intents=INTENTS)

http_session: Optional[aiohttp.ClientSession] = None


# ------------------------------------------------------------
# ✅ 유틸(시간/포맷)
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


def safe_str(v: Any) -> str:
    return str(v).replace("\n", " ").replace(";", ",").strip()


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ------------------------------------------------------------
# ✅ 설정 저장소(파일) - 설정만 저장
# ------------------------------------------------------------
class ConfigStore:
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
        except Exception:
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


config = ConfigStore(DATA_FILE)


def ensure_guild_cfg(data: Dict[str, Any], guild_id: int) -> Dict[str, Any]:
    gid = str(guild_id)
    g = data["guilds"].get(gid)
    if not g:
        g = {
            "panel_channel_id": None,
            "panel_message_id": None,
            "log_channel_id": None,
            "settlement_channel_id": None,
            "dashboard_hash": None,
        }
        data["guilds"][gid] = g
    else:
        g.setdefault("panel_channel_id", None)
        g.setdefault("panel_message_id", None)
        g.setdefault("log_channel_id", None)
        g.setdefault("settlement_channel_id", None)
        g.setdefault("dashboard_hash", None)
    return g


# ------------------------------------------------------------
# ✅ 이벤트 소싱 상태(메모리)
# ------------------------------------------------------------
class UserState:
    __slots__ = (
        "name",
        "status",
        "start_time",
        "break_start",
        "total_break_today",
        "weekly_total_sec",
        "streak",
        "last_work_date",
    )

    def __init__(self, name: str):
        self.name = name
        self.status = "off"  # off / work / break
        self.start_time: Optional[datetime] = None
        self.break_start: Optional[datetime] = None
        self.total_break_today = 0
        self.weekly_total_sec = 0
        self.streak = 0
        self.last_work_date: Optional[str] = None


class GuildState:
    def __init__(self):
        self.users: Dict[int, UserState] = {}
        self.last_reset_ts: Optional[datetime] = None

    def ensure_user(self, user_id: int, name: str) -> UserState:
        u = self.users.get(user_id)
        if not u:
            u = UserState(name=name)
            self.users[user_id] = u
        else:
            u.name = name
        return u


STATE: Dict[int, GuildState] = {}
STATE_LOCK = asyncio.Lock()


def get_gstate(guild_id: int) -> GuildState:
    gs = STATE.get(guild_id)
    if not gs:
        gs = GuildState()
        STATE[guild_id] = gs
    return gs


# ------------------------------------------------------------
# ✅ 로그 포맷/파싱
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


def parse_log_line(content: str) -> Optional[Dict[str, str]]:
    content = content.strip()
    if not content.startswith(LOG_PREFIX):
        return None
    payload = content[len(LOG_PREFIX):].strip()
    if not payload:
        return None

    out: Dict[str, str] = {}
    parts = [p.strip() for p in payload.split(";")]
    for p in parts:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        out[k.strip()] = v.strip()

    if "action" not in out or "uid" not in out:
        return None
    return out


# ------------------------------------------------------------
# ✅ 권한 체크(관리자)
# ------------------------------------------------------------
def is_admin_member(member: discord.Member) -> bool:
    perms = member.guild_permissions
    return perms.administrator or perms.manage_guild


def is_admin_ctx(ctx: commands.Context) -> bool:
    return bool(ctx.guild and isinstance(ctx.author, discord.Member) and is_admin_member(ctx.author))


# ------------------------------------------------------------
# ✅ 채널 파서/선택
# ------------------------------------------------------------
def resolve_text_channel(guild: discord.Guild, raw: str) -> Optional[discord.TextChannel]:
    raw = raw.strip()

    if raw.startswith("<#") and raw.endswith(">"):
        cid = raw[2:-1]
        if cid.isdigit():
            ch = guild.get_channel(int(cid))
            return ch if isinstance(ch, discord.TextChannel) else None

    if raw.isdigit():
        ch = guild.get_channel(int(raw))
        return ch if isinstance(ch, discord.TextChannel) else None

    name = raw.lstrip("#")
    for ch in guild.text_channels:
        if ch.name == name:
            return ch

    return None


def get_log_channel(guild: discord.Guild, cfg: Dict[str, Any]) -> Optional[discord.TextChannel]:
    cid = cfg.get("log_channel_id")
    if not cid:
        return None
    ch = guild.get_channel(int(cid))
    return ch if isinstance(ch, discord.TextChannel) else None


def get_settlement_channel(guild: discord.Guild, cfg: Dict[str, Any]) -> Optional[discord.TextChannel]:
    cid = cfg.get("settlement_channel_id")
    if cid:
        ch = guild.get_channel(int(cid))
        if isinstance(ch, discord.TextChannel):
            return ch

    pch = cfg.get("panel_channel_id")
    if pch:
        ch = guild.get_channel(int(pch))
        if isinstance(ch, discord.TextChannel):
            return ch

    logch = get_log_channel(guild, cfg)
    if logch:
        return logch

    return guild.text_channels[0] if guild.text_channels else None


# ------------------------------------------------------------
# ✅ 안전한 interaction defer / followup
# ------------------------------------------------------------
async def safe_defer(interaction: discord.Interaction, *, ephemeral: bool = False, thinking: bool = False) -> bool:
    try:
        if interaction.response.is_done():
            return True
        await interaction.response.defer(ephemeral=ephemeral, thinking=thinking)
        return True
    except Exception:
        return False


async def safe_followup(interaction: discord.Interaction, content: str, *, ephemeral: bool = False) -> bool:
    try:
        await interaction.followup.send(content, ephemeral=ephemeral)
        return True
    except Exception:
        return False


def schedule(coro):
    try:
        asyncio.create_task(coro)
    except Exception:
        pass


# ------------------------------------------------------------
# ✅ 이벤트 소싱: 로그 전송(필수) + replay
# ------------------------------------------------------------
async def append_log_event(guild: discord.Guild, cfg: Dict[str, Any], text: str) -> bool:
    ch = get_log_channel(guild, cfg)
    if not ch:
        return False
    try:
        await ch.send(text)
        return True
    except Exception:
        return False


def calc_effective_study_sec(u: UserState, now: datetime) -> int:
    if not u.start_time:
        return 0
    total_break = int(u.total_break_today)
    if u.status == "break" and u.break_start:
        total_break += int((now - u.break_start).total_seconds())
    total = int((now - u.start_time).total_seconds()) - total_break
    return max(total, 0)


def apply_event(gs: GuildState, ev: Dict[str, str]):
    action = ev.get("action", "")
    uid_str = ev.get("uid", "0") or "0"
    try:
        uid = int(uid_str)
    except Exception:
        uid = 0
    name = ev.get("name", "알 수 없음")
    ts = iso_to_dt(ev.get("ts")) or now_kst()

    # weekly_reset은 uid=0이라도 상태 반영 필요
    if action == "weekly_reset":
        gs.last_reset_ts = ts
        for _u in gs.users.values():
            _u.weekly_total_sec = 0
        return

    # 일반 유저 이벤트
    u = gs.ensure_user(uid, name)

    if action == "checkin":
        u.status = "work"
        u.start_time = ts
        u.break_start = None
        u.total_break_today = 0

    elif action == "break_start":
        if u.status == "work" and u.start_time:
            u.status = "break"
            u.break_start = ts

    elif action == "break_end":
        if u.status == "break" and u.start_time:
            if u.break_start:
                delta = int((ts - u.break_start).total_seconds())
                u.total_break_today += max(delta, 0)
            u.status = "work"
            u.break_start = None

    elif action == "checkout":
        studied_sec = 0
        if "studied_sec" in ev:
            try:
                studied_sec = int(float(ev["studied_sec"]))
            except Exception:
                studied_sec = 0
        else:
            studied_sec = calc_effective_study_sec(u, ts)

        u.weekly_total_sec = max(int(u.weekly_total_sec) + max(studied_sec, 0), 0)

        today_s = ts.date().isoformat()
        yday_s = (ts.date() - timedelta(days=1)).isoformat()
        last = u.last_work_date

        if last == yday_s:
            u.streak = int(u.streak) + 1
        elif last == today_s:
            u.streak = int(u.streak)
        else:
            u.streak = 1

        u.last_work_date = today_s

        u.status = "off"
        u.start_time = None
        u.break_start = None
        u.total_break_today = 0

    elif action == "time_adjust":
        # 이 이벤트는 보통 "관리자(행위자)" uid로 찍히고, target_uid가 따로 있음.
        # replay에서는 target_uid를 우선 적용.
        target_uid = None
        if "target_uid" in ev:
            try:
                target_uid = int(ev["target_uid"])
            except Exception:
                target_uid = None

        delta = 0
        if "delta_sec" in ev:
            try:
                delta = int(float(ev["delta_sec"]))
            except Exception:
                delta = 0

        if target_uid is not None:
            tu = gs.ensure_user(target_uid, ev.get("target_name", "알 수 없음"))
            tu.weekly_total_sec = max(int(tu.weekly_total_sec) + delta, 0)
        else:
            u.weekly_total_sec = max(int(u.weekly_total_sec) + delta, 0)


async def replay_from_logs(guild: discord.Guild, cfg: Dict[str, Any]) -> GuildState:
    """
    ✅ 최적화:
    - 최신부터 훑다가 "마지막 weekly_reset"을 만나면 거기서 중단
    - weekly_reset이 없으면 "이번 주 시작(월 00:00 KST)"부터만 replay
    """
    logch = get_log_channel(guild, cfg)
    gs = GuildState()
    if not logch:
        return gs

    # fallback 기준(weekly_reset 못 찾았을 때)
    today = now_kst().date()
    ws = week_start_kst(today)
    week_start_dt = datetime(ws.year, ws.month, ws.day, 0, 0, 0, tzinfo=KST) - timedelta(hours=1)

    events: List[Dict[str, str]] = []
    found_reset = False

    try:
        async for msg in logch.history(limit=3000, oldest_first=False):
            ev = parse_log_line(msg.content)
            if not ev:
                continue

            ts = iso_to_dt(ev.get("ts")) or msg.created_at.astimezone(KST)
            if "ts" not in ev or not ev["ts"]:
                ev["ts"] = dt_to_iso(ts)

            # weekly_reset을 찾으면 포함하고 중단(그 이후 이벤트만 의미있게)
            if ev.get("action") == "weekly_reset":
                events.append(ev)
                found_reset = True
                break

            # reset을 아직 못 찾았으면 fallback 조건 적용(이번 주 이전이면 중단)
            if not found_reset and ts < week_start_dt:
                break

            events.append(ev)

    except Exception:
        return gs

    # oldest_first로 적용
    for ev in reversed(events):
        apply_event(gs, ev)

    return gs


# ------------------------------------------------------------
# ✅ 대시보드(현황판)
# ------------------------------------------------------------
def build_dashboard_text(gs: GuildState) -> str:
    now = now_kst()
    work_lines: List[str] = []
    break_lines: List[str] = []

    for u in gs.users.values():
        if u.status == "work":
            sec = calc_effective_study_sec(u, now)
            work_lines.append(f"🟢 {u.name} ({fmt_hhmm(sec)}째)")
        elif u.status == "break":
            break_lines.append(f"🟡 {u.name} (휴식 중)")

    lines = work_lines + break_lines
    if not lines:
        return "지금 공부 중인 사람이 없습니다.\n\n버튼으로 출근해서 스터디를 시작해 보세요."

    # ✅ (요청 2) 줄바꿈 적용
    return "\n".join(lines)


def build_dashboard_embed(guild: discord.Guild, gs: GuildState, last_actor: Optional[discord.Member] = None) -> discord.Embed:
    now = now_kst()
    desc = build_dashboard_text(gs)

    embed = discord.Embed(
        title="📅 스터디 현황판",
        description=desc,
        color=discord.Color.blurple(),
        timestamp=now
    )

    if last_actor:
        u = gs.users.get(last_actor.id)
        st = status_label(u.status if u else "off")
        embed.set_footer(text=f"최근 조작: {last_actor.display_name} · 내 상태: {st} · 기준시간: KST")
    else:
        embed.set_footer(text="상태 확인: [📊 내 정보] 버튼 · 기준시간: KST")

    return embed


async def fetch_panel_message(guild: discord.Guild, cfg: Dict[str, Any]) -> Optional[discord.Message]:
    ch_id = cfg.get("panel_channel_id")
    msg_id = cfg.get("panel_message_id")
    if not ch_id or not msg_id:
        return None
    ch = guild.get_channel(int(ch_id))
    if not isinstance(ch, discord.TextChannel):
        return None
    try:
        return await ch.fetch_message(int(msg_id))
    except Exception:
        return None


async def update_dashboard(guild: discord.Guild, cfg: Dict[str, Any], *, force: bool = False, last_actor: Optional[discord.Member] = None):
    msg = await fetch_panel_message(guild, cfg)
    if not msg:
        return

    async with STATE_LOCK:
        gs = get_gstate(guild.id)
        desc = build_dashboard_text(gs)

    h = sha256(desc)
    if (not force) and cfg.get("dashboard_hash") == h:
        return

    cfg["dashboard_hash"] = h
    embed = build_dashboard_embed(guild, gs, last_actor=last_actor)
    try:
        await msg.edit(embed=embed, view=StudyView())
    except Exception:
        pass


def has_any_activity(gs: GuildState) -> bool:
    for u in gs.users.values():
        if u.status in ("work", "break"):
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

        await safe_defer(interaction, ephemeral=True)

        guild = interaction.guild
        member = interaction.user
        now = now_kst()

        async with config.lock:
            cfg = ensure_guild_cfg(config.data, guild.id)
            if not get_log_channel(guild, cfg):
                await safe_followup(interaction, "❌ 로그 채널이 설정되어 있지 않습니다. `!로그채널설정 #채널`을 먼저 해주세요.", ephemeral=True)
                return

        async with STATE_LOCK:
            gs = get_gstate(guild.id)
            u = gs.ensure_user(member.id, member.display_name)
            if u.status == "work":
                await safe_followup(interaction, "이미 출근(공부 중) 상태입니다.", ephemeral=True)
                return
            if u.status == "break":
                await safe_followup(interaction, "현재 휴식 중입니다. 휴식/복귀 버튼으로 복귀하거나 퇴근하세요.", ephemeral=True)
                return

        log_text = make_log("checkin", member, now)
        ok = await append_log_event(guild, cfg, log_text)
        if not ok:
            await safe_followup(interaction, "❌ 로그 채널에 기록하지 못했습니다. 데이터 안정성을 위해 출근 처리를 중단했습니다.", ephemeral=True)
            return

        async with STATE_LOCK:
            gs = get_gstate(guild.id)
            apply_event(gs, {"action": "checkin", "uid": str(member.id), "name": member.display_name, "ts": dt_to_iso(now)})

        await safe_followup(interaction, "✅ 출근 완료!", ephemeral=True)

        async def after():
            async with config.lock:
                cfg2 = ensure_guild_cfg(config.data, guild.id)
                await update_dashboard(guild, cfg2, force=True, last_actor=member)
                config.save_now_locked()

        schedule(after())

    @discord.ui.button(label="⏸ 휴식/복귀", style=discord.ButtonStyle.secondary, custom_id="study:toggle_break")
    async def toggle_break(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        await safe_defer(interaction, ephemeral=True)

        guild = interaction.guild
        member = interaction.user
        now = now_kst()

        async with config.lock:
            cfg = ensure_guild_cfg(config.data, guild.id)
            if not get_log_channel(guild, cfg):
                await safe_followup(interaction, "❌ 로그 채널이 설정되어 있지 않습니다. `!로그채널설정 #채널`을 먼저 해주세요.", ephemeral=True)
                return

        async with STATE_LOCK:
            gs = get_gstate(guild.id)
            u = gs.ensure_user(member.id, member.display_name)
            st = u.status

        if st == "off":
            await safe_followup(interaction, "출근 후에 사용할 수 있습니다. 먼저 [▶ 출근]을 눌러주세요.", ephemeral=True)
            return

        if st == "work":
            log_text = make_log("break_start", member, now)
            ok = await append_log_event(guild, cfg, log_text)
            if not ok:
                await safe_followup(interaction, "❌ 로그 채널 기록 실패 → 휴식 처리를 중단했습니다.", ephemeral=True)
                return

            async with STATE_LOCK:
                gs = get_gstate(guild.id)
                apply_event(gs, {"action": "break_start", "uid": str(member.id), "name": member.display_name, "ts": dt_to_iso(now)})

            await safe_followup(interaction, "⏸ 휴식 시작!", ephemeral=True)

        elif st == "break":
            # 안내용 휴식시간 계산
            delta = 0
            async with STATE_LOCK:
                gs = get_gstate(guild.id)
                u2 = gs.ensure_user(member.id, member.display_name)
                if u2.break_start:
                    delta = int((now - u2.break_start).total_seconds())

            log_text = make_log("break_end", member, now)
            ok = await append_log_event(guild, cfg, log_text)
            if not ok:
                await safe_followup(interaction, "❌ 로그 채널 기록 실패 → 복귀 처리를 중단했습니다.", ephemeral=True)
                return

            async with STATE_LOCK:
                gs = get_gstate(guild.id)
                apply_event(gs, {"action": "break_end", "uid": str(member.id), "name": member.display_name, "ts": dt_to_iso(now)})

            await safe_followup(interaction, f"▶ 복귀 완료! (휴식 {fmt_hhmm(delta)})", ephemeral=True)

        else:
            await safe_followup(interaction, "알 수 없는 상태입니다.", ephemeral=True)
            return

        async def after():
            async with config.lock:
                cfg2 = ensure_guild_cfg(config.data, guild.id)
                await update_dashboard(guild, cfg2, force=True, last_actor=member)
                config.save_now_locked()

        schedule(after())

    @discord.ui.button(label="⏹ 퇴근", style=discord.ButtonStyle.danger, custom_id="study:checkout")
    async def checkout(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        await safe_defer(interaction, thinking=True)

        guild = interaction.guild
        member = interaction.user
        now = now_kst()

        async with config.lock:
            cfg = ensure_guild_cfg(config.data, guild.id)
            if not get_log_channel(guild, cfg):
                await safe_followup(interaction, "❌ 로그 채널이 설정되어 있지 않습니다. `!로그채널설정 #채널`을 먼저 해주세요.", ephemeral=True)
                return

        async with STATE_LOCK:
            gs = get_gstate(guild.id)
            u = gs.ensure_user(member.id, member.display_name)
            if u.status == "off":
                await safe_followup(interaction, "현재 대기 중입니다. 출근하지 않은 상태에서는 퇴근할 수 없습니다.", ephemeral=True)
                return
            studied_sec = calc_effective_study_sec(u, now)

        log_text = make_log("checkout", member, now, studied_sec=studied_sec)
        ok = await append_log_event(guild, cfg, log_text)
        if not ok:
            await safe_followup(interaction, "❌ 로그 채널 기록 실패 → 퇴근 처리를 중단했습니다.", ephemeral=True)
            return

        async with STATE_LOCK:
            gs = get_gstate(guild.id)
            apply_event(gs, {"action": "checkout", "uid": str(member.id), "name": member.display_name, "ts": dt_to_iso(now), "studied_sec": str(studied_sec)})
            u2 = gs.users.get(member.id)
            weekly_after = u2.weekly_total_sec if u2 else studied_sec
            streak = u2.streak if u2 else 1
            tier = tier_from_weekly(weekly_after)

        msg = f"{member.mention} 수고하셨습니다! 오늘 {fmt_hhmm(studied_sec)} 공부함. (현재 티어: {tier} / 🔥 {streak}일 연속)"
        await safe_followup(interaction, msg, ephemeral=False)

        async def after():
            async with config.lock:
                cfg2 = ensure_guild_cfg(config.data, guild.id)
                await update_dashboard(guild, cfg2, force=True, last_actor=member)
                config.save_now_locked()

        schedule(after())

    @discord.ui.button(label="📊 내 정보", style=discord.ButtonStyle.secondary, custom_id="study:myinfo")
    async def myinfo(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        await safe_defer(interaction, ephemeral=True)

        member = interaction.user
        now = now_kst()

        async with STATE_LOCK:
            gs = get_gstate(interaction.guild.id)
            u = gs.ensure_user(member.id, member.display_name)
            weekly_sec = int(u.weekly_total_sec)
            tier = tier_from_weekly(weekly_sec)
            streak = int(u.streak)
            st = status_label(u.status)
            current_session = calc_effective_study_sec(u, now) if u.status in ("work", "break") else 0

        text = (
            f"**이름:** {member.display_name}\n"
            f"**현재 상태:** {st}\n"
            f"**이번 주 누적:** {fmt_hhmm(weekly_sec)}\n"
            f"**현재 티어:** {tier}\n"
            f"**연속 출근:** 🔥 {streak}일\n"
        )
        if current_session > 0:
            text += f"**현재 세션 실공부:** {fmt_hhmm(current_session)}\n"

        await safe_followup(interaction, text, ephemeral=True)


# ------------------------------------------------------------
# ✅ 명령어: !로그채널설정 (반드시 먼저)
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

    async with config.lock:
        cfg = ensure_guild_cfg(config.data, ctx.guild.id)
        cfg["log_channel_id"] = ch.id
        config.save_now_locked()

    await ctx.send(
        f"✅ 로그 채널이 설정되었습니다: {ch.mention}\n"
        f"재시작 시 로그를 Replay하여 상태가 복구됩니다."
    )

    async def after():
        async with config.lock:
            cfg2 = ensure_guild_cfg(config.data, ctx.guild.id)
        gs = await replay_from_logs(ctx.guild, cfg2)
        async with STATE_LOCK:
            STATE[ctx.guild.id] = gs

    schedule(after())


# ------------------------------------------------------------
# ✅ 명령어: !정산채널설정
# ------------------------------------------------------------
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

    async with config.lock:
        cfg = ensure_guild_cfg(config.data, ctx.guild.id)
        cfg["settlement_channel_id"] = ch.id
        config.save_now_locked()

    await ctx.send(f"✅ 자동 주간정산 채널이 설정되었습니다: {ch.mention}\n(일요일 12:00 KST에 자동 출력)")


# ------------------------------------------------------------
# ✅ 명령어: !설치 (현황판)
# ------------------------------------------------------------
@bot.command(name="설치")
async def install_panel(ctx: commands.Context):
    if not ctx.guild:
        return

    async with config.lock:
        cfg = ensure_guild_cfg(config.data, ctx.guild.id)
        if not cfg.get("log_channel_id"):
            await ctx.send("❌ 먼저 `!로그채널설정 #채널`을 실행해 로그 채널을 지정해 주세요.")
            return

    async with config.lock:
        cfg = ensure_guild_cfg(config.data, ctx.guild.id)
    old = await fetch_panel_message(ctx.guild, cfg)
    if old:
        await ctx.send("이미 현황판이 설치되어 있습니다. (기존 메시지를 사용 중)")
        await update_dashboard(ctx.guild, cfg, force=True)
        async with config.lock:
            config.save_now_locked()
        return

    async with STATE_LOCK:
        gs = get_gstate(ctx.guild.id)
        embed = build_dashboard_embed(ctx.guild, gs)

    try:
        msg = await ctx.send(embed=embed, view=StudyView())
    except discord.Forbidden:
        await ctx.send("봇에 메시지 보내기/임베드/버튼 권한이 없습니다. 채널 권한을 확인해 주세요.")
        return

    async with config.lock:
        cfg = ensure_guild_cfg(config.data, ctx.guild.id)
        cfg["panel_channel_id"] = msg.channel.id
        cfg["panel_message_id"] = msg.id
        cfg["dashboard_hash"] = sha256(build_dashboard_text(get_gstate(ctx.guild.id)))
        config.save_now_locked()

    await ctx.send("✅ 스터디 현황판을 설치했습니다!")


# ------------------------------------------------------------
# ✅ (추가 1) 관리자 명령: !패널복구
# - “현재 채널의 마지막 봇 메시지 중 현황판을 찾아서 panel.message_id 재등록”
# ------------------------------------------------------------
@bot.command(name="패널복구")
async def panel_recover(ctx: commands.Context):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return
    if not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("이 명령어는 텍스트 채널에서만 사용할 수 있습니다.")
        return

    me = ctx.guild.me
    if not me:
        await ctx.send("봇 정보를 가져오지 못했습니다.")
        return

    found: Optional[discord.Message] = None

    # 최근 메시지에서 “봇이 보낸 것 + 임베드 제목이 현황판”을 찾음
    try:
        async for msg in ctx.channel.history(limit=200, oldest_first=False):
            if msg.author.id != me.id:
                continue
            if not msg.embeds:
                continue
            e = msg.embeds[0]
            if (e.title or "").strip() == "📅 스터디 현황판":
                found = msg
                break
    except Exception:
        await ctx.send("최근 메시지 검색에 실패했습니다. (권한: 메시지 기록 보기 확인)")
        return

    if not found:
        await ctx.send("현재 채널에서 현황판 메시지를 찾지 못했습니다. (최근 200개 범위)")
        return

    async with config.lock:
        cfg = ensure_guild_cfg(config.data, ctx.guild.id)
        cfg["panel_channel_id"] = ctx.channel.id
        cfg["panel_message_id"] = found.id
        # 해시는 갱신 시 업데이트됨
        config.save_now_locked()

    await ctx.send(f"✅ 패널 복구 완료: 이 채널의 메시지(ID={found.id})를 현황판으로 재등록했습니다.")

    # 즉시 갱신(버튼 다시 붙이고 최신 상태 표시)
    async def after():
        # replay도 한 번 수행(로그 기준 상태가 최신이도록)
        async with config.lock:
            cfg2 = ensure_guild_cfg(config.data, ctx.guild.id)
        if get_log_channel(ctx.guild, cfg2):
            gs = await replay_from_logs(ctx.guild, cfg2)
            async with STATE_LOCK:
                STATE[ctx.guild.id] = gs

        async with config.lock:
            cfg3 = ensure_guild_cfg(config.data, ctx.guild.id)
            await update_dashboard(ctx.guild, cfg3, force=True, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None)
            config.save_now_locked()

    schedule(after())


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
    now = now_kst()

    async with config.lock:
        cfg = ensure_guild_cfg(config.data, ctx.guild.id)
        if not get_log_channel(ctx.guild, cfg):
            await ctx.send("❌ 로그 채널이 설정되어 있지 않습니다. 먼저 `!로그채널설정`을 해주세요.")
            return

    log_text = make_log(
        "time_adjust",
        ctx.author if isinstance(ctx.author, discord.Member) else member,
        now,
        target_uid=member.id,
        target_name=member.display_name,
        delta_sec=delta_sec
    )
    ok = await append_log_event(ctx.guild, cfg, log_text)
    if not ok:
        await ctx.send("❌ 로그 채널 기록 실패 → 시간정정을 중단했습니다.")
        return

    async with STATE_LOCK:
        gs = get_gstate(ctx.guild.id)
        u = gs.ensure_user(member.id, member.display_name)
        u.weekly_total_sec = max(int(u.weekly_total_sec) + delta_sec, 0)
        current = fmt_hhmm(u.weekly_total_sec)

    await ctx.send(
        f"✅ 시간 정정 완료: {member.display_name} / {fmt_hhmm(abs(delta_sec))} ({'추가' if delta_sec >= 0 else '차감'})\n"
        f"현재 주간 누적: {current}"
    )

    async def after():
        async with config.lock:
            cfg2 = ensure_guild_cfg(config.data, ctx.guild.id)
            await update_dashboard(ctx.guild, cfg2, force=True, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None)
            config.save_now_locked()

    schedule(after())


# ------------------------------------------------------------
# ✅ 주간정산 출력 생성/실행
# ------------------------------------------------------------
async def send_to_channel(channel: Optional[discord.TextChannel], content: str):
    if not channel:
        return
    try:
        await channel.send(content)
    except Exception:
        pass


def build_weekly_ranking_lines(gs: GuildState) -> Tuple[str, Optional[str]]:
    users = list(gs.users.values())
    users.sort(key=lambda u: int(u.weekly_total_sec), reverse=True)

    if not users or all(int(u.weekly_total_sec) == 0 for u in users):
        return ("이번 주 누적 기록이 없습니다.", None)

    top_sec = max(int(users[0].weekly_total_sec), 1)
    lines: List[str] = []
    rank = 1
    for u in users:
        sec = int(u.weekly_total_sec)
        if sec <= 0:
            continue
        bar_len = max(int((sec / top_sec) * 20), 1)
        lines.append(f"{rank}등 {u.name} {'■'*bar_len} ({sec/3600:.1f}시간)")
        rank += 1
        if rank > 20:
            break

    ranking_msg = "**📊 이번 주 스터디 랭킹**\n" + "\n".join(lines)
    reset_msg = "✅ 주간 정산이 완료되어 이번 주 누적 시간이 초기화되었습니다."
    return ranking_msg, reset_msg


async def run_weekly_settlement(guild: discord.Guild, cfg: Dict[str, Any]):
    settle_ch = get_settlement_channel(guild, cfg)
    if not settle_ch:
        return
    if not get_log_channel(guild, cfg):
        return

    async with STATE_LOCK:
        gs = get_gstate(guild.id)
        ranking_msg, reset_msg = build_weekly_ranking_lines(gs)

    announce = "📌 **이번 주 종료!** 지금부터 주간정산을 시작합니다."
    await send_to_channel(settle_ch, announce)
    await send_to_channel(settle_ch, ranking_msg)
    if reset_msg:
        await send_to_channel(settle_ch, reset_msg)

    logch = get_log_channel(guild, cfg)
    if logch and logch.id != settle_ch.id:
        await send_to_channel(logch, announce)
        await send_to_channel(logch, ranking_msg)
        if reset_msg:
            await send_to_channel(logch, reset_msg)

    now = now_kst()
    reset_event_text = f"{LOG_PREFIX} action=weekly_reset; uid=0; name=SYSTEM; ts={dt_to_iso(now)}"
    ok = await append_log_event(guild, cfg, reset_event_text)
    if not ok:
        await send_to_channel(settle_ch, "⚠ 주간 리셋 이벤트 기록에 실패했습니다. (로그 채널 권한/상태 확인 필요)")
        return

    async with STATE_LOCK:
        gs = get_gstate(guild.id)
        apply_event(gs, {"action": "weekly_reset", "uid": "0", "name": "SYSTEM", "ts": dt_to_iso(now)})


@bot.command(name="주간정산")
async def weekly_settlement_cmd(ctx: commands.Context):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    async with config.lock:
        cfg = ensure_guild_cfg(config.data, ctx.guild.id)
        if not get_log_channel(ctx.guild, cfg):
            await ctx.send("❌ 로그 채널이 설정되어 있지 않습니다. 먼저 `!로그채널설정`을 해주세요.")
            return

    await ctx.send("📌 수동 주간정산을 시작합니다...")
    await run_weekly_settlement(ctx.guild, cfg)

    async def after():
        async with config.lock:
            cfg2 = ensure_guild_cfg(config.data, ctx.guild.id)
            await update_dashboard(ctx.guild, cfg2, force=True, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None)
            config.save_now_locked()

    schedule(after())


# ------------------------------------------------------------
# ✅ 자동 주간정산: 일요일 12:00(KST)
# ------------------------------------------------------------
@tasks.loop(time=time(hour=12, minute=0, tzinfo=KST))
async def auto_weekly_settlement():
    if not bot.is_ready():
        return
    if now_kst().weekday() != 6:
        return

    for guild in bot.guilds:
        async with config.lock:
            cfg = ensure_guild_cfg(config.data, guild.id)
            if not get_log_channel(guild, cfg):
                continue

        await run_weekly_settlement(guild, cfg)

        async with config.lock:
            cfg2 = ensure_guild_cfg(config.data, guild.id)
            await update_dashboard(guild, cfg2, force=True)
            config.save_now_locked()


@auto_weekly_settlement.before_loop
async def before_auto_weekly_settlement():
    await bot.wait_until_ready()


# ------------------------------------------------------------
# ✅ 현황판 조건부 갱신: 활동 있으면 1분, 없으면 5분
# ------------------------------------------------------------
@tasks.loop(seconds=60)
async def auto_dashboard_refresh():
    if not bot.is_ready():
        return

    any_active = False
    async with STATE_LOCK:
        for g in bot.guilds:
            gs = get_gstate(g.id)
            if has_any_activity(gs):
                any_active = True
                break

    for guild in bot.guilds:
        async with config.lock:
            cfg = ensure_guild_cfg(config.data, guild.id)
        await update_dashboard(guild, cfg, force=False)

    target = 60 if any_active else 300
    try:
        auto_dashboard_refresh.change_interval(seconds=target)
    except Exception:
        pass

    async with config.lock:
        config.save_now_locked()


@auto_dashboard_refresh.before_loop
async def before_auto_dashboard_refresh():
    await bot.wait_until_ready()


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
# ✅ on_ready: 설정 로드 + 로그 replay + 태스크 시작 + 패널 갱신
# ------------------------------------------------------------
@bot.event
async def on_ready():
    bot.add_view(StudyView())

    bot.loop.create_task(start_web_server())
    bot.loop.create_task(ping_self())

    await config.load_once()

    # ✅ 로그 replay(최적화: 마지막 weekly_reset 이후부터)
    for guild in bot.guilds:
        async with config.lock:
            cfg = ensure_guild_cfg(config.data, guild.id)
        if not get_log_channel(guild, cfg):
            continue
        gs = await replay_from_logs(guild, cfg)
        async with STATE_LOCK:
            STATE[guild.id] = gs

    if not auto_dashboard_refresh.is_running():
        auto_dashboard_refresh.start()
    if not auto_weekly_settlement.is_running():
        auto_weekly_settlement.start()

    # 패널이 있으면 강제 갱신(재시작 직후 1회)
    for guild in bot.guilds:
        async with config.lock:
            cfg = ensure_guild_cfg(config.data, guild.id)
        await update_dashboard(guild, cfg, force=True)

    async with config.lock:
        config.save_now_locked()

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
