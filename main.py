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
# ✅ 데이터 저장소 (최적화 핵심)
# - 프로세스 내에서 JSON을 캐시로 유지하고, 변경 시 즉시 원자적(atomic) 저장
# - 매 버튼/명령마다 파일을 다시 읽지 않아도 됨(디스크 IO 감소)
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
        async with self.lock:
            self._atomic_save_sync()

    async def with_data(self):
        """
        async with store.with_data() as data: ... 처럼 쓰기 위한 컨텍스트
        """
        return self.lock  # 단순화: lock을 그대로 반환


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
            # ✅ 최적화: 마지막으로 만든 임베드 해시(동일하면 edit 생략)
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
# ✅ 로그 채널(이벤트/정산 결과) 전송
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


async def send_log(guild: discord.Guild, guild_data: Dict[str, Any], text: str):
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
# ✅ 대시보드(현황판) - edit 최소화 최적화(해시 비교)
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
        embed.set_footer(text="상태 확인: [📊 내 정보] 버튼 · 기준시간: KST")

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


async def update_dashboard(guild: discord.Guild, guild_data: Dict[str, Any], last_actor: Optional[discord.Member] = None, force: bool = False):
    """
    ✅ 최적화 포인트
    - 대시보드 description을 만들고 해시 비교
    - 동일하면 msg.edit 생략(디스코드 API 호출 감소)
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
# ✅ 버튼 UI(View) - persistent
# ------------------------------------------------------------
class StudyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="▶ 출근", style=discord.ButtonStyle.success, custom_id="study:checkin")
    async def checkin(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        async with await store.with_data():
            data = store.data
            g = ensure_guild(data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)

            now = now_kst()

            if u.get("status") == "work":
                await interaction.response.send_message("이미 출근(공부 중) 상태입니다.", ephemeral=True)
                return
            if u.get("status") == "break":
                await interaction.response.send_message("현재 휴식 중입니다. 휴식/복귀 버튼으로 복귀하거나 퇴근하세요.", ephemeral=True)
                return

            u["status"] = "work"
            u["start_time"] = dt_to_iso(now)
            u["break_start"] = None
            u["total_break_today"] = 0

            await store.save_now()

            await send_log(interaction.guild, g, make_log("checkin", interaction.user, now))
            await update_dashboard(interaction.guild, g, last_actor=interaction.user, force=True)

        await interaction.response.send_message("✅ 출근 완료!", ephemeral=True)

    @discord.ui.button(label="⏸ 휴식/복귀", style=discord.ButtonStyle.secondary, custom_id="study:toggle_break")
    async def toggle_break(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        async with await store.with_data():
            data = store.data
            g = ensure_guild(data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)

            now = now_kst()
            st = u.get("status", "off")

            if st == "off":
                await interaction.response.send_message("출근 후에 사용할 수 있습니다. 먼저 [▶ 출근]을 눌러주세요.", ephemeral=True)
                return

            if st == "work":
                u["status"] = "break"
                u["break_start"] = dt_to_iso(now)

                await store.save_now()
                await send_log(interaction.guild, g, make_log("break_start", interaction.user, now))
                await update_dashboard(interaction.guild, g, last_actor=interaction.user, force=True)

                await interaction.response.send_message("⏸ 휴식 시작!", ephemeral=True)
                return

            if st == "break":
                bs = iso_to_dt(u.get("break_start"))
                if bs:
                    delta = int((now - bs).total_seconds())
                else:
                    delta = 0

                u["total_break_today"] = int(u.get("total_break_today", 0)) + max(delta, 0)
                u["status"] = "work"
                u["break_start"] = None

                await store.save_now()
                await send_log(interaction.guild, g, make_log("break_end", interaction.user, now, break_sec=delta, total_break_today=u.get("total_break_today", 0)))
                await update_dashboard(interaction.guild, g, last_actor=interaction.user, force=True)

                await interaction.response.send_message(f"▶ 복귀 완료! (휴식 {fmt_hhmm(delta)})", ephemeral=True)
                return

            await interaction.response.send_message("알 수 없는 상태입니다.", ephemeral=True)

    @discord.ui.button(label="⏹ 퇴근", style=discord.ButtonStyle.danger, custom_id="study:checkout")
    async def checkout(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        async with await store.with_data():
            data = store.data
            g = ensure_guild(data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)

            now = now_kst()
            st = u.get("status", "off")

            if st == "off":
                await interaction.response.send_message("현재 대기 중입니다. 출근하지 않은 상태에서는 퇴근할 수 없습니다.", ephemeral=True)
                return

            # 휴식 중 퇴근 처리(휴식 시간 반영)
            if st == "break":
                bs = iso_to_dt(u.get("break_start"))
                if bs:
                    delta = int((now - bs).total_seconds())
                    u["total_break_today"] = int(u.get("total_break_today", 0)) + max(delta, 0)
                u["break_start"] = None

            studied_sec = calc_effective_study_sec(u, now)
            u["weekly_total_sec"] = int(u.get("weekly_total_sec", 0)) + studied_sec

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

            tier = tier_from_weekly(int(u.get("weekly_total_sec", 0)))
            streak = int(u.get("streak", 0))

            # 종료
            u["status"] = "off"
            u["start_time"] = None
            u["break_start"] = None
            u["total_break_today"] = 0

            await store.save_now()

            await send_log(
                interaction.guild,
                g,
                make_log(
                    "checkout",
                    interaction.user,
                    now,
                    studied_sec=studied_sec,
                    weekly_total_sec=u.get("weekly_total_sec", 0),
                    streak=streak,
                    tier=tier
                )
            )

            await update_dashboard(interaction.guild, g, last_actor=interaction.user, force=True)

        msg = f"{interaction.user.mention} 수고하셨습니다! 오늘 {fmt_hhmm(studied_sec)} 공부함. (현재 티어: {tier} / 🔥 {streak}일 연속)"
        await interaction.response.send_message(msg, ephemeral=False)

    @discord.ui.button(label="📊 내 정보", style=discord.ButtonStyle.secondary, custom_id="study:myinfo")
    async def myinfo(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        async with await store.with_data():
            data = store.data
            g = ensure_guild(data, interaction.guild.id)
            ensure_week_current(g)
            u = ensure_user(g, interaction.user)

            weekly_sec = int(u.get("weekly_total_sec", 0))
            tier = tier_from_weekly(weekly_sec)
            streak = int(u.get("streak", 0))
            st = status_label(u.get("status", "off"))

            now = now_kst()
            current_session = 0
            if u.get("status") in ("work", "break"):
                current_session = calc_effective_study_sec(u, now)

        info = (
            f"**이름:** {u.get('name', interaction.user.display_name)}\n"
            f"**현재 상태:** {st}\n"
            f"**이번 주 누적:** {fmt_hhmm(weekly_sec)}\n"
            f"**현재 티어:** {tier}\n"
            f"**연속 출근:** 🔥 {streak}일\n"
        )
        if current_session > 0:
            info += f"**현재 세션 실공부:** {fmt_hhmm(current_session)}\n"

        await interaction.response.send_message(info, ephemeral=True)


# ------------------------------------------------------------
# ✅ 명령어: !설치 (현황판)
# ------------------------------------------------------------
@bot.command(name="설치")
async def install_panel(ctx: commands.Context):
    if not ctx.guild:
        return

    async with await store.with_data():
        data = store.data
        g = ensure_guild(data, ctx.guild.id)
        ensure_week_current(g)

        old = await fetch_panel_message(ctx.guild, g)
        if old:
            await send_to_channel(ctx.channel if isinstance(ctx.channel, discord.TextChannel) else None,
                                 "이미 이 서버에 현황판이 설치되어 있습니다. (기존 메시지를 사용 중)")
            return

        embed = build_dashboard_embed(ctx.guild, g)
        try:
            msg = await ctx.send(embed=embed, view=StudyView())
        except discord.Forbidden:
            return

        g["panel"]["channel_id"] = msg.channel.id
        g["panel"]["message_id"] = msg.id
        g["dashboard_hash"] = dashboard_hash(build_dashboard_text(g))

        await store.save_now()

    await ctx.send("✅ 스터디 현황판을 설치했습니다!")


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

    async with await store.with_data():
        data = store.data
        g = ensure_guild(data, ctx.guild.id)
        g["log_channel_id"] = ch.id
        await store.save_now()

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

    async with await store.with_data():
        data = store.data
        g = ensure_guild(data, ctx.guild.id)
        g["settlement_channel_id"] = ch.id
        await store.save_now()

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

    async with await store.with_data():
        data = store.data
        g = ensure_guild(data, ctx.guild.id)
        ensure_week_current(g)
        u = ensure_user(g, member)

        u["weekly_total_sec"] = max(int(u.get("weekly_total_sec", 0)) + delta_sec, 0)
        await store.save_now()

        await update_dashboard(ctx.guild, g, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None, force=True)

        current = fmt_hhmm(int(u.get("weekly_total_sec", 0)))

    await ctx.send(
        f"✅ 시간 정정 완료: {member.display_name} / {fmt_hhmm(abs(delta_sec))} ({'추가' if delta_sec >= 0 else '차감'})\n"
        f"현재 주간 누적: {current}"
    )


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

    async with await store.with_data():
        data = store.data
        g = ensure_guild(data, ctx.guild.id)

        ch = get_settlement_channel(ctx.guild, g)
        if not ch:
            await ctx.send("정산 메시지를 보낼 채널을 찾지 못했습니다.")
            return

        await run_weekly_settlement(ctx.guild, g, ch)
        g["last_settlement_week_start"] = g.get("week_start")

        await store.save_now()
        await update_dashboard(ctx.guild, g, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None, force=True)


# ------------------------------------------------------------
# ✅ 자동 주간정산: 일요일 12:00(KST) 정확히 실행
# - per-minute poll 대신 time-based 스케줄로 최적화
# ------------------------------------------------------------
@tasks.loop(time=time(hour=12, minute=0, tzinfo=KST))
async def auto_weekly_settlement():
    async with await store.with_data():
        data = store.data

        for guild in bot.guilds:
            g = ensure_guild(data, guild.id)

            ensure_week_current(g)
            ws = g.get("week_start")
            if g.get("last_settlement_week_start") == ws:
                continue

            ch = get_settlement_channel(guild, g)
            if not ch:
                continue

            # ✅ 일요일 12:00에는 정확히 실행됨(weekday 검사)
            #    tasks.loop(time=...)는 매일 12:00에 호출되므로, 일요일만 걸러야 함
            if now_kst().weekday() != 6:
                continue

            await run_weekly_settlement(guild, g, ch)
            g["last_settlement_week_start"] = ws

            await update_dashboard(guild, g, last_actor=None, force=True)

        await store.save_now()


@auto_weekly_settlement.before_loop
async def before_auto_weekly_settlement():
    await bot.wait_until_ready()


# ------------------------------------------------------------
# ✅ 현황판 조건부 갱신: 활동 있으면 1분, 없으면 5분
# - 해시 비교로 edit 최소화
# ------------------------------------------------------------
@tasks.loop(seconds=60)
async def auto_dashboard_refresh():
    async with await store.with_data():
        data = store.data
        any_changed = False

        for guild in bot.guilds:
            g = ensure_guild(data, guild.id)

            if ensure_week_current(g):
                any_changed = True

            # force=False: 해시 같으면 edit 스킵
            await update_dashboard(guild, g, last_actor=None, force=False)

        if any_changed:
            await store.save_now()

        # 다음 interval 조절
        any_active = False
        for guild in bot.guilds:
            g = ensure_guild(data, guild.id)
            if has_any_activity(g):
                any_active = True
                break

    target_seconds = 60 if any_active else 300
    try:
        auto_dashboard_refresh.change_interval(seconds=target_seconds)
    except Exception:
        pass


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
    if not auto_dashboard_refresh.is_running():
        auto_dashboard_refresh.start()
    if not auto_weekly_settlement.is_running():
        auto_weekly_settlement.start()

    # 재시작 시 패널 복구(1회) - force=True로 정확히 갱신
    async with await store.with_data():
        data = store.data
        for guild in bot.guilds:
            g = ensure_guild(data, guild.id)
            ensure_week_current(g)
            await update_dashboard(guild, g, last_actor=None, force=True)

        await store.save_now()

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
