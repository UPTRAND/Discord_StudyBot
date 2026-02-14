# main.py
# ------------------------------------------------------------
# ✅ 권장 설치 (Koyeb/Windows 공통)
#   python -m pip install -U discord.py tzdata aiohttp
#
# ✅ 실행
#   python main.py
#
# ✅ Koyeb 배포용(velog 글 방식)
# - Health Check: GET /health  -> "OK"
# - Scale-to-zero 방지: KOYEB_URL(예: https://xxxx.koyeb.app/health)로 주기적 ping
# - 토큰은 환경변수 DISCORD_TOKEN 사용 권장 (코드 TOKEN은 빈칸 유지)
#
# ✅ 디스코드에서 사용
# 1) !설치
# 2) !로그채널설정 #study-log   (채널 멘션으로 입력 권장)
# ------------------------------------------------------------

import os
import json
import asyncio
from datetime import datetime, timedelta, date, timezone
from typing import Dict, Any, Optional

import discord
from discord.ext import commands

import aiohttp
from aiohttp import web

from zoneinfo import ZoneInfo


# ------------------------------------------------------------
# ✅ 토큰 입력란 (요청대로 빈칸 유지)
#    실제 배포/운영은 환경변수 DISCORD_TOKEN 사용 권장
# ------------------------------------------------------------
TOKEN = ""

DATA_FILE = "study_data.json"

# ✅ KST (Windows에서 tzdata 없으면 실패할 수 있어 안전장치 포함)
try:
    KST = ZoneInfo("Asia/Seoul")
except Exception:
    KST = timezone(timedelta(hours=9), name="KST")

INTENTS = discord.Intents.default()
INTENTS.message_content = True  # !설치 등 접두사 명령어 사용

bot = commands.Bot(command_prefix="!", intents=INTENTS)

# JSON 파일 동시 접근 보호
data_lock = asyncio.Lock()

# 로그 포맷 프리픽스 (파싱/복구에 사용)
LOG_PREFIX = "[STUDYLOG]"


# ------------------------------------------------------------
# 시간/포맷 유틸
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


# ------------------------------------------------------------
# JSON 로드/세이브 (없으면 자동 생성)
# ------------------------------------------------------------
def ensure_data_file():
    if not os.path.exists(DATA_FILE):
        base = {"version": 1, "guilds": {}}
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(base, f, ensure_ascii=False, indent=2)


def load_data_sync() -> Dict[str, Any]:
    ensure_data_file()
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {"version": 1, "guilds": {}}

    if "guilds" not in data:
        data["guilds"] = {}
    return data


def save_data_sync(data: Dict[str, Any]):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


async def load_data() -> Dict[str, Any]:
    async with data_lock:
        return load_data_sync()


async def save_data(data: Dict[str, Any]):
    async with data_lock:
        save_data_sync(data)


# ------------------------------------------------------------
# 길드/유저 기본 구조 보장
# ------------------------------------------------------------
def ensure_guild(data: Dict[str, Any], guild_id: int) -> Dict[str, Any]:
    gid = str(guild_id)
    if gid not in data["guilds"]:
        today = now_kst().date()
        data["guilds"][gid] = {
            "week_start": week_start_kst(today).isoformat(),
            "panel": {"channel_id": None, "message_id": None},
            "log_channel_id": None,  # ✅ 로그 채널
            "users": {}
        }
    else:
        g = data["guilds"][gid]
        if "panel" not in g:
            g["panel"] = {"channel_id": None, "message_id": None}
        if "log_channel_id" not in g:
            g["log_channel_id"] = None
        if "users" not in g:
            g["users"] = {}
        if "week_start" not in g:
            today = now_kst().date()
            g["week_start"] = week_start_kst(today).isoformat()
    return data["guilds"][gid]


def ensure_week_current(guild_data: Dict[str, Any]) -> bool:
    """
    주가 바뀌면 weekly_total_sec를 자동으로 0으로 리셋.
    """
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
    if uid not in users:
        users[uid] = {
            "name": member.display_name,
            "status": "off",              # work / break / off
            "start_time": None,           # iso
            "break_start": None,          # iso
            "total_break_today": 0,       # 초
            "weekly_total_sec": 0,        # 초
            "streak": 0,
            "last_work_date": None        # YYYY-MM-DD
        }
    else:
        users[uid]["name"] = member.display_name
    return users[uid]


def calc_effective_study_sec(user: Dict[str, Any], now: datetime) -> int:
    """
    실공부(초) = (now - start_time) - (누적휴식 + 현재휴식중이면 now-break_start)
    """
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


# ------------------------------------------------------------
# ✅ 로그 시스템: 출근/휴식/복귀/퇴근을 항상 로그 채널에 남김
# ------------------------------------------------------------
def safe_str(s: Any) -> str:
    return str(s).replace("\n", " ").replace(";", ",").strip()


def make_log(action: str, member: discord.Member, ts: datetime, **fields) -> str:
    """
    파싱 친화 포맷: [STUDYLOG] key=value; key=value; ...
    """
    base = {
        "action": action,
        "uid": str(member.id),
        "name": safe_str(member.display_name),
        "ts": dt_to_iso(ts)
    }
    for k, v in fields.items():
        base[k] = safe_str(v)

    parts = [f"{k}={base[k]}" for k in base]
    return f"{LOG_PREFIX} " + "; ".join(parts)


async def send_log(guild: discord.Guild, guild_data: Dict[str, Any], text: str):
    ch_id = guild_data.get("log_channel_id")
    if not ch_id:
        return
    channel = guild.get_channel(int(ch_id))
    if channel and isinstance(channel, discord.TextChannel):
        try:
            await channel.send(text)
        except Exception:
            pass


def parse_log_line(content: str) -> Optional[Dict[str, str]]:
    """
    [STUDYLOG] action=...; uid=...; ts=...; ...
    """
    if not content.startswith(LOG_PREFIX):
        return None
    try:
        body = content[len(LOG_PREFIX):].strip()
        pairs = [p.strip() for p in body.split(";")]
        out: Dict[str, str] = {}
        for p in pairs:
            if not p or "=" not in p:
                continue
            k, v = p.split("=", 1)
            out[k.strip()] = v.strip()
        if "action" not in out or "uid" not in out or "ts" not in out:
            return None
        return out
    except Exception:
        return None


# ------------------------------------------------------------
# 대시보드(고정 패널) 구성/수정
# ------------------------------------------------------------
def build_dashboard_embed(
    guild: discord.Guild,
    guild_data: Dict[str, Any],
    last_actor: Optional[discord.Member] = None
) -> discord.Embed:
    now = now_kst()
    embed = discord.Embed(
        title="📅 스터디 현황판",
        description="",
        color=discord.Color.blurple(),
        timestamp=now
    )

    work_lines = []
    break_lines = []

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
        embed.description = "지금 공부 중인 사람이 없습니다.\n\n버튼으로 출근해서 스터디를 시작해 보세요."
    else:
        embed.description = " | ".join(lines)

    # 공유 임베드에서 "내 상태"를 개인별로 표기할 수 없어서 최근 조작자 기준으로 표시
    if last_actor:
        u = ensure_user(guild_data, last_actor)
        embed.set_footer(text=f"최근 조작: {u.get('name', last_actor.display_name)} · 내 상태: {status_label(u.get('status','off'))} · 기준시간: KST")
    else:
        embed.set_footer(text="상태 확인: [📊 내 정보] 버튼 · 기준시간: KST")

    return embed


async def fetch_panel_message(guild: discord.Guild, guild_data: Dict[str, Any]) -> Optional[discord.Message]:
    panel = guild_data.get("panel", {})
    ch_id = panel.get("channel_id")
    msg_id = panel.get("message_id")
    if not ch_id or not msg_id:
        return None
    channel = guild.get_channel(int(ch_id))
    if not channel or not isinstance(channel, discord.TextChannel):
        return None
    try:
        return await channel.fetch_message(int(msg_id))
    except Exception:
        return None


async def update_dashboard(guild: discord.Guild, guild_data: Dict[str, Any], last_actor: Optional[discord.Member] = None):
    msg = await fetch_panel_message(guild, guild_data)
    if not msg:
        return
    embed = build_dashboard_embed(guild, guild_data, last_actor=last_actor)
    try:
        await msg.edit(embed=embed, view=StudyView())
    except Exception:
        pass


# ------------------------------------------------------------
# 권한 체크(관리자)
# ------------------------------------------------------------
def is_admin_member(member: discord.Member) -> bool:
    perms = member.guild_permissions
    return perms.administrator or perms.manage_guild


def is_admin_ctx(ctx: commands.Context) -> bool:
    if not ctx.guild or not isinstance(ctx.author, discord.Member):
        return False
    return is_admin_member(ctx.author)


# ------------------------------------------------------------
# 버튼 UI(View) - persistent
# ------------------------------------------------------------
class StudyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="▶ 출근", style=discord.ButtonStyle.success, custom_id="study:checkin")
    async def checkin(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        data = await load_data()
        g = ensure_guild(data, interaction.guild.id)
        ensure_week_current(g)

        user = ensure_user(g, interaction.user)
        now = now_kst()

        if user.get("status") == "work":
            await interaction.response.send_message("이미 출근(공부 중) 상태입니다.", ephemeral=True)
            return
        if user.get("status") == "break":
            await interaction.response.send_message("현재 휴식 중입니다. 휴식/복귀 버튼으로 복귀하거나 퇴근하세요.", ephemeral=True)
            return

        user["status"] = "work"
        user["start_time"] = dt_to_iso(now)
        user["break_start"] = None
        user["total_break_today"] = 0

        await save_data(data)

        # ✅ 로그
        await send_log(interaction.guild, g, make_log("checkin", interaction.user, now))

        await update_dashboard(interaction.guild, g, last_actor=interaction.user)
        await interaction.response.send_message("✅ 출근 완료!", ephemeral=True)

    @discord.ui.button(label="⏸ 휴식/복귀", style=discord.ButtonStyle.secondary, custom_id="study:toggle_break")
    async def toggle_break(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        data = await load_data()
        g = ensure_guild(data, interaction.guild.id)
        ensure_week_current(g)

        user = ensure_user(g, interaction.user)
        now = now_kst()
        st = user.get("status", "off")

        if st == "off":
            await interaction.response.send_message("출근 후에 사용할 수 있습니다. 먼저 [▶ 출근]을 눌러주세요.", ephemeral=True)
            return

        if st == "work":
            user["status"] = "break"
            user["break_start"] = dt_to_iso(now)
            await save_data(data)

            # ✅ 로그
            await send_log(interaction.guild, g, make_log("break_start", interaction.user, now))

            await update_dashboard(interaction.guild, g, last_actor=interaction.user)
            await interaction.response.send_message("⏸ 휴식 시작!", ephemeral=True)
            return

        if st == "break":
            bs = iso_to_dt(user.get("break_start"))
            if not bs:
                user["status"] = "work"
                user["break_start"] = None
                await save_data(data)

                await send_log(interaction.guild, g, make_log("break_end", interaction.user, now, break_sec=0, total_break_today=user.get("total_break_today", 0)))
                await update_dashboard(interaction.guild, g, last_actor=interaction.user)
                await interaction.response.send_message("▶ 복귀 처리했습니다. (휴식 시작 시간이 없어 0분 처리)", ephemeral=True)
                return

            delta = int((now - bs).total_seconds())
            user["total_break_today"] = int(user.get("total_break_today", 0)) + max(delta, 0)
            user["status"] = "work"
            user["break_start"] = None
            await save_data(data)

            # ✅ 로그
            await send_log(interaction.guild, g, make_log("break_end", interaction.user, now, break_sec=delta, total_break_today=user.get("total_break_today", 0)))

            await update_dashboard(interaction.guild, g, last_actor=interaction.user)
            await interaction.response.send_message(f"▶ 복귀 완료! (휴식 {fmt_hhmm(delta)})", ephemeral=True)
            return

        await interaction.response.send_message("알 수 없는 상태입니다.", ephemeral=True)

    @discord.ui.button(label="⏹ 퇴근", style=discord.ButtonStyle.danger, custom_id="study:checkout")
    async def checkout(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        data = await load_data()
        g = ensure_guild(data, interaction.guild.id)
        ensure_week_current(g)

        user = ensure_user(g, interaction.user)
        now = now_kst()

        st = user.get("status", "off")
        if st == "off":
            await interaction.response.send_message("현재 대기 중입니다. 출근하지 않은 상태에서는 퇴근할 수 없습니다.", ephemeral=True)
            return

        if st == "break":
            bs = iso_to_dt(user.get("break_start"))
            if bs:
                delta = int((now - bs).total_seconds())
                user["total_break_today"] = int(user.get("total_break_today", 0)) + max(delta, 0)
            user["break_start"] = None

        studied_sec = calc_effective_study_sec(user, now)

        user["weekly_total_sec"] = int(user.get("weekly_total_sec", 0)) + studied_sec

        today_s = now.date().isoformat()
        yday_s = (now.date() - timedelta(days=1)).isoformat()
        last = user.get("last_work_date")

        if last == yday_s:
            user["streak"] = int(user.get("streak", 0)) + 1
        elif last == today_s:
            user["streak"] = int(user.get("streak", 0))
        else:
            user["streak"] = 1

        user["last_work_date"] = today_s

        tier = tier_from_weekly(int(user.get("weekly_total_sec", 0)))
        streak = int(user.get("streak", 0))

        # 종료 처리
        user["status"] = "off"
        user["start_time"] = None
        user["break_start"] = None
        user["total_break_today"] = 0

        await save_data(data)

        # ✅ 로그(결과값 포함)
        await send_log(
            interaction.guild,
            g,
            make_log(
                "checkout",
                interaction.user,
                now,
                studied_sec=studied_sec,
                weekly_total_sec=user.get("weekly_total_sec", 0),
                streak=streak,
                tier=tier
            )
        )

        await update_dashboard(interaction.guild, g, last_actor=interaction.user)

        msg = f"{interaction.user.mention} 수고하셨습니다! 오늘 {fmt_hhmm(studied_sec)} 공부함. (현재 티어: {tier} / 🔥 {streak}일 연속)"
        await interaction.response.send_message(msg, ephemeral=False)

    @discord.ui.button(label="📊 내 정보", style=discord.ButtonStyle.secondary, custom_id="study:myinfo")
    async def myinfo(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        data = await load_data()
        g = ensure_guild(data, interaction.guild.id)
        changed = ensure_week_current(g)

        user = ensure_user(g, interaction.user)
        if changed:
            await save_data(data)

        weekly_sec = int(user.get("weekly_total_sec", 0))
        tier = tier_from_weekly(weekly_sec)
        streak = int(user.get("streak", 0))
        st = status_label(user.get("status", "off"))

        now = now_kst()
        current_session = 0
        if user.get("status") in ("work", "break"):
            current_session = calc_effective_study_sec(user, now)

        info = (
            f"**이름:** {user.get('name', interaction.user.display_name)}\n"
            f"**현재 상태:** {st}\n"
            f"**이번 주 누적:** {fmt_hhmm(weekly_sec)}\n"
            f"**현재 티어:** {tier}\n"
            f"**연속 출근:** 🔥 {streak}일\n"
        )
        if current_session > 0:
            info += f"**현재 세션 실공부:** {fmt_hhmm(current_session)}\n"

        await interaction.response.send_message(info, ephemeral=True)


# ------------------------------------------------------------
# 명령어: !설치 (고정 패널)
# - ctx.reply 대신 ctx.send 사용 (권한/참조 문제를 줄이기 위함)
# ------------------------------------------------------------
@bot.command(name="설치")
async def install_panel(ctx: commands.Context):
    if not ctx.guild:
        return

    data = await load_data()
    g = ensure_guild(data, ctx.guild.id)
    ensure_week_current(g)

    old = await fetch_panel_message(ctx.guild, g)
    if old:
        # ✅ reply는 reference 권한이 꼬일 수 있어 send로 고정
        try:
            await ctx.send("이미 이 서버에 현황판이 설치되어 있습니다. (기존 메시지를 사용 중)")
        except discord.Forbidden:
            # 채널에 보낼 권한 자체가 없으면 여기서 끝
            pass
        return

    embed = build_dashboard_embed(ctx.guild, g)
    try:
        msg = await ctx.send(embed=embed, view=StudyView())
    except discord.Forbidden:
        # 권한 부족 안내(보낼 권한이 없다면 이 메시지도 못 보냄)
        return

    g["panel"]["channel_id"] = msg.channel.id
    g["panel"]["message_id"] = msg.id

    await save_data(data)
    try:
        await ctx.send("✅ 스터디 현황판을 설치했습니다!")
    except discord.Forbidden:
        pass


# ------------------------------------------------------------
# 로그 채널 설정: 채널 멘션(#채널) 입력 권장
# - "#study-log" 같은 문자열을 그냥 치면 변환이 실패할 수 있어 보완:
#   !로그채널설정 #study-log  (멘션/자동완성)
#   !로그채널설정 study-log   (이름만)
# ------------------------------------------------------------
def resolve_text_channel(guild: discord.Guild, raw: str) -> Optional[discord.TextChannel]:
    raw = raw.strip()

    # <#1234567890> 형태(멘션) 처리
    if raw.startswith("<#") and raw.endswith(">"):
        cid = raw[2:-1]
        if cid.isdigit():
            ch = guild.get_channel(int(cid))
            if isinstance(ch, discord.TextChannel):
                return ch

    # 숫자 ID 직접 입력 처리
    if raw.isdigit():
        ch = guild.get_channel(int(raw))
        if isinstance(ch, discord.TextChannel):
            return ch

    # 이름으로 찾기
    name = raw.lstrip("#")
    for ch in guild.text_channels:
        if ch.name == name:
            return ch

    return None


@bot.command(name="로그채널설정")
async def set_log_channel(ctx: commands.Context, channel_arg: str):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    ch = resolve_text_channel(ctx.guild, channel_arg)
    if not ch:
        await ctx.send("채널을 찾지 못했습니다. `!로그채널설정 #채널`처럼 채널 멘션(자동완성)으로 입력해 주세요.")
        return

    data = await load_data()
    g = ensure_guild(data, ctx.guild.id)
    g["log_channel_id"] = ch.id
    await save_data(data)

    await ctx.send(f"✅ 로그 채널이 설정되었습니다: {ch.mention}\n이제 출근/휴식/복귀/퇴근 이벤트가 모두 기록됩니다.")


@bot.command(name="로그채널해제")
async def unset_log_channel(ctx: commands.Context):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    data = await load_data()
    g = ensure_guild(data, ctx.guild.id)
    g["log_channel_id"] = None
    await save_data(data)
    await ctx.send("✅ 로그 채널 설정을 해제했습니다.")


# ------------------------------------------------------------
# 관리자 명령어: !시간정정 @유저 [시간]
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

    data = await load_data()
    g = ensure_guild(data, ctx.guild.id)
    ensure_week_current(g)

    user = ensure_user(g, member)
    user["weekly_total_sec"] = max(int(user.get("weekly_total_sec", 0)) + delta_sec, 0)

    await save_data(data)
    await update_dashboard(ctx.guild, g, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None)

    await ctx.send(
        f"✅ 시간 정정 완료: {member.display_name} / "
        f"{fmt_hhmm(abs(delta_sec))} ({'추가' if delta_sec >= 0 else '차감'})\n"
        f"현재 주간 누적: {fmt_hhmm(int(user.get('weekly_total_sec', 0)))}"
    )


# ------------------------------------------------------------
# 관리자 명령어: !주간정산 (텍스트 막대 그래프 출력 후 주간 누적 초기화)
# ------------------------------------------------------------
@bot.command(name="주간정산")
async def weekly_settlement(ctx: commands.Context):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    data = await load_data()
    g = ensure_guild(data, ctx.guild.id)
    ensure_week_current(g)

    users = list(g["users"].values())
    users.sort(key=lambda u: int(u.get("weekly_total_sec", 0)), reverse=True)

    if not users or all(int(u.get("weekly_total_sec", 0)) == 0 for u in users):
        for u in g["users"].values():
            u["weekly_total_sec"] = 0
        await save_data(data)
        await update_dashboard(ctx.guild, g, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None)
        await ctx.send("이번 주 누적 기록이 없습니다. (초기화 완료)")
        return

    top_sec = max(int(users[0].get("weekly_total_sec", 0)), 1)

    lines = []
    rank = 1
    for u in users:
        sec = int(u.get("weekly_total_sec", 0))
        if sec <= 0:
            continue

        bar_len = int((sec / top_sec) * 20)
        bar_len = max(bar_len, 1)
        bar = "■" * bar_len
        lines.append(f"{rank}등 {u.get('name','?')} {bar} ({sec/3600:.1f}시간)")
        rank += 1
        if rank > 20:
            break

    await ctx.send("**📊 이번 주 스터디 랭킹**\n" + "\n".join(lines))

    for u in g["users"].values():
        u["weekly_total_sec"] = 0

    await save_data(data)
    await update_dashboard(ctx.guild, g, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None)
    await ctx.send("✅ 주간 정산이 완료되어 이번 주 누적 시간이 초기화되었습니다.")


# ------------------------------------------------------------
# (선택) 관리자 명령어: !로그복구 [일수]
# - 로그 채널의 checkout 로그를 읽어 "이번 주" 누적/스트릭을 재구성
# - Koyeb에서 파일이 날아가도 복구 가능하게 하는 핵심 안전장치
# ------------------------------------------------------------
@bot.command(name="로그복구")
async def recover_from_logs(ctx: commands.Context, days: int = 30):
    if not ctx.guild:
        return
    if not is_admin_ctx(ctx):
        await ctx.send("이 명령어는 관리자만 사용할 수 있습니다.")
        return

    if days < 1:
        days = 1
    if days > 180:
        days = 180

    data = await load_data()
    g = ensure_guild(data, ctx.guild.id)

    log_ch_id = g.get("log_channel_id")
    if not log_ch_id:
        await ctx.send("로그 채널이 설정되어 있지 않습니다. 먼저 `!로그채널설정 #채널`을 실행하세요.")
        return

    log_channel = ctx.guild.get_channel(int(log_ch_id))
    if not log_channel or not isinstance(log_channel, discord.TextChannel):
        await ctx.send("로그 채널을 찾을 수 없습니다. 설정을 확인하세요.")
        return

    today = now_kst().date()
    ws = week_start_kst(today)
    ws_dt = datetime(ws.year, ws.month, ws.day, 0, 0, 0, tzinfo=KST)

    since_dt = now_kst() - timedelta(days=days)

    weekly_sec_by_uid: Dict[str, int] = {}
    checkout_dates: Dict[str, set] = {}

    async for msg in log_channel.history(limit=None, after=since_dt):
        parsed = parse_log_line(msg.content)
        if not parsed:
            continue

        ts = iso_to_dt(parsed.get("ts"))
        if not ts:
            continue
        if ts < ws_dt:
            continue

        if parsed.get("action") != "checkout":
            continue

        uid = parsed["uid"]
        try:
            studied_sec = int(float(parsed.get("studied_sec", "0")))
        except Exception:
            studied_sec = 0

        weekly_sec_by_uid[uid] = weekly_sec_by_uid.get(uid, 0) + max(studied_sec, 0)
        checkout_dates.setdefault(uid, set()).add(ts.date().isoformat())

    def compute_streak(dates_set: set, ref: date) -> int:
        streak = 0
        cur = ref
        while cur.isoformat() in dates_set:
            streak += 1
            cur = cur - timedelta(days=1)
        return streak

    for uid, sec in weekly_sec_by_uid.items():
        member = ctx.guild.get_member(int(uid))
        if member:
            u = ensure_user(g, member)
        else:
            users = g["users"]
            if uid not in users:
                users[uid] = {
                    "name": f"User({uid})",
                    "status": "off",
                    "start_time": None,
                    "break_start": None,
                    "total_break_today": 0,
                    "weekly_total_sec": 0,
                    "streak": 0,
                    "last_work_date": None
                }
            u = users[uid]

        u["weekly_total_sec"] = int(sec)

        dset = checkout_dates.get(uid, set())
        u["streak"] = compute_streak(dset, now_kst().date())
        u["last_work_date"] = max(dset) if dset else None

        # 실행 중 세션은 복구에서 끊어버림(로그 기준 확정값이 checkout이기 때문)
        u["status"] = "off"
        u["start_time"] = None
        u["break_start"] = None
        u["total_break_today"] = 0

    await save_data(data)
    await update_dashboard(ctx.guild, g, last_actor=ctx.author if isinstance(ctx.author, discord.Member) else None)

    await ctx.send(
        f"✅ 로그 기반 복구 완료\n"
        f"- 탐색: 최근 {days}일\n"
        f"- 대상: 이번 주({ws.isoformat()}~) checkout 로그\n"
        f"- 복구 유저: {len(weekly_sec_by_uid)}명"
    )


# ------------------------------------------------------------
# ✅ Koyeb Health Check 서버 (/health)
# ------------------------------------------------------------
async def health_check(request: web.Request):
    return web.Response(text="OK", status=200)


async def start_web_server():
    """
    Koyeb에서 Health check path=/health로 설정하면 안정적으로 살아있음 체크 가능
    """
    app = web.Application()
    app.router.add_get("/health", health_check)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "8000"))  # Koyeb 환경변수 PORT 우선
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()


async def ping_self():
    """
    Koyeb free의 scale-to-zero를 막고 싶다면
    환경변수 KOYEB_URL을 아래처럼 지정:
      KOYEB_URL=https://xxxx.koyeb.app/health
    """
    await bot.wait_until_ready()

    url = os.getenv("KOYEB_URL", "").strip()
    if not url:
        return

    while not bot.is_closed():
        try:
            async with aiohttp.ClientSession() as session:
                await session.get(url, timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass

        await asyncio.sleep(180)  # 3분마다 ping


# ------------------------------------------------------------
# 시작 시: persistent view 등록 + 패널 복구 + 웹서버/자가핑 시작
# ------------------------------------------------------------
@bot.event
async def on_ready():
    bot.add_view(StudyView())  # ✅ 재시작 후에도 버튼 작동

    # ✅ Koyeb health server + self ping (URL 있으면)
    bot.loop.create_task(start_web_server())
    bot.loop.create_task(ping_self())

    data = await load_data()
    changed_any = False

    for guild in bot.guilds:
        g = ensure_guild(data, guild.id)
        if ensure_week_current(g):
            changed_any = True

        msg = await fetch_panel_message(guild, g)
        if msg:
            try:
                embed = build_dashboard_embed(guild, g)
                await msg.edit(embed=embed, view=StudyView())
            except Exception:
                pass

    if changed_any:
        await save_data(data)

    print(f"✅ 로그인 완료: {bot.user} (서버 {len(bot.guilds)}개)")


# ------------------------------------------------------------
# 실행
# ------------------------------------------------------------
if __name__ == "__main__":
    # ✅ 로컬 TOKEN이 비어 있으면 환경변수 DISCORD_TOKEN 사용
    token = TOKEN.strip() or os.getenv("DISCORD_TOKEN", "").strip()

    if not token:
        print("⚠ TOKEN이 비어 있습니다. main.py 상단 TOKEN 또는 환경변수 DISCORD_TOKEN을 설정하세요.")
    else:
        bot.run(token)
