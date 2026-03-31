"""
Discord-бот з повноцінним приватним меню для кожного користувача.

Як це працює:
- Юзер пише /start у загальному каналі
- Бот відповідає ТІЛЬКИ ЙОМУ (ephemeral=True) — інші не бачать
- У відповіді — інтерактивне меню з кнопками
- Кожен юзер має свій ізольований стан (сесія прив'язана до user.id)
- Стани не перетинаються між юзерами

Залежності:
    pip install discord.py aiosqlite
"""

import asyncio
import aiosqlite
import discord
import time
from datetime import datetime, timedelta, timezone
from discord import app_commands
from discord.ext import commands

# =========================================================
# CONFIG
# =========================================================
import os
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS = {int(os.environ.get("ADMIN_IDS", "0"))}
NOTIFY_CHANNEL_ID = int(os.environ.get("NOTIFY_CHANNEL_ID", "0"))
GATHER_CHANNEL_ID = int(os.environ.get("GATHER_CHANNEL_ID", "0"))
WELCOME_CHANNEL_ID = 1487848644781805789  # канал з кнопкою реєстрації

RANK_NAMES = {
    1: "1 | Кандидат", 2: "2 | Резидент", 3: "3 | Оператор",
    4: "4 | Основний склад",  5: "5 | Старший склад",       6: "Наставник",
    7: "Куратор", 8: "Партнер",
    9: "Співзасновник",   10: "Засновник",
}
DB_NAME = "/data/database.db"

# Назва ролі "1 | Кандидат" на сервері (має збігатись точно)
PASSENGER_ROLE_NAME = "1 | Кандидат"


def rank_label(r: int) -> str:
    return f"{r} — {RANK_NAMES.get(r, '?')}"


def parse_static_ids(raw: str) -> list[str]:
    ids = [x.strip() for x in raw.split(",") if x.strip()]
    return list(dict.fromkeys(ids))


# =========================================================
# PER-USER SESSION  (ізольований стан)
# =========================================================
_sessions: dict[int, dict] = {}

def sess(uid: int) -> dict:
    if uid not in _sessions:
        _sessions[uid] = {}
    return _sessions[uid]

def sess_clear(uid: int):
    _sessions[uid] = {}


# =========================================================
# LAST CONTRACT MEMORY (зберігається до наступного контракту)
# =========================================================
_last_contracts: dict[int, dict] = {}

def save_last_contract(discord_id: int, contract_id: int, title: str, participants: list):
    _last_contracts[discord_id] = {
        "contract_id": contract_id,
        "title": title,
        "participants": participants,  # list of static_ids
    }

def get_last_contract(discord_id: int) -> dict | None:
    return _last_contracts.get(discord_id)


# =========================================================
# DATABASE
# =========================================================
def db():
    return aiosqlite.connect(DB_NAME)


async def init_db():
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        await cx.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            discord_id INTEGER UNIQUE,
            username TEXT,
            game_name TEXT UNIQUE,
            static_id TEXT UNIQUE,
            real_name TEXT,
            rank INTEGER DEFAULT 1,
            balance INTEGER DEFAULT 0,
            contracts_count INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS contract_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT UNIQUE,
            price INTEGER NOT NULL,
            level INTEGER DEFAULT 1,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS family_bank (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            balance INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS completed_contracts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contract_type_id INTEGER,
            contract_title TEXT,
            total_amount INTEGER,
            family_amount INTEGER,
            per_user_amount INTEGER,
            created_by_discord_id INTEGER,
            participants_count INTEGER,
            participants_text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS completed_contract_participants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            completed_contract_id INTEGER,
            discord_id INTEGER,
            game_name TEXT,
            static_id TEXT,
            payout_amount INTEGER
        );
        CREATE TABLE IF NOT EXISTS withdrawal_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            discord_id INTEGER,
            game_name TEXT,
            amount INTEGER,
            status TEXT DEFAULT 'new',
            reviewed_by INTEGER,
            reviewed_at TIMESTAMP,
            paid_by INTEGER,
            paid_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT OR IGNORE INTO family_bank (id, balance) VALUES (1, 0);
        """)
        await cx.commit()


# ---------- users ----------
async def user_by_did(did: int, only_active=True):
    q = "SELECT * FROM users WHERE discord_id=?" + (" AND is_active=1" if only_active else "")
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        return await (await cx.execute(q, (did,))).fetchone()


async def user_by_sid(sid: str, only_active=True):
    q = "SELECT * FROM users WHERE static_id=?" + (" AND is_active=1" if only_active else "")
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        return await (await cx.execute(q, (sid,))).fetchone()


async def all_active_users():
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        return await (await cx.execute(
            "SELECT * FROM users WHERE is_active=1 ORDER BY rank DESC, game_name ASC"
        )).fetchall()


async def add_user(data: dict):
    async with db() as cx:
        await cx.execute(
            "INSERT INTO users (discord_id,username,game_name,static_id,real_name) VALUES(?,?,?,?,?)",
            (data["discord_id"], data["username"], data["game_name"], data["static_id"], data["real_name"]),
        )
        await cx.commit()


async def set_rank(sid: str, rank: int):
    async with db() as cx:
        await cx.execute("UPDATE users SET rank=? WHERE static_id=? AND is_active=1", (rank, sid))
        await cx.commit()


async def deactivate_user(sid: str):
    async with db() as cx:
        await cx.execute("UPDATE users SET is_active=0 WHERE static_id=? AND is_active=1", (sid,))
        await cx.commit()


async def deduct_balance(did: int, amount: int):
    async with db() as cx:
        await cx.execute(
            "UPDATE users SET balance=balance-? WHERE discord_id=? AND is_active=1", (amount, did)
        )
        await cx.commit()


# ---------- contract types ----------
async def all_contract_types(level: int = None):
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        if level:
            rows = await (await cx.execute(
                "SELECT * FROM contract_types WHERE is_active=1 AND level=? ORDER BY id", (level,)
            )).fetchall()
        else:
            rows = await (await cx.execute(
                "SELECT * FROM contract_types WHERE is_active=1 ORDER BY level, id"
            )).fetchall()
        return rows


async def contract_type_by_id(cid: int):
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        return await (await cx.execute(
            "SELECT * FROM contract_types WHERE id=? AND is_active=1", (cid,)
        )).fetchone()


async def add_contract_type(title: str, price: int, level: int):
    async with db() as cx:
        await cx.execute("INSERT INTO contract_types(title,price,level) VALUES(?,?,?)", (title, price, level))
        await cx.commit()


async def deactivate_contract_type(cid: int):
    async with db() as cx:
        await cx.execute("UPDATE contract_types SET is_active=0 WHERE id=?", (cid,))
        await cx.commit()


# ---------- bank / withdrawals ----------
async def family_balance():
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        row = await (await cx.execute("SELECT balance FROM family_bank WHERE id=1")).fetchone()
        return row["balance"] if row else 0


async def create_wd(did: int, game_name: str, amount: int) -> int:
    async with db() as cx:
        cur = await cx.execute(
            "INSERT INTO withdrawal_requests(discord_id,game_name,amount) VALUES(?,?,?)",
            (did, game_name, amount),
        )
        await cx.commit()
        return cur.lastrowid


async def wd_by_id(wid: int):
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        return await (await cx.execute("SELECT * FROM withdrawal_requests WHERE id=?", (wid,))).fetchone()


async def wd_set_status(wid: int, status: str, by: int):
    async with db() as cx:
        await cx.execute(
            "UPDATE withdrawal_requests SET status=?,reviewed_by=?,reviewed_at=CURRENT_TIMESTAMP WHERE id=?",
            (status, by, wid),
        )
        await cx.commit()


async def wd_set_paid(wid: int, by: int):
    async with db() as cx:
        await cx.execute(
            "UPDATE withdrawal_requests SET status='paid',paid_by=?,paid_at=CURRENT_TIMESTAMP WHERE id=?",
            (by, wid),
        )
        await cx.commit()


async def all_wd(limit=30):
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        return await (await cx.execute(
            "SELECT * FROM withdrawal_requests ORDER BY id DESC LIMIT ?", (limit,)
        )).fetchall()


# =========================================================
# DB: ТОП КОНТРАКТІВ
# =========================================================
def get_week_bounds() -> tuple[str, str]:
    """Повертає (початок_нд, кінець_нд) поточного тижня у форматі ISO."""
    now = datetime.now(timezone.utc)
    # Знаходимо минулу неділю (weekday: 6=нд)
    days_since_sunday = (now.weekday() + 1) % 7
    start = (now - timedelta(days=days_since_sunday)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = start + timedelta(days=7)
    return start.isoformat(), end.isoformat()


async def get_top_alltime(limit: int = 10):
    """Топ гравців за всіма часами — за кількістю контрактів."""
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        return await (await cx.execute("""
            SELECT u.game_name, u.static_id, u.contracts_count
            FROM users u
            WHERE u.is_active = 1
            ORDER BY u.contracts_count DESC
            LIMIT ?
        """, (limit,))).fetchall()


async def get_top_weekly(limit: int = 10):
    """Топ гравців за поточний тиждень — за кількістю контрактів."""
    start, end = get_week_bounds()
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        return await (await cx.execute("""
            SELECT u.game_name, u.static_id,
                   COUNT(ccp.id) as week_contracts
            FROM users u
            LEFT JOIN completed_contract_participants ccp ON ccp.discord_id = u.discord_id
            LEFT JOIN completed_contracts cc ON cc.id = ccp.completed_contract_id
                AND cc.created_at >= ? AND cc.created_at < ?
            WHERE u.is_active = 1
            GROUP BY u.discord_id
            ORDER BY week_contracts DESC
            LIMIT ?
        """, (start, end, limit))).fetchall()


async def get_weekly_family_fund() -> int:
    """Сума що надійшла до сімейного банку за поточний тиждень."""
    start, end = get_week_bounds()
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        row = await (await cx.execute("""
            SELECT COALESCE(SUM(family_amount), 0) as total
            FROM completed_contracts
            WHERE created_at >= ? AND created_at < ?
        """, (start, end))).fetchone()
        return row["total"] if row else 0


async def get_all_active_users_with_weekly(start: str, end: str):
    """Всі активні гравці з їх тижневою кількістю контрактів."""
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        return await (await cx.execute("""
            SELECT u.discord_id, u.game_name, u.static_id,
                   COUNT(ccp.id) as week_contracts
            FROM users u
            LEFT JOIN completed_contract_participants ccp ON ccp.discord_id = u.discord_id
            LEFT JOIN completed_contracts cc ON cc.id = ccp.completed_contract_id
                AND cc.created_at >= ? AND cc.created_at < ?
            WHERE u.is_active = 1
            GROUP BY u.discord_id
            ORDER BY week_contracts DESC
        """, (start, end))).fetchall()


async def add_to_balance(discord_id: int, amount: int):
    async with db() as cx:
        await cx.execute(
            "UPDATE users SET balance=balance+? WHERE discord_id=? AND is_active=1",
            (amount, discord_id),
        )
        await cx.commit()


async def set_family_balance(amount: int):
    async with db() as cx:
        await cx.execute("UPDATE family_bank SET balance=? WHERE id=1", (amount,))
        await cx.commit()


async def add_to_family_balance(amount: int):
    async with db() as cx:
        await cx.execute("UPDATE family_bank SET balance=balance+? WHERE id=1", (amount,))
        await cx.commit()


# ---------- completed contracts ----------
async def create_completed_contract(ct_id: int, creator_did: int, sids: list[str]):
    ct = await contract_type_by_id(ct_id)
    if not ct:
        raise ValueError("Тип контракту не знайдено")

    parts = []
    for sid in sids:
        u = await user_by_sid(sid)
        if not u:
            raise ValueError(f"Гравця {sid} не знайдено")
        parts.append(u)

    total = ct["price"]
    fam = int(total * 0.40)
    per = (total - fam) // len(parts)
    ptxt = ", ".join(f'{u["game_name"]} ({u["static_id"]})' for u in parts)

    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        for u in parts:
            await cx.execute(
                "UPDATE users SET balance=balance+?,contracts_count=contracts_count+1 WHERE static_id=? AND is_active=1",
                (per, u["static_id"]),
            )
        await cx.execute("UPDATE family_bank SET balance=balance+? WHERE id=1", (fam,))
        cur = await cx.execute(
            "INSERT INTO completed_contracts(contract_type_id,contract_title,total_amount,family_amount,per_user_amount,created_by_discord_id,participants_count,participants_text) VALUES(?,?,?,?,?,?,?,?)",
            (ct["id"], ct["title"], total, fam, per, creator_did, len(parts), ptxt),
        )
        cc_id = cur.lastrowid
        for u in parts:
            await cx.execute(
                "INSERT INTO completed_contract_participants(completed_contract_id,discord_id,game_name,static_id,payout_amount) VALUES(?,?,?,?,?)",
                (cc_id, u["discord_id"], u["game_name"], u["static_id"], per),
            )
        await cx.commit()

    return {"contract_id": cc_id, "contract_title": ct["title"], "total_amount": total,
            "family_amount": fam, "per_user_amount": per, "participants": parts}


async def delete_completed_contract(cid: int):
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        cc = await (await cx.execute("SELECT * FROM completed_contracts WHERE id=?", (cid,))).fetchone()
        if not cc:
            raise ValueError("Контракт не знайдено")
        ps = await (await cx.execute(
            "SELECT * FROM completed_contract_participants WHERE completed_contract_id=?", (cid,)
        )).fetchall()
        for p in ps:
            await cx.execute(
                "UPDATE users SET balance=balance-?,contracts_count=MAX(0,contracts_count-1) WHERE static_id=?",
                (p["payout_amount"], p["static_id"]),
            )
        await cx.execute("UPDATE family_bank SET balance=balance-? WHERE id=1", (cc["family_amount"],))
        await cx.execute("DELETE FROM completed_contract_participants WHERE completed_contract_id=?", (cid,))
        await cx.execute("DELETE FROM completed_contracts WHERE id=?", (cid,))
        await cx.commit()
    return cc, ps


async def replace_contract_participants(cid: int, new_sids: list[str]):
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        cc = await (await cx.execute("SELECT * FROM completed_contracts WHERE id=?", (cid,))).fetchone()
        if not cc:
            raise ValueError("Контракт не знайдено")
        old = await (await cx.execute(
            "SELECT * FROM completed_contract_participants WHERE completed_contract_id=?", (cid,)
        )).fetchall()
        for p in old:
            await cx.execute(
                "UPDATE users SET balance=balance-?,contracts_count=MAX(0,contracts_count-1) WHERE static_id=?",
                (p["payout_amount"], p["static_id"]),
            )
        await cx.execute("UPDATE family_bank SET balance=balance-? WHERE id=1", (cc["family_amount"],))
        await cx.execute("DELETE FROM completed_contract_participants WHERE completed_contract_id=?", (cid,))

        parts = []
        for sid in new_sids:
            u = await (await cx.execute(
                "SELECT * FROM users WHERE static_id=? AND is_active=1", (sid,)
            )).fetchone()
            if not u:
                raise ValueError(f"Гравця {sid} не знайдено")
            parts.append(u)

        total = cc["total_amount"]
        fam = int(total * 0.40)
        per = (total - fam) // len(parts)

        for u in parts:
            await cx.execute(
                "UPDATE users SET balance=balance+?,contracts_count=contracts_count+1 WHERE static_id=? AND is_active=1",
                (per, u["static_id"]),
            )
        await cx.execute("UPDATE family_bank SET balance=balance+? WHERE id=1", (fam,))
        for u in parts:
            await cx.execute(
                "INSERT INTO completed_contract_participants(completed_contract_id,discord_id,game_name,static_id,payout_amount) VALUES(?,?,?,?,?)",
                (cid, u["discord_id"], u["game_name"], u["static_id"], per),
            )
        ptxt = ", ".join(f'{u["game_name"]} ({u["static_id"]})' for u in parts)
        await cx.execute(
            "UPDATE completed_contracts SET family_amount=?,per_user_amount=?,participants_count=?,participants_text=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (fam, per, len(parts), ptxt, cid),
        )
        await cx.commit()

    return {"contract_id": cid, "contract_title": cc["contract_title"], "total_amount": total,
            "family_amount": fam, "per_user_amount": per, "participants": parts}


# =========================================================
# BOT
# =========================================================
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree


async def notify(text: str, view: discord.ui.View = None):
    if not NOTIFY_CHANNEL_ID:
        return
    ch = bot.get_channel(NOTIFY_CHANNEL_ID)
    if ch:
        try:
            await ch.send(text, view=view)
        except Exception as e:
            print(f"notify error: {e}")


# =========================================================
# GUARD: усі View перевіряють власника
# =========================================================
class OwnedView(discord.ui.View):
    """Базовий View — тільки власник може натискати кнопки."""
    def __init__(self, owner_id: int, timeout: float = 300):
        super().__init__(timeout=timeout)
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Це не твоє меню. Напиши `/fly` щоб відкрити своє.", ephemeral=True
            )
            return False
        return True

    async def on_timeout(self):
        pass


# =========================================================
# ГОЛОВНЕ МЕНЮ
# =========================================================
async def send_main_menu(interaction: discord.Interaction, edit: bool = False):
    u = await user_by_did(interaction.user.id)
    if not u:
        txt = "Ти не зареєстрований. Напиши `/fly`"
        if edit:
            await interaction.response.edit_message(content=txt, view=None)
        else:
            await interaction.response.send_message(txt, ephemeral=True)
        return

    view = MainMenuView(interaction.user.id, u["rank"])
    txt = (
        f"╔══════════════════════╗\n"
        f"      🏠 **ГОЛОВНЕ МЕНЮ**\n"
        f"╚══════════════════════╝\n\n"
        f"👤 **{u['game_name']}**\n"
        f"🎖️ {rank_label(u['rank'])}\n"
        f"💵 Баланс: **${u['balance']:,}**"
    )
    if edit:
        await interaction.response.edit_message(content=txt, view=view)
    else:
        await interaction.response.send_message(txt, view=view, ephemeral=True)


class MainMenuView(OwnedView):
    def __init__(self, owner_id: int, rank: int):
        super().__init__(owner_id)
        self.rank = rank

        # Видаляємо кнопку адмін-панелі якщо ранг < 7
        if rank < 7:
            for item in self.children.copy():
                if hasattr(item, 'label') and item.label == "🛠 Адмін-панель":
                    self.remove_item(item)

    @discord.ui.button(label="👤 Профіль", style=discord.ButtonStyle.blurple, row=0)
    async def profile(self, interaction: discord.Interaction, _):
        u = await user_by_did(interaction.user.id)
        txt = (
            "╔══════════════════════╗\n"
            "         👤 **ПРОФІЛЬ**\n"
            "╚══════════════════════╝\n\n"
            f"🎮 **Ігрове ім'я:** {u['game_name']}\n"
            f"🆔 **Static ID:** `{u['static_id']}`\n"
            f"🙍 **Реальне ім'я:** {u['real_name']}\n\n"
            f"🎖️ **Ранг:** {rank_label(u['rank'])}\n"
            f"💵 **Баланс:** ${u['balance']:,}\n"
            f"📋 **Контракти:** {u['contracts_count']}"
        )
        await interaction.response.edit_message(content=txt, view=BackView(interaction.user.id))

    @discord.ui.button(label="💰 Баланс", style=discord.ButtonStyle.blurple, row=0)
    async def balance(self, interaction: discord.Interaction, _):
        u = await user_by_did(interaction.user.id)
        await interaction.response.edit_message(
            content=(
                "╔══════════════════════╗\n"
                "         💰 **БАЛАНС**\n"
                "╚══════════════════════╝\n\n"
                f"💵 Твій поточний баланс:\n"
                f"# ${u['balance']:,}"
            ),
            view=BackView(interaction.user.id),
        )

    @discord.ui.button(label="📄 Мої контракти", style=discord.ButtonStyle.secondary, row=1)
    async def my_contracts(self, interaction: discord.Interaction, _):
        u = await user_by_did(interaction.user.id)
        async with db() as cx:
            cx.row_factory = aiosqlite.Row
            rows = await (await cx.execute(
                """SELECT cc.id, cc.contract_title, ccp.payout_amount, cc.created_at
                   FROM completed_contract_participants ccp
                   JOIN completed_contracts cc ON ccp.completed_contract_id=cc.id
                   WHERE ccp.discord_id=? ORDER BY cc.id DESC LIMIT 20""",
                (u["discord_id"],),
            )).fetchall()

        if not rows:
            txt = (
                "╔══════════════════════╗\n"
                "     📄 **МОЇ КОНТРАКТИ**\n"
                "╚══════════════════════╝\n\n"
                "😔 Ти ще не брав участі в жодному контракті."
            )
        else:
            lines = "\n".join(
                f"▸ #{r['id']} | {r['contract_title']} | **+${r['payout_amount']:,}** | {r['created_at'][:10]}"
                for r in rows
            )
            txt = (
                "╔══════════════════════╗\n"
                "     📄 **МОЇ КОНТРАКТИ**\n"
                "╚══════════════════════╝\n\n"
                f"{lines}"
            )
        await interaction.response.edit_message(content=txt, view=BackView(interaction.user.id))

    @discord.ui.button(label="💸 Запросити вивід", style=discord.ButtonStyle.secondary, row=1)
    async def withdraw(self, interaction: discord.Interaction, _):
        u = await user_by_did(interaction.user.id)
        if u["balance"] <= 0:
            await interaction.response.send_message(
                f"❌ Твій баланс: {u['balance']}. Нічого виводити.", ephemeral=True
            )
            return
        await interaction.response.send_modal(WithdrawModal())

    @discord.ui.button(label="📝 Заповнити контракт", style=discord.ButtonStyle.green, row=2)
    async def fill_contract(self, interaction: discord.Interaction, _):
        u = await user_by_did(interaction.user.id)
        if u["rank"] < 5:
            await interaction.response.send_message("Функція доступна з рангу 5+", ephemeral=True)
            return
        view = LevelSelectView(interaction.user.id, flow="fill")
        await interaction.response.edit_message(content="📝 Вибери рівень контракту:", view=view)

    @discord.ui.button(label="📢 Сповістити гравців", style=discord.ButtonStyle.green, row=2)
    async def notify_players(self, interaction: discord.Interaction, _):
        u = await user_by_did(interaction.user.id)
        if u["rank"] < 5:
            await interaction.response.send_message("Функція доступна з рангу 5+", ephemeral=True)
            return
        view = NotifyLevelSelectView(interaction.user.id)
        await interaction.response.edit_message(
            content="📢 **Сповістити гравців**\n\nВибери рівень контракту:", view=view
        )

    @discord.ui.button(label="🛠 Адмін-панель", style=discord.ButtonStyle.blurple, row=3)
    async def admin_panel(self, interaction: discord.Interaction, _):
        u = await user_by_did(interaction.user.id)
        if u["rank"] < 7:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return
        view = AdminMenuView(interaction.user.id, u["rank"])
        await interaction.response.edit_message(content="🛠 **Адмін-панель**:", view=view)

    @discord.ui.button(label="🏆 Топ гравців", style=discord.ButtonStyle.secondary, row=3)
    async def top_players(self, interaction: discord.Interaction, _):
        view = TopSelectView(interaction.user.id)
        await interaction.response.edit_message(
            content="🏆 **Топ гравців** — вибери період:", view=view
        )

    @discord.ui.button(label="💎 Донат сім'ї", style=discord.ButtonStyle.secondary, row=4)
    async def donate(self, interaction: discord.Interaction, _):
        u = await user_by_did(interaction.user.id)
        if u["balance"] <= 0:
            await interaction.response.send_message(
                "❌ Твій баланс порожній.", ephemeral=True
            )
            return
        await interaction.response.send_modal(DonateModal())


# =========================================================
# BACK BUTTON
# =========================================================
class BackView(OwnedView):
    @discord.ui.button(label="⬅️ Назад", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _):
        await send_main_menu(interaction, edit=True)


# =========================================================
# REGISTRATION MODAL
# =========================================================
class RegisterModal(discord.ui.Modal, title="Реєстрація"):
    game_name = discord.ui.TextInput(
        label="Ігрове ім'я (Ім'я Прізвище)", placeholder="Sam Fly", min_length=3, max_length=50
    )
    static_id = discord.ui.TextInput(
        label="Static ID (1-5 символів)", placeholder="12345", min_length=1, max_length=5
    )
    real_name = discord.ui.TextInput(
        label="Реальне ім'я", placeholder="Іван", min_length=1, max_length=50
    )

    async def on_submit(self, interaction: discord.Interaction):
        gn = self.game_name.value.strip()
        sid = self.static_id.value.strip()
        rn = self.real_name.value.strip()

        if len(gn.split()) != 2:
            await interaction.response.send_message(
                "❌ Ігрове ім'я: «Ім'я Прізвище» (два слова). Натисни `/fly` знову.", ephemeral=True
            )
            return

        async with db() as cx:
            cx.row_factory = aiosqlite.Row
            if await (await cx.execute("SELECT id FROM users WHERE game_name=?", (gn,))).fetchone():
                await interaction.response.send_message(
                    f"❌ Ігрове ім'я «{gn}» вже зайнято. Натисни `/fly` знову.", ephemeral=True
                )
                return
            if await (await cx.execute("SELECT id FROM users WHERE static_id=?", (sid,))).fetchone():
                await interaction.response.send_message(
                    f"❌ Static ID «{sid}» вже зайнято. Натисни `/fly` знову.", ephemeral=True
                )
                return

        try:
            await add_user({
                "discord_id": interaction.user.id,
                "username": str(interaction.user),
                "game_name": gn, "static_id": sid, "real_name": rn,
            })
        except Exception:
            await interaction.response.send_message("Помилка реєстрації. Спробуй знову.", ephemeral=True)
            return

        # Змінити нікнейм на сервері: "Sam Fly | Іван"
        new_nick = f"{gn} | {rn}"
        try:
            await interaction.user.edit(nick=new_nick)
        except discord.Forbidden:
            pass  # Немає прав — не критично

        # Видати роль "1 | Кандидат"
        try:
            guild = interaction.guild
            if guild:
                role = discord.utils.get(guild.roles, name=PASSENGER_ROLE_NAME)
                if role:
                    await interaction.user.add_roles(role)
        except discord.Forbidden:
            pass  # Немає прав — не критично

        view = MainMenuView(interaction.user.id, 1)
        welcome = (
            f"✅ Ласкаво просимо, **{gn}**!\n\n"
            "Ти успішно зареєстрований у системі сім'ї.\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "📖 **Короткий гайд**\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "👤 **Профіль** — ігрове ім'я, Static ID, ранг, баланс\n"
            "💰 **Баланс** — нараховується після контрактів\n"
            "📄 **Мої контракти** — історія участі\n"
            "💸 **Вивід** — запит адміністратору\n"
            "📝 **Заповнити контракт** — ранг 5+\n"
            "📢 **Сповістити** — ранг 5+\n"
            "🛠 **Адмін-панель** — ранг 5+\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"Твій ранг: **{rank_label(1)}**\n"
            f"Твій нік на сервері: **{new_nick}**\n"
            "Удачі! 🚀\n\n"
            "👇 **Головне меню:**"
        )
        await interaction.response.send_message(welcome, view=view, ephemeral=True)
        await notify(
            f"👤 Новий користувач: **{gn}**\nStatic ID: {sid}\nРеальне ім'я: {rn}\nDiscord: {interaction.user}\nНік: {new_nick}"
        )


# =========================================================
# /start
# =========================================================
@tree.command(name="fly", description="Відкрити меню")
async def cmd_start(interaction: discord.Interaction):
    u_any = await user_by_did(interaction.user.id, only_active=False)

    if u_any and u_any["is_active"] == 0:
        await interaction.response.send_message(
            "❌ Твій доступ відключений адміністратором.", ephemeral=True
        )
        return

    if u_any:
        await send_main_menu(interaction)
        return

    await interaction.response.send_modal(RegisterModal())


# =========================================================
# WITHDRAW MODAL
# =========================================================
class WithdrawModal(discord.ui.Modal, title="Запит на вивід"):
    amount_field = discord.ui.TextInput(label="Сума", placeholder="1000", min_length=1, max_length=10)

    async def on_submit(self, interaction: discord.Interaction):
        u = await user_by_did(interaction.user.id)
        try:
            amount = int(self.amount_field.value.strip())
        except ValueError:
            await interaction.response.send_message("Введи суму числом", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Сума > 0", ephemeral=True)
            return
        if amount > u["balance"]:
            await interaction.response.send_message(
                f"❌ Баланс: ${u['balance']:,}. Недостатньо.", ephemeral=True
            )
            return

        # Одразу списуємо з балансу
        await deduct_balance(u["discord_id"], amount)

        wid = await create_wd(u["discord_id"], u["game_name"], amount)

        view = MainMenuView(interaction.user.id, u["rank"])
        await interaction.response.send_message(
            f"✅ Заявка **#{wid}** на вивід **${amount:,}** відправлена адміністратору.\n"
            f"💵 Сума вже списана з балансу.\n\n👇 Головне меню:",
            view=view, ephemeral=True,
        )

        # Відправити запит особисто кожному адміну в ЛС
        wd_view = WithdrawalAdminView(wid)
        msg_text = (
            f"💸 **Новий запит на вивід #{wid}**\n\n"
            f"👤 Гравець: **{u['game_name']}**\n"
            f"💵 Сума: **${amount:,}**\n\n"
            f"Схвали або відхили заявку:"
        )
        for admin_id in ADMIN_IDS:
            admin_user = bot.get_user(admin_id)
            if admin_user:
                try:
                    await admin_user.send(msg_text, view=wd_view)
                except Exception:
                    pass


# =========================================================
# WITHDRAWAL ADMIN VIEW (в ЛС адміну)
# =========================================================
class WithdrawalAdminView(discord.ui.View):
    """Кнопки в ЛС адміна — без обмеження owner."""
    def __init__(self, wid: int):
        super().__init__(timeout=None)
        self.wid = wid

    @discord.ui.button(label="✅ Підтвердити вивід", style=discord.ButtonStyle.green)
    async def approve(self, interaction: discord.Interaction, _):
        rev = await user_by_did(interaction.user.id)
        if not rev or rev["rank"] < 8:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return
        req = await wd_by_id(self.wid)
        if not req or req["status"] != "new":
            await interaction.response.send_message(
                f"Вже оброблено: {req['status'] if req else '?'}", ephemeral=True
            )
            return
        await wd_set_status(self.wid, "approved", rev["discord_id"])
        await interaction.response.edit_message(
            content=(
                f"✅ **Заявка #{self.wid} підтверджена**\n\n"
                f"👤 {req['game_name']} | 💵 ${req['amount']:,}\n"
                f"Підтвердив: {rev['game_name']}"
            ),
            view=None,
        )
        # Сповістити гравця в ЛС
        target = bot.get_user(req["discord_id"])
        if target:
            try:
                await target.send(
                    f"✅ **Твій запит на вивід підтверджено!**\n\n"
                    f"💵 Сума: **${req['amount']:,}**\n"
                    f"Адміністратор підтвердив виплату."
                )
            except Exception:
                pass

    @discord.ui.button(label="❌ Відхилити", style=discord.ButtonStyle.red)
    async def reject(self, interaction: discord.Interaction, _):
        rev = await user_by_did(interaction.user.id)
        if not rev or rev["rank"] < 8:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return
        req = await wd_by_id(self.wid)
        if not req or req["status"] != "new":
            await interaction.response.send_message("Вже оброблено", ephemeral=True)
            return
        await wd_set_status(self.wid, "rejected", rev["discord_id"])

        # Повернути гроші гравцю
        async with db() as cx:
            await cx.execute(
                "UPDATE users SET balance=balance+? WHERE discord_id=? AND is_active=1",
                (req["amount"], req["discord_id"]),
            )
            await cx.commit()

        await interaction.response.edit_message(
            content=(
                f"❌ **Заявка #{self.wid} відхилена**\n\n"
                f"👤 {req['game_name']} | 💵 ${req['amount']:,}\n"
                f"Відхилив: {rev['game_name']}\n"
                f"💰 Кошти повернуто гравцю."
            ),
            view=None,
        )
        # Сповістити гравця в ЛС
        target = bot.get_user(req["discord_id"])
        if target:
            try:
                await target.send(
                    f"❌ **Твій запит на вивід відхилено.**\n\n"
                    f"💵 Сума: **${req['amount']:,}**\n"
                    f"💰 Кошти повернуто на твій баланс."
                )
            except Exception:
                pass


# =========================================================
# FILL CONTRACT FLOW
# =========================================================
class LevelSelectView(OwnedView):
    def __init__(self, owner_id: int, flow: str):
        super().__init__(owner_id)
        self.flow = flow

    @discord.ui.button(label="1️⃣ Рівень 1", style=discord.ButtonStyle.blurple)
    async def lvl1(self, interaction: discord.Interaction, _):
        await self._pick(interaction, 1)

    @discord.ui.button(label="2️⃣ Рівень 2", style=discord.ButtonStyle.blurple)
    async def lvl2(self, interaction: discord.Interaction, _):
        await self._pick(interaction, 2)

    @discord.ui.button(label="❌ Скасувати", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _):
        sess_clear(interaction.user.id)
        await send_main_menu(interaction, edit=True)

    async def _pick(self, interaction: discord.Interaction, level: int):
        contracts = await all_contract_types(level)
        if not contracts:
            await interaction.response.send_message(f"Контрактів {level} рівня поки немає.", ephemeral=True)
            return
        view = ContractSelectView(interaction.user.id, contracts, self.flow)
        lines = "\n".join(f"{c['id']}. **{c['title']}** — {c['price']}" for c in contracts)
        await interaction.response.edit_message(
            content=f"Контракти {level} рівня:\n\n{lines}", view=view
        )


class ContractSelectView(OwnedView):
    def __init__(self, owner_id: int, contracts, flow: str):
        super().__init__(owner_id)
        self.flow = flow
        self.contracts = {str(c["id"]): dict(c) for c in contracts}

        opts = [
            discord.SelectOption(label=f"{c['title']} — {c['price']}", value=str(c["id"]))
            for c in contracts
        ]
        sel = discord.ui.Select(placeholder="Вибери контракт...", options=opts)
        sel.callback = self._on_select
        self.add_item(sel)

        back = discord.ui.Button(label="⬅️ Назад", style=discord.ButtonStyle.secondary)
        back.callback = self._on_back
        self.add_item(back)

    async def _on_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        view = LevelSelectView(interaction.user.id, self.flow)
        await interaction.response.edit_message(content="Вибери рівень контракту:", view=view)

    async def _on_select(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        chosen = self.contracts[interaction.data["values"][0]]
        s = sess(interaction.user.id)
        s["ct_id"] = chosen["id"]
        s["ct_title"] = chosen["title"]
        s["ct_price"] = chosen["price"]
        s["p_ids"] = []
        s["p_names"] = []

        if self.flow == "fill":
            view = ParticipantsView(interaction.user.id)
            await interaction.response.edit_message(
                content=(
                    f"✅ Контракт: **{chosen['title']}**\n\n"
                    "👥 Вибери учасників через **@** (до 10 осіб)\n"
                    "Можна вибирати по одному — список накопичується"
                ),
                view=view,
            )
        elif self.flow == "notify":
            await interaction.response.send_modal(NotifyLocationModal(interaction.user.id, chosen["title"]))


class ParticipantsView(OwnedView):
    """Вибір учасників контракту через Discord UserSelect (@)."""
    def __init__(self, owner_id: int):
        super().__init__(owner_id)
        self._build()

    def _build(self):
        self.clear_items()
        s = sess(self.owner_id)
        selected = s.get("p_discord_ids", [])

        sel = discord.ui.UserSelect(
            placeholder=f"Вибери учасників ({len(selected)}/10)...",
            min_values=1,
            max_values=min(10, 25),
        )
        sel.callback = self._on_select
        self.add_item(sel)

        if selected:
            done_btn = discord.ui.Button(label=f"✔️ Готово ({len(selected)} обрано)", style=discord.ButtonStyle.green, row=1)
            done_btn.callback = self._on_done
            self.add_item(done_btn)

            clear_btn = discord.ui.Button(label="🗑 Очистити список", style=discord.ButtonStyle.secondary, row=1)
            clear_btn.callback = self._on_clear
            self.add_item(clear_btn)

        cancel_btn = discord.ui.Button(label="❌ Скасувати", style=discord.ButtonStyle.secondary, row=1)
        cancel_btn.callback = self._on_cancel
        self.add_item(cancel_btn)

    async def _on_select(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return

        s = sess(interaction.user.id)
        selected_discord_ids = s.get("p_discord_ids", [])
        selected_names = s.get("p_names", [])
        selected_sids = s.get("p_ids", [])

        # Отримуємо вибраних юзерів
        resolved = interaction.data.get("resolved", {}).get("users", {})
        new_member_ids = [int(uid) for uid in resolved.keys()]

        errors = []
        for did in new_member_ids:
            if did in selected_discord_ids:
                continue  # вже є
            if len(selected_discord_ids) >= 10:
                errors.append("Максимум 10 учасників")
                break
            # Перевіряємо чи є в БД
            db_user = await user_by_did(did)
            if not db_user:
                member = interaction.guild.get_member(did)
                name = member.display_name if member else str(did)
                errors.append(f"**{name}** не зареєстрований у боті")
                continue
            selected_discord_ids.append(did)
            selected_names.append(db_user["game_name"])
            selected_sids.append(db_user["static_id"])

        s["p_discord_ids"] = selected_discord_ids
        s["p_names"] = selected_names
        s["p_ids"] = selected_sids

        names_txt = ", ".join(selected_names) if selected_names else "**нікого**"
        err_txt = ("\n\n⚠️ " + "\n".join(errors)) if errors else ""

        self._build()
        await interaction.response.edit_message(
            content=(
                f"✅ Контракт: **{s.get('ct_title', '')}**\n\n"
                f"Вибрано ({len(selected_discord_ids)}/10): {names_txt}"
                f"{err_txt}\n\n"
                "_Можна вибирати ще — UserSelect додає до списку_"
            ),
            view=self,
        )

    async def _on_clear(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        s = sess(interaction.user.id)
        s["p_discord_ids"] = []
        s["p_names"] = []
        s["p_ids"] = []
        self._build()
        await interaction.response.edit_message(
            content=f"✅ Контракт: **{s.get('ct_title', '')}**\n\nСписок очищено. Вибери учасників знову:",
            view=self,
        )

    async def _on_done(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        s = sess(interaction.user.id)
        ids = s.get("p_ids", [])
        if not ids:
            await interaction.response.send_message("Вибери хоча б одного учасника", ephemeral=True)
            return

        total = s["ct_price"]
        fam = int(total * 0.40)
        per = (total - fam) // len(ids)
        names_txt = "\n".join(f"• {n}" for n in s["p_names"])

        await interaction.response.edit_message(
            content=(
                "📋 **Перевір контракт:**\n\n"
                f"Контракт: {s['ct_title']}\n"
                f"Сума: {total}\n"
                f"Сім'ї (40%): {fam}\n"
                f"Кожному: {per}\n\n"
                f"Учасники ({len(ids)}):\n{names_txt}"
            ),
            view=ContractConfirmView(interaction.user.id),
        )

    async def _on_cancel(self, interaction: discord.Interaction):
        sess_clear(interaction.user.id)
        await send_main_menu(interaction, edit=True)


class PlayerLookupView(OwnedView):
    """Пошук гравця через @ для перегляду даних в адмін-панелі."""
    def __init__(self, owner_id: int, caller_rank: int):
        super().__init__(owner_id)
        self.caller_rank = caller_rank

        sel = discord.ui.UserSelect(placeholder="Вибери учасника через @...")
        sel.callback = self._on_pick
        self.add_item(sel)

        back = discord.ui.Button(label="⬅️ Назад", style=discord.ButtonStyle.secondary, row=1)
        back.callback = self._on_back
        self.add_item(back)

    async def _on_pick(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return

        resolved = interaction.data.get("resolved", {}).get("users", {})
        if not resolved:
            await interaction.response.send_message("Учасника не знайдено", ephemeral=True)
            return

        did = int(list(resolved.keys())[0])
        member = interaction.guild.get_member(did)
        db_user = await user_by_did(did)

        if not db_user:
            name = member.display_name if member else str(did)
            await interaction.response.edit_message(
                content=(
                    f"👤 **{name}** (@{member.name if member else did})\n\n"
                    "❌ Цей учасник **не зареєстрований** у боті."
                ),
                view=PlayerLookupView(interaction.user.id, self.caller_rank),
            )
            return

        await interaction.response.edit_message(
            content=(
                "╔══════════════════════╗\n"
                "      👤 **ПРОФІЛЬ ГРАВЦЯ**\n"
                "╚══════════════════════╝\n\n"
                f"🎮 **Ігрове ім'я:** {db_user['game_name']}\n"
                f"🆔 **Static ID:** `{db_user['static_id']}`\n"
                f"🙍 **Реальне ім'я:** {db_user['real_name']}\n"
                f"💬 **Discord:** {member.mention if member else did}\n\n"
                f"🎖️ **Ранг:** {rank_label(db_user['rank'])}\n"
                f"💵 **Баланс:** ${db_user['balance']:,}\n"
                f"📋 **Контракти:** {db_user['contracts_count']}\n"
                f"{'✅ Активний' if db_user['is_active'] else '❌ Деактивований'}"
            ),
            view=PlayerLookupView(interaction.user.id, self.caller_rank),
        )

    async def _on_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        u = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, u["rank"] if u else self.caller_rank)
        await interaction.response.edit_message(content="🛠 **Адмін-панель:**", view=view)


# =========================================================
# EDIT PLAYER
# =========================================================
class EditPlayerPickView(OwnedView):
    """Вибір гравця через @ для редагування."""
    def __init__(self, owner_id: int, caller_rank: int):
        super().__init__(owner_id)
        self.caller_rank = caller_rank

        sel = discord.ui.UserSelect(placeholder="Вибери гравця для редагування...")
        sel.callback = self._on_pick
        self.add_item(sel)

        back = discord.ui.Button(label="⬅️ Назад", style=discord.ButtonStyle.secondary, row=1)
        back.callback = self._on_back
        self.add_item(back)

    async def _on_pick(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return

        resolved = interaction.data.get("resolved", {}).get("users", {})
        did = int(list(resolved.keys())[0])
        member = interaction.guild.get_member(did)
        db_user = await user_by_did(did)

        if not db_user:
            name = member.display_name if member else str(did)
            await interaction.response.edit_message(
                content=f"❌ **{name}** не зареєстрований у боті.",
                view=EditPlayerPickView(interaction.user.id, self.caller_rank),
            )
            return

        await interaction.response.send_modal(
            EditPlayerModal(
                owner_id=interaction.user.id,
                caller_rank=self.caller_rank,
                db_user=dict(db_user),
                member=member,
            )
        )

    async def _on_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        u = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, u["rank"] if u else self.caller_rank)
        await interaction.response.edit_message(content="🛠 **Адмін-панель:**", view=view)


class EditPlayerModal(discord.ui.Modal, title="Редагувати гравця"):
    game_name_f = discord.ui.TextInput(
        label="Ігрове ім'я (Ім'я Прізвище)", min_length=3, max_length=50
    )
    real_name_f = discord.ui.TextInput(
        label="Реальне ім'я", min_length=1, max_length=50
    )
    rank_f = discord.ui.TextInput(
        label="Ранг (1-10)", min_length=1, max_length=2
    )
    balance_f = discord.ui.TextInput(
        label="Баланс гравця", min_length=1, max_length=12
    )
    contracts_f = discord.ui.TextInput(
        label="Кількість контрактів", min_length=1, max_length=6
    )

    def __init__(self, owner_id: int, caller_rank: int, db_user: dict, member):
        super().__init__()
        self.owner_id = owner_id
        self.caller_rank = caller_rank
        self.db_user = db_user
        self.member = member

        self.game_name_f.default = db_user["game_name"]
        self.real_name_f.default = db_user["real_name"]
        self.rank_f.default = str(db_user["rank"])
        self.balance_f.default = str(db_user["balance"])
        self.contracts_f.default = str(db_user["contracts_count"])

    async def on_submit(self, interaction: discord.Interaction):
        caller = await user_by_did(interaction.user.id)
        if not caller or caller["rank"] < 6:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return

        new_gn = self.game_name_f.value.strip()
        new_rn = self.real_name_f.value.strip()

        try:
            new_rank = int(self.rank_f.value.strip())
            new_balance = int(self.balance_f.value.strip())
            new_contracts = int(self.contracts_f.value.strip())
        except ValueError:
            await interaction.response.send_message("Ранг, баланс і контракти — числа", ephemeral=True)
            return

        if len(new_gn.split()) != 2:
            await interaction.response.send_message("Ігрове ім'я: «Ім'я Прізвище»", ephemeral=True)
            return
        if not 1 <= new_rank <= 10:
            await interaction.response.send_message("Ранг 1-10", ephemeral=True)
            return
        if new_rank >= caller["rank"] and caller["rank"] < 10:
            await interaction.response.send_message("Не можна виставити ранг ≥ своєму", ephemeral=True)
            return
        if new_balance < 0 or new_contracts < 0:
            await interaction.response.send_message("Баланс і контракти не можуть бути від'ємними", ephemeral=True)
            return

        balance_changed = new_balance != self.db_user["balance"]
        contracts_changed = new_contracts != self.db_user["contracts_count"]

        if (balance_changed or contracts_changed) and caller["rank"] < 9:
            await interaction.response.send_message(
                "❌ Змінювати баланс і контракти може тільки ранг 9+", ephemeral=True
            )
            return

        # Оновити в БД
        async with db() as cx:
            await cx.execute(
                "UPDATE users SET game_name=?, real_name=?, rank=?, balance=?, contracts_count=? WHERE discord_id=?",
                (new_gn, new_rn, new_rank, new_balance, new_contracts, self.db_user["discord_id"]),
            )
            await cx.commit()

        # Якщо баланс зменшився — різниця іде в сім'ю
        if balance_changed and new_balance < self.db_user["balance"]:
            diff = self.db_user["balance"] - new_balance
            await add_to_family_balance(diff)

        # Оновити нік на сервері
        new_nick = f"{new_gn} | {new_rn}"
        try:
            if self.member:
                await self.member.edit(nick=new_nick)
        except discord.Forbidden:
            pass

        # Оновити роль рангу на сервері
        if self.member:
            rank_role_names = list(RANK_NAMES.values())
            roles_to_remove = [r for r in self.member.roles if r.name in rank_role_names]
            new_role = discord.utils.get(interaction.guild.roles, name=RANK_NAMES[new_rank])
            try:
                if roles_to_remove:
                    await self.member.remove_roles(*roles_to_remove)
                if new_role:
                    await self.member.add_roles(new_role)
            except discord.Forbidden:
                pass

        changes = []
        if new_gn != self.db_user["game_name"]:
            changes.append(f"Ім'я: {self.db_user['game_name']} → {new_gn}")
        if new_rn != self.db_user["real_name"]:
            changes.append(f"Реальне: {self.db_user['real_name']} → {new_rn}")
        if new_rank != self.db_user["rank"]:
            changes.append(f"Ранг: {rank_label(self.db_user['rank'])} → {rank_label(new_rank)}")
        if balance_changed:
            if new_balance < self.db_user["balance"]:
                diff = self.db_user["balance"] - new_balance
                changes.append(f"Баланс: ${self.db_user['balance']:,} → ${new_balance:,} (різниця ${diff:,} → сім'я)")
            else:
                changes.append(f"Баланс: ${self.db_user['balance']:,} → ${new_balance:,}")
        if contracts_changed:
            changes.append(f"Контракти: {self.db_user['contracts_count']} → {new_contracts}")

        changes_txt = "\n".join(f"▸ {c}" for c in changes) if changes else "Змін немає"

        view = BackToAdminView(interaction.user.id, caller["rank"])
        await interaction.response.edit_message(
            content=f"✅ **Гравця оновлено: {new_gn}**\n\n{changes_txt}",
            view=view,
        )

        if changes:
            await notify(
                f"✏️ **Гравця відредаговано**\n\n"
                f"👤 {new_gn}\n"
                + "\n".join(f"▸ {c}" for c in changes) +
                f"\n\nРедагував: {caller['game_name']}"
            )

        if changes:
            await notify(
                f"✏️ **Гравця відредаговано**\n\n"
                f"👤 {new_gn}\n"
                + "\n".join(f"▸ {c}" for c in changes) +
                f"\n\nРедагував: {caller['game_name']}"
            )


class ContractConfirmView(OwnedView):
    @discord.ui.button(label="✅ Підтвердити", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, _):
        s = sess(interaction.user.id)
        try:
            result = await create_completed_contract(s["ct_id"], interaction.user.id, s["p_ids"])
        except Exception as e:
            sess_clear(interaction.user.id)
            await interaction.response.edit_message(
                content=f"❌ Помилка: {e}", view=BackView(interaction.user.id)
            )
            return

        sess_clear(interaction.user.id)
        save_last_contract(
            interaction.user.id,
            result["contract_id"],
            result["contract_title"],
            [u["static_id"] for u in result["participants"]],
        )
        utxt = "\n".join(f"- {u['game_name']} ({u['static_id']})" for u in result["participants"])
        u = await user_by_did(interaction.user.id)
        await interaction.response.edit_message(
            content=(
                "✅ **Контракт збережено**\n\n"
                f"ID: {result['contract_id']}\n"
                f"Контракт: {result['contract_title']}\n"
                f"Сума: {result['total_amount']}\n"
                f"Сім'ї: {result['family_amount']}\n"
                f"Кожному: {result['per_user_amount']}\n\n"
                f"Учасники:\n{utxt}\n\n👇 Головне меню:"
            ),
            view=MainMenuView(interaction.user.id, u["rank"] if u else 1),
        )
        creator = await user_by_did(interaction.user.id)
        await notify(
            "📝 **Новий контракт**\n\n"
            f"Контракт: {result['contract_title']}\n"
            f"Заповнив: {creator['game_name'] if creator else '?'}\n"
            f"Учасників: {len(result['participants'])}\n"
            f"Сума: {result['total_amount']} | Сім'ї: {result['family_amount']} | Кожному: {result['per_user_amount']}\n\n"
            f"Учасники:\n{utxt}"
        )

    @discord.ui.button(label="❌ Скасувати", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _):
        sess_clear(interaction.user.id)
        await send_main_menu(interaction, edit=True)


# =========================================================
# NOTIFY MODALS
# =========================================================
class NotifyLevelSelectView(OwnedView):
    @discord.ui.button(label="1️⃣ Рівень 1", style=discord.ButtonStyle.blurple)
    async def lvl1(self, interaction: discord.Interaction, _):
        await self._pick(interaction, 1)

    @discord.ui.button(label="2️⃣ Рівень 2", style=discord.ButtonStyle.blurple)
    async def lvl2(self, interaction: discord.Interaction, _):
        await self._pick(interaction, 2)

    @discord.ui.button(label="❌ Скасувати", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _):
        await send_main_menu(interaction, edit=True)

    async def _pick(self, interaction: discord.Interaction, level: int):
        contracts = await all_contract_types(level)
        if not contracts:
            await interaction.response.send_message(
                f"Контрактів {level} рівня поки немає.", ephemeral=True
            )
            return
        view = NotifyContractSelectView(interaction.user.id, contracts)
        lines = "\n".join(f"{c['id']}. **{c['title']}**" for c in contracts)
        await interaction.response.edit_message(
            content=f"📢 Контракти {level} рівня:\n\n{lines}\n\nВибери контракт:",
            view=view,
        )


class NotifyContractSelectView(OwnedView):
    def __init__(self, owner_id: int, contracts):
        super().__init__(owner_id)
        self.contracts = {str(c["id"]): dict(c) for c in contracts}

        opts = [
            discord.SelectOption(label=c["title"], value=str(c["id"]))
            for c in contracts
        ]
        sel = discord.ui.Select(placeholder="Вибери контракт...", options=opts)
        sel.callback = self._on_select
        self.add_item(sel)

        back = discord.ui.Button(label="⬅️ Назад", style=discord.ButtonStyle.secondary)
        back.callback = self._on_back
        self.add_item(back)

    async def _on_select(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        chosen = self.contracts[interaction.data["values"][0]]
        await interaction.response.send_modal(
            NotifyLocationModal(interaction.user.id, chosen["title"])
        )

    async def _on_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        view = NotifyLevelSelectView(interaction.user.id)
        await interaction.response.edit_message(
            content="📢 **Сповістити гравців**\n\nВибери рівень контракту:", view=view
        )


class NotifyLocationModal(discord.ui.Modal, title="Місце проведення"):
    location = discord.ui.TextInput(
        label="Локація", placeholder="Порт, склад №3", min_length=1, max_length=100
    )

    def __init__(self, owner_id: int, ct_title: str):
        super().__init__()
        self.owner_id = owner_id
        self.ct_title = ct_title

    async def on_submit(self, interaction: discord.Interaction):
        msg = (
            "🚨 **ЗБІР НА КОНТРАКТ** @FLY\n\n"
            f"📋 Контракт: **{self.ct_title}**\n"
            f"📍 Локація: **{self.location.value}**\n"
            "⏱ Починаємо через 5-10 хвилин\n\n"
            "Чекаємо всіх на місці!"
        )

        # Відправити в канал збору (окремий від загального каналу сповіщень)
        channel = bot.get_channel(GATHER_CHANNEL_ID) or bot.get_channel(NOTIFY_CHANNEL_ID)
        if channel:
            # Знайти роль @FLY для згадки
            guild = interaction.guild
            fly_role = discord.utils.get(guild.roles, name="FLY") if guild else None
            role_mention = fly_role.mention if fly_role else "@FLY"

            notify_msg = (
                f"🚨 **ЗБІР НА КОНТРАКТ** {role_mention}\n\n"
                f"📋 Контракт: **{self.ct_title}**\n"
                f"📍 Локація: **{self.location.value}**\n"
                "⏱ Починаємо через 5-10 хвилин\n\n"
                "Чекаємо всіх на місці!"
            )
            await channel.send(notify_msg)

        caller = await user_by_did(interaction.user.id)
        view = MainMenuView(interaction.user.id, caller["rank"] if caller else 1)
        await interaction.response.edit_message(
            content=f"✅ Сповіщення відправлено в канал нотифікацій\n\n👇 Головне меню:",
            view=view,
        )


# =========================================================
# ADMIN MENU
# =========================================================
class AdminMenuView(OwnedView):
    def __init__(self, owner_id: int, rank: int):
        super().__init__(owner_id)
        self.rank = rank

    @discord.ui.button(label="📋 Типи контрактів", style=discord.ButtonStyle.blurple, row=0)
    async def ct_list(self, interaction: discord.Interaction, _):
        rows = await all_contract_types()
        if not rows:
            await interaction.response.send_message("Типів немає", ephemeral=True)
            return
        l1 = [r for r in rows if r["level"] == 1]
        l2 = [r for r in rows if r["level"] == 2]
        txt = "📋 **Типи контрактів:**\n\n**1️⃣ Рівень 1:**\n"
        txt += "\n".join(f"  {r['id']}. {r['title']} — {r['price']}" for r in l1) or "  Порожньо"
        txt += "\n\n**2️⃣ Рівень 2:**\n"
        txt += "\n".join(f"  {r['id']}. {r['title']} — {r['price']}" for r in l2) or "  Порожньо"
        await interaction.response.edit_message(content=txt, view=BackToAdminView(interaction.user.id, self.rank))

    @discord.ui.button(label="👥 Гравці", style=discord.ButtonStyle.blurple, row=0)
    async def players_list(self, interaction: discord.Interaction, _):
        view = PlayerLookupView(interaction.user.id, self.rank)
        await interaction.response.edit_message(
            content="👥 **Гравці**\n\nВибери учасника через @ щоб переглянути його дані:",
            view=view,
        )

    @discord.ui.button(label="✏️ Редагувати гравця", style=discord.ButtonStyle.secondary, row=0)
    async def edit_player_btn(self, interaction: discord.Interaction, _):
        if self.rank < 9:
            await interaction.response.send_message("Нема доступу (ранг 9+)", ephemeral=True)
            return
        view = EditPlayerPickView(interaction.user.id, self.rank)
        await interaction.response.edit_message(
            content="✏️ **Редагувати гравця**\n\nВибери учасника через @:",
            view=view,
        )

    @discord.ui.button(label="💳 Заявки на вивід", style=discord.ButtonStyle.blurple, row=0)
    async def wd_list(self, interaction: discord.Interaction, _):
        if self.rank < 9:
            await interaction.response.send_message("Нема доступу (ранг 9+)", ephemeral=True)
            return
        rows = await all_wd(20)
        if not rows:
            await interaction.response.edit_message(
                content="💳 Заявок на вивід немає",
                view=BackToAdminView(interaction.user.id, self.rank),
            )
            return
        view = WithdrawalListView(interaction.user.id, self.rank, [dict(r) for r in rows])
        await interaction.response.edit_message(
            content=view._current_content(),
            view=view,
        )

    @discord.ui.button(label="🪖 Встановити ранг", style=discord.ButtonStyle.secondary, row=1)
    async def set_rank_btn(self, interaction: discord.Interaction, _):
        if self.rank < 6:
            await interaction.response.send_message("Нема доступу (ранг 6+)", ephemeral=True)
            return
        view = UserPickView(interaction.user.id, action="set_rank", caller_rank=self.rank)
        await interaction.response.edit_message(
            content="🪖 **Встановити ранг**\n\nВибери учасника зі списку:", view=view
        )

    @discord.ui.button(label="🏦 Баланс сім'ї", style=discord.ButtonStyle.secondary, row=1)
    async def fam_bal(self, interaction: discord.Interaction, _):
        if self.rank < 9:
            await interaction.response.send_message("Нема доступу (ранг 9+)", ephemeral=True)
            return
        bal = await family_balance()
        weekly_fund = await get_weekly_family_fund()
        distribute = weekly_fund // 2
        family_keeps = weekly_fund - distribute
        view = FamilyBalanceView(interaction.user.id, self.rank)
        await interaction.response.edit_message(
            content=(
                "╔══════════════════════╗\n"
                "      🏦 **БАЛАНС СІМ'Ї**\n"
                "╚══════════════════════╝\n\n"
                f"💵 **Актуальний баланс:** ${bal:,}\n\n"
                f"📅 **За поточний тиждень:**\n"
                f"▸ Фонд тижня: **${weekly_fund:,}**\n"
                f"▸ Піде на виплати (50%): **${distribute:,}**\n"
                f"▸ Залишається сім'ї (50%): **${family_keeps:,}**"
            ),
            view=view,
        )

    @discord.ui.button(label="🎁 Видати премію", style=discord.ButtonStyle.green, row=1)
    async def give_bonus(self, interaction: discord.Interaction, _):
        if self.rank < 9:
            await interaction.response.send_message("Нема доступу (ранг 9+)", ephemeral=True)
            return
        view = BonusUserPickView(interaction.user.id, self.rank)
        await interaction.response.edit_message(
            content="🎁 **Видати премію**\n\nВибери гравця через @:",
            view=view,
        )

    @discord.ui.button(label="➕ Додати контракт", style=discord.ButtonStyle.green, row=2)
    async def add_ct(self, interaction: discord.Interaction, _):
        if self.rank < 9:
            await interaction.response.send_message("Нема доступу (ранг 9+)", ephemeral=True)
            return
        await interaction.response.send_modal(AddContractTypeModal())

    @discord.ui.button(label="🗑 Видалити тип контракту", style=discord.ButtonStyle.red, row=2)
    async def del_ct(self, interaction: discord.Interaction, _):
        if self.rank < 9:
            await interaction.response.send_message("Нема доступу (ранг 9+)", ephemeral=True)
            return
        await interaction.response.send_modal(DeleteContractTypeModal())

    @discord.ui.button(label="✏️ Змінити контракт", style=discord.ButtonStyle.secondary, row=3)
    async def edit_cc(self, interaction: discord.Interaction, _):
        if self.rank < 9:
            await interaction.response.send_message("Нема доступу (ранг 9+)", ephemeral=True)
            return
        view = EditContractView(interaction.user.id, self.rank)
        await interaction.response.edit_message(
            content="✏️ **Змінити виконаний контракт**\n\nВибери автора контракту через @:",
            view=view,
        )

    @discord.ui.button(label="🚫 Видалити гравця", style=discord.ButtonStyle.red, row=3)
    async def del_player(self, interaction: discord.Interaction, _):
        if self.rank < 10:
            await interaction.response.send_message("Нема доступу (тільки ранг 10)", ephemeral=True)
            return
        view = UserPickView(interaction.user.id, action="del_player", caller_rank=self.rank)
        await interaction.response.edit_message(
            content="🚫 **Видалити гравця**\n\nВибери учасника зі списку:", view=view
        )

    @discord.ui.button(label="⬅️ Головне меню", style=discord.ButtonStyle.secondary, row=4)
    async def back(self, interaction: discord.Interaction, _):
        await send_main_menu(interaction, edit=True)


class BackToAdminView(OwnedView):
    def __init__(self, owner_id: int, rank: int):
        super().__init__(owner_id)
        self.rank = rank

    @discord.ui.button(label="⬅️ Назад до адмін-панелі", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _):
        u = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, u["rank"] if u else self.rank)
        await interaction.response.edit_message(content="🛠 **Адмін-панель:**", view=view)


# =========================================================
# UNIVERSAL USER PICKER (discord.ui.UserSelect)
# =========================================================
class UserPickView(OwnedView):
    """
    Єдиний View для вибору учасника через вбудований Discord UserSelect.
    action: 'set_rank' | 'del_player' | 'give_role'
    """
    def __init__(self, owner_id: int, action: str, caller_rank: int):
        super().__init__(owner_id)
        self.action = action
        self.caller_rank = caller_rank

        sel = discord.ui.UserSelect(placeholder="Почни вводити ім'я або @нікнейм...")
        sel.callback = self._on_pick
        self.add_item(sel)

        back = discord.ui.Button(label="⬅️ Назад", style=discord.ButtonStyle.secondary, row=1)
        back.callback = self._on_back
        self.add_item(back)

    async def _on_pick(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return

        member: discord.Member = interaction.data["resolved"]["members"]
        # UserSelect повертає dict — беремо першого
        member_id = int(list(interaction.data["resolved"]["members"].keys())[0])
        member = interaction.guild.get_member(member_id)

        if not member:
            await interaction.response.send_message("Учасника не знайдено на сервері", ephemeral=True)
            return

        if self.action == "set_rank":
            view = RankPickView(interaction.user.id, member, self.caller_rank)
            await interaction.response.edit_message(
                content=(
                    f"🪖 Встановити ранг для **{member.display_name}** (@{member.name})\n\n"
                    "Вибери новий ранг:"
                ),
                view=view,
            )

        elif self.action == "del_player":
            view = ConfirmDeletePlayerView(interaction.user.id, member, self.caller_rank)
            await interaction.response.edit_message(
                content=(
                    f"🗑 Видалити гравця **{member.display_name}** (@{member.name})?\n\n"
                    "⚠️ Гравець буде деактивований у базі даних."
                ),
                view=view,
            )

        elif self.action == "give_role":
            view = GiveRoleSelectView(interaction.user.id, member)
            await interaction.response.edit_message(
                content=(
                    f"🎭 Видати роль учаснику **{member.display_name}** (@{member.name})\n\n"
                    "Вибери роль:"
                ),
                view=view,
            )

    async def _on_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        u = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, u["rank"] if u else self.caller_rank)
        await interaction.response.edit_message(content="🛠 **Адмін-панель:**", view=view)


class RankPickView(OwnedView):
    """Вибір рангу після вибору учасника."""
    def __init__(self, owner_id: int, target: discord.Member, caller_rank: int):
        super().__init__(owner_id)
        self.target = target
        self.caller_rank = caller_rank

        # Генеруємо кнопки для рангів нижче caller_rank
        opts = [
            discord.SelectOption(label=rank_label(r), value=str(r))
            for r in range(1, caller_rank + 1 if caller_rank >= 10 else caller_rank)  # ранг 10 може виставляти будь-який
        ]
        if opts:
            sel = discord.ui.Select(placeholder="Вибери ранг...", options=opts)
            sel.callback = self._on_rank
            self.add_item(sel)

        back = discord.ui.Button(label="⬅️ Назад", style=discord.ButtonStyle.secondary, row=1)
        back.callback = self._on_back
        self.add_item(back)

    async def _on_rank(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        caller = await user_by_did(interaction.user.id)
        new_rank = int(interaction.data["values"][0])

        # Оновити в БД якщо гравець зареєстрований
        target_db = await user_by_did(self.target.id)
        if target_db:
            await set_rank(target_db["static_id"], new_rank)

        # Оновити роль на сервері
        rank_role_names = list(RANK_NAMES.values())
        roles_to_remove = [r for r in self.target.roles if r.name in rank_role_names]
        new_role = discord.utils.get(interaction.guild.roles, name=RANK_NAMES[new_rank])
        try:
            if roles_to_remove:
                await self.target.remove_roles(*roles_to_remove)
            if new_role:
                await self.target.add_roles(new_role)
        except discord.Forbidden:
            pass

        u = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, u["rank"] if u else self.caller_rank)
        await interaction.response.edit_message(
            content=(
                f"✅ **{self.target.display_name}** → **{rank_label(new_rank)}**\n\n"
                "🛠 **Адмін-панель:**"
            ),
            view=view,
        )
        await notify(
            f"🪖 Зміна рангу\n{self.target.display_name} → {rank_label(new_rank)}\n"
            f"Встановив: {caller['game_name'] if caller else interaction.user}"
        )

    async def _on_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        view = UserPickView(interaction.user.id, action="set_rank", caller_rank=self.caller_rank)
        await interaction.response.edit_message(
            content="🪖 **Встановити ранг**\n\nВибери учасника зі списку:", view=view
        )


class ConfirmDeletePlayerView(OwnedView):
    """Підтвердження видалення гравця."""
    def __init__(self, owner_id: int, target: discord.Member, caller_rank: int):
        super().__init__(owner_id)
        self.target = target
        self.caller_rank = caller_rank

    @discord.ui.button(label="✅ Підтвердити видалення", style=discord.ButtonStyle.red)
    async def confirm(self, interaction: discord.Interaction, _):
        caller = await user_by_did(interaction.user.id)
        if not caller or caller["rank"] < 10:
            await interaction.response.send_message("Нема доступу (тільки ранг 10)", ephemeral=True)
            return

        # Видалити з БД повністю
        async with db() as cx:
            await cx.execute(
                "DELETE FROM completed_contract_participants WHERE discord_id=?",
                (self.target.id,)
            )
            await cx.execute("DELETE FROM users WHERE discord_id=?", (self.target.id,))
            await cx.commit()

        # Кікнути з сервера
        try:
            await self.target.kick(reason=f"Видалено адміністратором {interaction.user}")
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ Не вдалося кікнути — недостатньо прав у бота", ephemeral=True
            )
            return

        u = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, u["rank"] if u else self.caller_rank)
        await interaction.response.edit_message(
            content=f"✅ Гравця **{self.target.display_name}** видалено з бази та кікнуто з сервера.\n\n🛠 **Адмін-панель:**",
            view=view,
        )
        await notify(f"🗑 Гравця **{self.target.display_name}** видалено та кікнуто.\nВидалив: {caller['game_name']}")

    @discord.ui.button(label="❌ Скасувати", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _):
        view = UserPickView(interaction.user.id, action="del_player", caller_rank=self.caller_rank)
        await interaction.response.edit_message(
            content="🗑 **Видалити гравця**\n\nВибери учасника зі списку:", view=view
        )


class AddContractTypeModal(discord.ui.Modal, title="Додати тип контракту"):
    level_f = discord.ui.TextInput(label="Рівень (1 або 2)", min_length=1, max_length=1)
    title_f = discord.ui.TextInput(label="Назва", min_length=1, max_length=100)
    price_f = discord.ui.TextInput(label="Вартість", min_length=1, max_length=10)

    async def on_submit(self, interaction: discord.Interaction):
        caller = await user_by_did(interaction.user.id)
        if not caller or caller["rank"] < 8:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return
        try:
            level = int(self.level_f.value.strip())
            price = int(self.price_f.value.strip())
        except ValueError:
            await interaction.response.send_message("Рівень і ціна — числа", ephemeral=True)
            return
        if level not in (1, 2) or price <= 0:
            await interaction.response.send_message("Рівень 1/2, ціна > 0", ephemeral=True)
            return
        try:
            await add_contract_type(self.title_f.value.strip(), price, level)
        except Exception:
            await interaction.response.send_message("Не вдалося додати (назва вже є?)", ephemeral=True)
            return
        view = BackToAdminView(interaction.user.id, caller["rank"])
        await interaction.response.edit_message(
            content=f"✅ Контракт **{self.title_f.value}** (рівень {level}, {price}) додано", view=view
        )


class DeleteContractTypeModal(discord.ui.Modal, title="Видалити тип контракту"):
    ct_id_f = discord.ui.TextInput(label="ID типу контракту", min_length=1, max_length=10)

    async def on_submit(self, interaction: discord.Interaction):
        caller = await user_by_did(interaction.user.id)
        if not caller or caller["rank"] < 8:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return
        try:
            cid = int(self.ct_id_f.value.strip())
        except ValueError:
            await interaction.response.send_message("ID — число", ephemeral=True)
            return
        ct = await contract_type_by_id(cid)
        if not ct:
            await interaction.response.send_message("Тип не знайдено", ephemeral=True)
            return
        await deactivate_contract_type(cid)
        view = BackToAdminView(interaction.user.id, caller["rank"])
        await interaction.response.edit_message(
            content=f"✅ Тип **{ct['title']}** видалено", view=view
        )



class EditContractView(OwnedView):
    """Вибір автора контракту через @ щоб знайти його останній контракт."""
    def __init__(self, owner_id: int, caller_rank: int):
        super().__init__(owner_id)
        self.caller_rank = caller_rank

        sel = discord.ui.UserSelect(placeholder="Вибери автора контракту...")
        sel.callback = self._on_pick
        self.add_item(sel)

        back = discord.ui.Button(label="⬅️ Назад", style=discord.ButtonStyle.secondary, row=1)
        back.callback = self._on_back
        self.add_item(back)

    async def _on_pick(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return

        resolved = interaction.data.get("resolved", {}).get("users", {})
        did = int(list(resolved.keys())[0])

        last = get_last_contract(did)
        if not last:
            await interaction.response.edit_message(
                content=(
                    "❌ Немає збереженого контракту для цього гравця\n\n"
                    "Контракт зберігається до наступного виконання.\n\n"
                    "Вибери іншого автора:"
                ),
                view=EditContractView(interaction.user.id, self.caller_rank),
            )
            return

        current_parts = []
        for sid in last["participants"]:
            u = await user_by_sid(sid)
            if u:
                current_parts.append(f"• {u['game_name']} ({sid})")

        parts_txt = "\n".join(current_parts) or "немає"
        view = EditContractParticipantsView(
            interaction.user.id, self.caller_rank, last["contract_id"], last["title"]
        )
        await interaction.response.edit_message(
            content=(
                f"✏️ **Контракт #{last['contract_id']}**: {last['title']}\n\n"
                f"**Поточні учасники:**\n{parts_txt}\n\n"
                "Вибери **новий повний склад** учасників через @\n"
                "_(старий склад буде замінено повністю)_"
            ),
            view=view,
        )

    async def _on_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        u = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, u["rank"] if u else self.caller_rank)
        await interaction.response.edit_message(content="🛠 **Адмін-панель:**", view=view)


class EditContractParticipantsView(OwnedView):
    """UserSelect для вибору нового складу учасників контракту."""
    def __init__(self, owner_id: int, caller_rank: int, contract_id: int, contract_title: str):
        super().__init__(owner_id)
        self.caller_rank = caller_rank
        self.contract_id = contract_id
        self.contract_title = contract_title

        sel = discord.ui.UserSelect(
            placeholder="Вибери нових учасників...",
            min_values=1,
            max_values=10,
        )
        sel.callback = self._on_select
        self.add_item(sel)

        back = discord.ui.Button(label="⬅️ Назад", style=discord.ButtonStyle.secondary, row=1)
        back.callback = self._on_back
        self.add_item(back)

    async def _on_select(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return

        resolved = interaction.data.get("resolved", {}).get("users", {})
        new_dids = [int(uid) for uid in resolved.keys()]

        new_sids = []
        errors = []
        new_names = []
        for did in new_dids:
            db_user = await user_by_did(did)
            if not db_user:
                member = interaction.guild.get_member(did)
                name = member.display_name if member else str(did)
                errors.append(f"**{name}** не зареєстрований у боті")
            else:
                new_sids.append(db_user["static_id"])
                new_names.append(db_user["game_name"])

        if errors:
            await interaction.response.send_message(
                "❌ Деякі учасники не зареєстровані:\n" + "\n".join(errors),
                ephemeral=True,
            )
            return

        names_txt = "\n".join(f"• {n}" for n in new_names)
        sess(interaction.user.id)["edit_new_sids"] = new_sids
        sess(interaction.user.id)["edit_new_names"] = new_names

        view = EditContractConfirmView(
            interaction.user.id, self.caller_rank, self.contract_id, self.contract_title
        )
        await interaction.response.edit_message(
            content=(
                f"✏️ **Контракт #{self.contract_id}**: {self.contract_title}\n\n"
                f"**Новий склад ({len(new_sids)}):**\n{names_txt}\n\n"
                "Підтвердити зміну?"
            ),
            view=view,
        )

    async def _on_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        view = EditContractView(interaction.user.id, self.caller_rank)
        await interaction.response.edit_message(
            content="✏️ **Змінити контракт**\n\nВибери автора контракту через @:",
            view=view,
        )


class EditContractConfirmView(OwnedView):
    def __init__(self, owner_id: int, caller_rank: int, contract_id: int, contract_title: str):
        super().__init__(owner_id)
        self.caller_rank = caller_rank
        self.contract_id = contract_id
        self.contract_title = contract_title

    @discord.ui.button(label="✅ Підтвердити", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, _):
        s = sess(interaction.user.id)
        new_sids = s.get("edit_new_sids", [])
        new_names = s.get("edit_new_names", [])

        try:
            result = await replace_contract_participants(self.contract_id, new_sids)
        except Exception as e:
            await interaction.response.send_message(f"Помилка: {e}", ephemeral=True)
            return

        sess_clear(interaction.user.id)
        utxt = "\n".join(f"• {n}" for n in new_names)
        u = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, u["rank"] if u else self.caller_rank)
        await interaction.response.edit_message(
            content=(
                f"✅ Контракт #{self.contract_id} оновлено\n\n"
                f"Контракт: {self.contract_title}\n"
                f"Сума: {result['total_amount']} | Сім'ї: {result['family_amount']} | Кожному: {result['per_user_amount']}\n\n"
                f"Новий склад:\n{utxt}\n\n🛠 **Адмін-панель:**"
            ),
            view=view,
        )
        await notify(f"✏️ Контракт #{self.contract_id} змінено\nНові учасники:\n{utxt}")

    @discord.ui.button(label="❌ Скасувати", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _):
        sess_clear(interaction.user.id)
        u = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, u["rank"] if u else self.caller_rank)
        await interaction.response.edit_message(content="🛠 **Адмін-панель:**", view=view)


class DeleteContractModal(discord.ui.Modal, title="Видалити виконаний контракт"):
    cc_id_f = discord.ui.TextInput(label="ID виконаного контракту", min_length=1, max_length=10)

    async def on_submit(self, interaction: discord.Interaction):
        caller = await user_by_did(interaction.user.id)
        if not caller or caller["rank"] < 8:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return
        try:
            cid = int(self.cc_id_f.value.strip())
        except ValueError:
            await interaction.response.send_message("ID — число", ephemeral=True)
            return
        try:
            cc, ps = await delete_completed_contract(cid)
        except Exception as e:
            await interaction.response.send_message(f"Помилка: {e}", ephemeral=True)
            return
        ptxt = "\n".join(f"- {p['game_name']} ({p['static_id']})" for p in ps)
        view = BackToAdminView(interaction.user.id, caller["rank"])
        await interaction.response.edit_message(
            content=(
                f"✅ Контракт #{cid} видалено з відкатом\n\n"
                f"Контракт: {cc['contract_title']}\n"
                f"Учасники:\n{ptxt}"
            ),
            view=view,
        )
        await notify(f"🗑 Видалено контракт #{cid}: {cc['contract_title']}\n{ptxt}")


# =========================================================
# GIVE ROLE SELECT — вибір ролі після UserSelect
# =========================================================
class GiveRoleSelectView(OwnedView):
    def __init__(self, owner_id: int, target: discord.Member):
        super().__init__(owner_id)
        self.target = target

    @discord.ui.button(label="1 — 1 | Кандидат", style=discord.ButtonStyle.secondary, row=0)
    async def r1(self, i, _): await self._give(i, "1 | Кандидат")

    @discord.ui.button(label="2 — 2 | Резидент", style=discord.ButtonStyle.secondary, row=0)
    async def r2(self, i, _): await self._give(i, "2 | Резидент")

    @discord.ui.button(label="3 — 3 | Оператор", style=discord.ButtonStyle.secondary, row=0)
    async def r3(self, i, _): await self._give(i, "3 | Оператор")

    @discord.ui.button(label="4 — 4 | Основний склад", style=discord.ButtonStyle.secondary, row=1)
    async def r4(self, i, _): await self._give(i, "4 | Основний склад")

    @discord.ui.button(label="5 — 5 | Старший склад", style=discord.ButtonStyle.primary, row=1)
    async def r5(self, i, _): await self._give(i, "5 | Старший склад")

    @discord.ui.button(label="6 — Наставник", style=discord.ButtonStyle.primary, row=1)
    async def r6(self, i, _): await self._give(i, "Наставник")

    @discord.ui.button(label="7 — Куратор", style=discord.ButtonStyle.primary, row=2)
    async def r7(self, i, _): await self._give(i, "Куратор")

    @discord.ui.button(label="8 — Партнер", style=discord.ButtonStyle.danger, row=2)
    async def r8(self, i, _): await self._give(i, "Партнер")

    @discord.ui.button(label="❌ Скасувати", style=discord.ButtonStyle.secondary, row=4)
    async def cancel(self, interaction: discord.Interaction, _):
        caller = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, caller["rank"] if caller else 5)
        await interaction.response.edit_message(content="🛠 **Адмін-панель:**", view=view)

    async def _give(self, interaction: discord.Interaction, role_name: str):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return

        guild = interaction.guild
        role = discord.utils.get(guild.roles, name=role_name)
        if not role:
            await interaction.response.send_message(
                f"❌ Роль **{role_name}** не знайдена на сервері.\nСтвори її спочатку в налаштуваннях сервера.",
                ephemeral=True,
            )
            return

        # Знімаємо всі ролі рангів, додаємо нову + @FLY
        rank_role_names = list(RANK_NAMES.values())
        roles_to_remove = [r for r in self.target.roles if r.name in rank_role_names]
        fly_role = discord.utils.get(guild.roles, name="FLY")
        try:
            if roles_to_remove:
                await self.target.remove_roles(*roles_to_remove)
            roles_to_add = [role]
            if fly_role and fly_role not in self.target.roles:
                roles_to_add.append(fly_role)
            await self.target.add_roles(*roles_to_add)
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ Боту не вистачає прав для видачі ролі.\nПеревір що роль бота вища за роль учасника.",
                ephemeral=True,
            )
            return

        caller = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, caller["rank"] if caller else 5)
        await interaction.response.edit_message(
            content=(
                f"✅ Роль **{role_name}** видана учаснику **{self.target.display_name}**\n\n"
                "🛠 **Адмін-панель:**"
            ),
            view=view,
        )
        await notify(
            f"🎭 Видана роль **{role_name}**\n"
            f"Учасник: {self.target.display_name} (@{self.target.name})\n"
            f"Видав: {caller['game_name'] if caller else interaction.user}"
        )


# =========================================================
# WITHDRAWAL LIST VIEW
# =========================================================
class WithdrawalListView(OwnedView):
    def __init__(self, owner_id: int, caller_rank: int, rows: list):
        super().__init__(owner_id)
        self.caller_rank = caller_rank
        self.rows = rows
        self.page = 0
        self._build()

    def _build(self):
        self.clear_items()
        if not self.rows:
            return

        row = self.rows[self.page]

        if len(self.rows) > 1:
            prev = discord.ui.Button(
                label="◀️", style=discord.ButtonStyle.secondary,
                disabled=(self.page == 0), row=0
            )
            prev.callback = self._on_prev
            self.add_item(prev)

            next_btn = discord.ui.Button(
                label="▶️", style=discord.ButtonStyle.secondary,
                disabled=(self.page >= len(self.rows) - 1), row=0
            )
            next_btn.callback = self._on_next
            self.add_item(next_btn)

        if row["status"] == "new":
            approve = discord.ui.Button(label="✅ Підтвердити", style=discord.ButtonStyle.green, row=1)
            approve.callback = self._on_approve
            self.add_item(approve)

            reject = discord.ui.Button(label="❌ Відхилити", style=discord.ButtonStyle.red, row=1)
            reject.callback = self._on_reject
            self.add_item(reject)

        back = discord.ui.Button(label="⬅️ Назад", style=discord.ButtonStyle.secondary, row=2)
        back.callback = self._on_back
        self.add_item(back)

    def _current_content(self) -> str:
        if not self.rows:
            return "Заявок немає"
        row = self.rows[self.page]
        status_map = {
            "new": "🆕 Нова", "approved": "✅ Схвалена",
            "rejected": "❌ Відхилена", "paid": "💵 Виплачена"
        }
        return (
            f"╔══════════════════════╗\n"
            f"    💳 **ЗАЯВКИ НА ВИВІД**\n"
            f"╚══════════════════════╝\n"
            f"_{self.page + 1} з {len(self.rows)}_\n\n"
            f"👤 **{row['game_name']}**\n"
            f"💵 Сума: **${row['amount']:,}**\n"
            f"📊 Статус: {status_map.get(row['status'], row['status'])}\n"
            f"📅 Дата: {row['created_at'][:10]}"
        )

    async def _on_prev(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        self.page -= 1
        self._build()
        await interaction.response.edit_message(content=self._current_content(), view=self)

    async def _on_next(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        self.page += 1
        self._build()
        await interaction.response.edit_message(content=self._current_content(), view=self)

    async def _on_approve(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        rev = await user_by_did(interaction.user.id)
        if not rev or rev["rank"] < 8:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return
        row = self.rows[self.page]
        await wd_set_status(row["id"], "approved", rev["discord_id"])
        self.rows[self.page]["status"] = "approved"
        self._build()
        await interaction.response.edit_message(content=self._current_content(), view=self)
        target = bot.get_user(row["discord_id"])
        if target:
            try:
                await target.send(
                    f"✅ **Твій запит на вивід підтверджено!**\n\n"
                    f"💵 Сума: **${row['amount']:,}**\n"
                    f"Адміністратор підтвердив виплату."
                )
            except Exception:
                pass

    async def _on_reject(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        rev = await user_by_did(interaction.user.id)
        if not rev or rev["rank"] < 8:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return
        row = self.rows[self.page]
        await wd_set_status(row["id"], "rejected", rev["discord_id"])

        # Повернути гроші гравцю
        async with db() as cx:
            await cx.execute(
                "UPDATE users SET balance=balance+? WHERE discord_id=? AND is_active=1",
                (row["amount"], row["discord_id"]),
            )
            await cx.commit()

        self.rows[self.page]["status"] = "rejected"
        self._build()
        await interaction.response.edit_message(content=self._current_content(), view=self)
        target = bot.get_user(row["discord_id"])
        if target:
            try:
                await target.send(
                    f"❌ **Твій запит на вивід відхилено.**\n\n"
                    f"💵 Сума: **${row['amount']:,}**\n"
                    f"💰 Кошти повернуто на твій баланс."
                )
            except Exception:
                pass

    async def _on_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        u = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, u["rank"] if u else self.caller_rank)
        await interaction.response.edit_message(content="🛠 **Адмін-панель:**", view=view)


# =========================================================
# BONUS — видача премії гравцю (ранг 9+)
# =========================================================
class BonusUserPickView(OwnedView):
    def __init__(self, owner_id: int, caller_rank: int):
        super().__init__(owner_id)
        self.caller_rank = caller_rank

        sel = discord.ui.UserSelect(placeholder="Вибери гравця для премії...")
        sel.callback = self._on_pick
        self.add_item(sel)

        back = discord.ui.Button(label="⬅️ Назад", style=discord.ButtonStyle.secondary, row=1)
        back.callback = self._on_back
        self.add_item(back)

    async def _on_pick(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return

        resolved = interaction.data.get("resolved", {}).get("users", {})
        did = int(list(resolved.keys())[0])
        member = interaction.guild.get_member(did)
        db_user = await user_by_did(did)

        if not db_user:
            name = member.display_name if member else str(did)
            await interaction.response.edit_message(
                content=f"❌ **{name}** не зареєстрований у боті.",
                view=BonusUserPickView(interaction.user.id, self.caller_rank),
            )
            return

        if did == interaction.user.id:
            await interaction.response.send_message(
                "❌ Не можна видати премію самому собі", ephemeral=True
            )
            return

        await interaction.response.send_modal(
            BonusModal(interaction.user.id, self.caller_rank, db_user["discord_id"], db_user["game_name"])
        )

    async def _on_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        u = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, u["rank"] if u else self.caller_rank)
        await interaction.response.edit_message(content="🛠 **Адмін-панель:**", view=view)


class BonusModal(discord.ui.Modal, title="Видати премію"):
    amount_field = discord.ui.TextInput(
        label="Сума премії", placeholder="5000", min_length=1, max_length=10
    )
    reason_field = discord.ui.TextInput(
        label="Причина (необов'язково)", placeholder="За активну участь...",
        required=False, max_length=200
    )

    def __init__(self, owner_id: int, caller_rank: int, target_did: int, target_name: str):
        super().__init__()
        self.owner_id = owner_id
        self.caller_rank = caller_rank
        self.target_did = target_did
        self.target_name = target_name

    async def on_submit(self, interaction: discord.Interaction):
        caller = await user_by_did(interaction.user.id)
        if not caller or caller["rank"] < 9:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return

        try:
            amount = int(self.amount_field.value.strip())
        except ValueError:
            await interaction.response.send_message("Сума — число", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Сума > 0", ephemeral=True)
            return

        reason = self.reason_field.value.strip() if self.reason_field.value else "—"

        # Перевірити чи вистачає балансу сім'ї
        fam_bal = await family_balance()
        if amount > fam_bal:
            await interaction.response.send_message(
                f"❌ Недостатньо коштів у банку сім'ї.\n💵 Доступно: **${fam_bal:,}**",
                ephemeral=True,
            )
            return

        # Списати з балансу сім'ї і додати гравцю
        async with db() as cx:
            await cx.execute("UPDATE family_bank SET balance=balance-? WHERE id=1", (amount,))
            await cx.commit()
        await add_to_balance(self.target_did, amount)

        view = BackToAdminView(interaction.user.id, caller["rank"])
        await interaction.response.edit_message(
            content=(
                f"✅ Премія видана!\n\n"
                f"👤 Гравець: **{self.target_name}**\n"
                f"💵 Сума: **${amount:,}**\n"
                f"📝 Причина: {reason}"
            ),
            view=view,
        )

        # Сповістити гравця в ЛС
        target = bot.get_user(self.target_did)
        if target:
            try:
                await target.send(
                    f"🎁 **Тобі нарахована премія!**\n\n"
                    f"💵 Сума: **${amount:,}**\n"
                    f"📝 Причина: {reason}\n"
                    f"👤 Від: {caller['game_name']}"
                )
            except Exception:
                pass

        await notify(
            f"🎁 **Премія видана**\n\n"
            f"👤 Гравець: {self.target_name}\n"
            f"💵 Сума: **${amount:,}**\n"
            f"📝 Причина: {reason}\n"
            f"🎖️ Видав: {caller['game_name']}"
        )


# =========================================================
# TOP LEADERBOARD
# =========================================================
class TopSelectView(OwnedView):
    @discord.ui.button(label="🕰 За весь час", style=discord.ButtonStyle.blurple)
    async def alltime(self, interaction: discord.Interaction, _):
        rows = await get_top_alltime(10)
        if not rows:
            await interaction.response.edit_message(
                content="Даних поки немає.", view=BackView(interaction.user.id)
            )
            return
        active = [r for r in rows if r["contracts_count"] > 0]
        if not active:
            await interaction.response.edit_message(
                content="😔 Ще ніхто не виконав жодного контракту.",
                view=BackView(interaction.user.id),
            )
            return
        medal = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        lines = "\n".join(
            f"{medal[i] if i < len(medal) else '▸'} **{r['game_name']}** — {r['contracts_count']} контр."
            for i, r in enumerate(active)
        )
        total = sum(r["contracts_count"] for r in active)
        await interaction.response.edit_message(
            content=(
                "╔══════════════════════╗\n"
                "   🏆 **ТОП — ЗА ВСЬ ЧАС**\n"
                "╚══════════════════════╝\n\n"
                f"{lines}\n\n"
                f"📋 Всього контрактів у топ-10: **{total}**"
            ),
            view=BackView(interaction.user.id),
        )

    @discord.ui.button(label="📅 За тиждень", style=discord.ButtonStyle.blurple)
    async def weekly(self, interaction: discord.Interaction, _):
        rows = await get_top_weekly(10)
        start, end = get_week_bounds()
        start_dt = datetime.fromisoformat(start).strftime("%d.%m")
        end_dt = (datetime.fromisoformat(end) - timedelta(days=1)).strftime("%d.%m")
        fund = await get_weekly_family_fund()

        active = [r for r in rows if r["week_contracts"] > 0]
        if not active:
            await interaction.response.edit_message(
                content=(
                    "╔══════════════════════╗\n"
                    f"  📅 **ТОП {start_dt}–{end_dt}**\n"
                    "╚══════════════════════╝\n\n"
                    "😔 Цього тижня контрактів ще не виконано."
                ),
                view=BackView(interaction.user.id),
            )
            return

        medal = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        lines = "\n".join(
            f"{medal[i] if i < len(medal) else '▸'} **{r['game_name']}** — {r['week_contracts']} контр."
            for i, r in enumerate(active)
        )
        total_week = sum(r["week_contracts"] for r in active)
        await interaction.response.edit_message(
            content=(
                "╔══════════════════════╗\n"
                f"  📅 **ТОП {start_dt}–{end_dt}**\n"
                "╚══════════════════════╝\n\n"
                f"{lines}\n\n"
                f"📋 Всього контрактів за тиждень: **{total_week}**\n"
                f"🏦 Фонд тижня: **${fund:,}**"
            ),
            view=BackView(interaction.user.id),
        )

    @discord.ui.button(label="⬅️ Назад", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _):
        await send_main_menu(interaction, edit=True)


# =========================================================
# DONATE MODAL
# =========================================================
class DonateModal(discord.ui.Modal, title="Донат сім'ї"):
    amount_field = discord.ui.TextInput(
        label="Сума донату", placeholder="1000", min_length=1, max_length=10
    )

    async def on_submit(self, interaction: discord.Interaction):
        u = await user_by_did(interaction.user.id)
        try:
            amount = int(self.amount_field.value.strip())
        except ValueError:
            await interaction.response.send_message("Введи суму числом", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Сума > 0", ephemeral=True)
            return
        if amount > u["balance"]:
            await interaction.response.send_message(
                f"❌ Недостатньо коштів. Баланс: ${u['balance']:,}", ephemeral=True
            )
            return

        await add_to_family_balance(amount)
        async with db() as cx:
            await cx.execute(
                "UPDATE users SET balance=balance-? WHERE discord_id=? AND is_active=1",
                (amount, u["discord_id"]),
            )
            await cx.commit()

        bal = await family_balance()
        view = MainMenuView(interaction.user.id, u["rank"])
        await interaction.response.send_message(
            f"💎 Дякуємо за донат **${amount:,}** сім'ї!\n"
            f"🏦 Баланс сім'ї тепер: **${bal:,}**\n\n👇 Головне меню:",
            view=view, ephemeral=True,
        )
        await notify(
            f"💎 **Донат сім'ї**\n\n"
            f"👤 {u['game_name']} задонатив **${amount:,}**\n"
            f"🏦 Баланс сім'ї: **${bal:,}**"
        )


# =========================================================
# FAMILY BALANCE VIEW (перегляд + редагування для ранг 10)
# =========================================================
class FamilyBalanceView(OwnedView):
    def __init__(self, owner_id: int, caller_rank: int):
        super().__init__(owner_id)
        self.caller_rank = caller_rank

        if caller_rank >= 10:
            edit_btn = discord.ui.Button(
                label="✏️ Редагувати баланс", style=discord.ButtonStyle.primary
            )
            edit_btn.callback = self._on_edit
            self.add_item(edit_btn)

        back_btn = discord.ui.Button(
            label="⬅️ Назад", style=discord.ButtonStyle.secondary
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    async def _on_edit(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        caller = await user_by_did(interaction.user.id)
        if not caller or caller["rank"] < 10:
            await interaction.response.send_message("Тільки для рангу 10", ephemeral=True)
            return
        await interaction.response.send_modal(EditFamilyBalanceModal())

    async def _on_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Не твоє меню", ephemeral=True)
            return
        u = await user_by_did(interaction.user.id)
        view = AdminMenuView(interaction.user.id, u["rank"] if u else self.caller_rank)
        await interaction.response.edit_message(content="🛠 **Адмін-панель:**", view=view)


class EditFamilyBalanceModal(discord.ui.Modal, title="Редагувати баланс сім'ї"):
    new_balance = discord.ui.TextInput(
        label="Новий баланс сім'ї (число)", min_length=1, max_length=12
    )

    async def on_submit(self, interaction: discord.Interaction):
        caller = await user_by_did(interaction.user.id)
        if not caller or caller["rank"] < 10:
            await interaction.response.send_message("Тільки для рангу 10", ephemeral=True)
            return
        try:
            new_bal = int(self.new_balance.value.strip())
        except ValueError:
            await interaction.response.send_message("Введи число", ephemeral=True)
            return
        if new_bal < 0:
            await interaction.response.send_message("Баланс не може бути від'ємним", ephemeral=True)
            return

        old_bal = await family_balance()
        await set_family_balance(new_bal)

        u = await user_by_did(interaction.user.id)
        view = BackToAdminView(interaction.user.id, caller["rank"])
        await interaction.response.edit_message(
            content=(
                f"✅ Баланс сім'ї оновлено\n\n"
                f"Було: **${old_bal:,}**\n"
                f"Стало: **${new_bal:,}**"
            ),
            view=view,
        )
        await notify(
            f"✏️ Баланс сім'ї змінено\n"
            f"Було: ${old_bal:,} → Стало: ${new_bal:,}\n"
            f"Змінив: {caller['game_name']}"
        )


# =========================================================
# WEEKLY PAYOUT LOGIC
# =========================================================
async def build_payout_preview(prev_start: str, prev_end_str: str) -> dict | None:
    """Рахує виплати і повертає словник з даними для підтвердження."""
    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        row = await (await cx.execute("""
            SELECT COALESCE(SUM(family_amount), 0) as total
            FROM completed_contracts
            WHERE created_at >= ? AND created_at < ?
        """, (prev_start, prev_end_str))).fetchone()
        weekly_fund = row["total"] if row else 0

    if weekly_fund == 0:
        return None

    fam_bal = await family_balance()
    distribute = weekly_fund // 2
    family_keeps = weekly_fund - distribute

    async with db() as cx:
        cx.row_factory = aiosqlite.Row
        players = await (await cx.execute("""
            SELECT u.discord_id, u.game_name,
                   COUNT(ccp.id) as week_contracts
            FROM users u
            LEFT JOIN completed_contract_participants ccp ON ccp.discord_id = u.discord_id
            LEFT JOIN completed_contracts cc ON cc.id = ccp.completed_contract_id
                AND cc.created_at >= ? AND cc.created_at < ?
            WHERE u.is_active = 1
            GROUP BY u.discord_id
            ORDER BY week_contracts DESC
        """, (prev_start, prev_end_str))).fetchall()

    players = [dict(p) for p in players]
    active_players = [p for p in players if p["week_contracts"] > 0]
    if not active_players:
        return None

    top5 = active_players[:5]
    others = active_players[5:]
    top5_percents = [0.14, 0.08, 0.05, 0.04, 0.04]

    top5_payouts = []
    for i, p in enumerate(top5):
        pct = top5_percents[i] if i < len(top5_percents) else 0
        payout = int(distribute * pct)
        top5_payouts.append({"discord_id": p["discord_id"], "game_name": p["game_name"],
                              "payout": payout, "contracts": p["week_contracts"]})

    others_fund = int(distribute * 0.15)
    others_payouts = []
    if others and others_fund > 0:
        per_person = others_fund // len(others)
        if per_person > 0:
            for p in others:
                others_payouts.append({"discord_id": p["discord_id"], "game_name": p["game_name"],
                                       "payout": per_person, "contracts": p["week_contracts"]})

    total_to_distribute = sum(p["payout"] for p in top5_payouts) + sum(p["payout"] for p in others_payouts)

    return {
        "weekly_fund": weekly_fund,
        "fam_bal": fam_bal,
        "distribute": distribute,
        "family_keeps": family_keeps,
        "total_to_distribute": total_to_distribute,
        "top5": top5_payouts,
        "others": others_payouts,
        "prev_start": prev_start,
        "prev_end_str": prev_end_str,
    }


async def execute_payout(data: dict):
    """Виконує фактичні виплати після підтвердження."""
    fam_bal = await family_balance()
    if fam_bal < data["total_to_distribute"]:
        return False, f"Недостатньо коштів у банку сім'ї. Є: ${fam_bal:,}, потрібно: ${data['total_to_distribute']:,}"

    # Списуємо з балансу сім'ї
    async with db() as cx:
        await cx.execute(
            "UPDATE family_bank SET balance=balance-? WHERE id=1",
            (data["total_to_distribute"],)
        )
        await cx.commit()

    # Зараховуємо гравцям
    for p in data["top5"] + data["others"]:
        await add_to_balance(p["discord_id"], p["payout"])

    return True, None


def build_preview_text(data: dict) -> str:
    medal = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    top_lines = "\n".join(
        f"{medal[i]} **{p['game_name']}** — {p.get('contracts', '?')} контр. | ${p['payout']:,}"
        for i, p in enumerate(data["top5"])
    )
    others_lines = ""
    if data["others"]:
        others_lines = "\n\n👥 **Решта учасників:**\n" + "\n".join(
            f"▸ ${p['payout']:,}"
            for p in data["others"]
        )
        others_total = sum(p["payout"] for p in data["others"])
        others_lines += f"\n💵 Разом: **${others_total:,}**"

    return (
        "╔══════════════════════╗\n"
        "   💰 **ТИЖНЕВА ВИПЛАТА**\n"
        "╚══════════════════════╝\n\n"
        f"📊 Фонд тижня: **${data['weekly_fund']:,}**\n"
        f"🏦 Баланс сім'ї: **${data['fam_bal']:,}**\n"
        f"💸 До розподілу: **${data['distribute']:,}**\n"
        f"🏦 Залишається сім'ї: **${data['family_keeps']:,}**\n"
        f"💳 Буде виплачено: **${data['total_to_distribute']:,}**\n\n"
        f"🏆 **ТОП-5:**\n{top_lines}"
        f"{others_lines}"
    )


class WeeklyPayoutConfirmView(discord.ui.View):
    """Відправляється в ЛС адміну для підтвердження виплати."""
    def __init__(self, data: dict):
        super().__init__(timeout=None)
        self.data = data

    @discord.ui.button(label="✅ Підтвердити виплату", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, _):
        caller = await user_by_did(interaction.user.id)
        if not caller or caller["rank"] < 10:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return

        ok, err = await execute_payout(self.data)
        if not ok:
            await interaction.response.edit_message(content=f"❌ {err}", view=None)
            return

        await interaction.response.edit_message(
            content=build_preview_text(self.data) + "\n\n✅ **Виплату підтверджено та виконано!**",
            view=None,
        )

        # Публічне сповіщення в канал
        medal = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        top_lines = "\n".join(
            f"{medal[i]} **{p['game_name']}** — {p.get('contracts', '?')} контр. | ${p['payout']:,}"
            for i, p in enumerate(self.data["top5"])
        )
        others_lines = ""
        if self.data["others"]:
            others_lines = "\n\n👥 **Решта учасників:**\n" + "\n".join(
                f"▸ ${p['payout']:,}"
                for p in self.data["others"]
            )
            others_total = sum(p["payout"] for p in self.data["others"])
            others_lines += f"\n💵 Разом: **${others_total:,}**"

        pub_msg = (
            "╔══════════════════════╗\n"
            "   💰 **ТИЖНЕВА ВИПЛАТА**\n"
            "╚══════════════════════╝\n\n"
            f"📊 Фонд тижня: **${self.data['weekly_fund']:,}**\n"
            f"🏦 Залишається сім'ї: **${self.data['family_keeps']:,}**\n"
            f"💸 Розподілено: **${self.data['total_to_distribute']:,}**\n\n"
            f"🏆 **ТОП-5 ТИЖНЯ:**\n{top_lines}"
            f"{others_lines}"
        )
        await notify(pub_msg)
        print(f"✅ Тижнева виплата виконана. Фонд: ${self.data['weekly_fund']:,}")

    @discord.ui.button(label="✏️ Змінити фонд", style=discord.ButtonStyle.secondary)
    async def edit_fund(self, interaction: discord.Interaction, _):
        caller = await user_by_did(interaction.user.id)
        if not caller or caller["rank"] < 10:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return
        await interaction.response.send_modal(EditPayoutFundModal(self.data))

    @discord.ui.button(label="❌ Скасувати", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction, _):
        await interaction.response.edit_message(
            content="❌ Тижнева виплата скасована.", view=None
        )


class EditPayoutFundModal(discord.ui.Modal, title="Змінити фонд виплати"):
    new_fund = discord.ui.TextInput(
        label="Новий розмір фонду (замість авто)", min_length=1, max_length=12
    )

    def __init__(self, data: dict):
        super().__init__()
        self.data = data

    async def on_submit(self, interaction: discord.Interaction):
        try:
            new_amount = int(self.new_fund.value.strip())
        except ValueError:
            await interaction.response.send_message("Введи число", ephemeral=True)
            return
        if new_amount <= 0:
            await interaction.response.send_message("Сума > 0", ephemeral=True)
            return

        # Перерахувати виплати з новим фондом
        distribute = new_amount // 2
        family_keeps = new_amount - distribute
        top5_percents = [0.14, 0.08, 0.05, 0.04, 0.04]

        new_top5 = []
        for i, p in enumerate(self.data["top5"]):
            pct = top5_percents[i] if i < len(top5_percents) else 0
            new_top5.append({**p, "payout": int(distribute * pct)})

        others_fund = int(distribute * 0.15)
        new_others = []
        if self.data["others"]:
            total_others = len(self.data["others"])
            per_other = others_fund // total_others if total_others > 0 else 0
            for p in self.data["others"]:
                new_others.append({**p, "payout": per_other})

        total_dist = sum(p["payout"] for p in new_top5) + sum(p["payout"] for p in new_others)

        new_data = {
            **self.data,
            "weekly_fund": new_amount,
            "distribute": distribute,
            "family_keeps": family_keeps,
            "total_to_distribute": total_dist,
            "top5": new_top5,
            "others": new_others,
        }

        view = WeeklyPayoutConfirmView(new_data)
        await interaction.response.edit_message(
            content=build_preview_text(new_data) + "\n\n_Фонд змінено вручну. Підтвердь виплату:_",
            view=view,
        )


async def do_weekly_payout():
    """Рахує виплати і відправляє підтвердження адміну в ЛС."""
    now = datetime.now(timezone.utc)
    prev_end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    prev_start = (prev_end - timedelta(days=7)).isoformat()
    prev_end_str = prev_end.isoformat()

    data = await build_payout_preview(prev_start, prev_end_str)

    if not data:
        await notify("📊 Тижнева виплата: фонд порожній або ніхто не виконував контракти.")
        return

    preview = build_preview_text(data)
    view = WeeklyPayoutConfirmView(data)

    # Відправити підтвердження адміну в ЛС
    for admin_id in ADMIN_IDS:
        admin_user = bot.get_user(admin_id)
        if admin_user:
            try:
                await admin_user.send(
                    preview + "\n\n⚠️ **Підтверди тижневу виплату:**",
                    view=view,
                )
            except Exception as e:
                print(f"Не вдалося надіслати виплату адміну {admin_id}: {e}")


async def weekly_payout_loop():
    """Фоновий цикл — чекає до наступної неділі 00:00 UTC."""
    await bot.wait_until_ready()
    while not bot.is_closed():
        now = datetime.now(timezone.utc)
        # Наступна неділя 00:00 UTC
        days_until_sunday = (6 - now.weekday()) % 7
        if days_until_sunday == 0 and now.hour == 0 and now.minute < 5:
            # Зараз неділя і ще не минуло 5 хвилин — виплачуємо
            await do_weekly_payout()
            await asyncio.sleep(360)  # чекаємо 6 хвилин щоб не виплатити двічі
        else:
            if days_until_sunday == 0:
                days_until_sunday = 7
            next_sunday = (now + timedelta(days=days_until_sunday)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            seconds_until = (next_sunday - now).total_seconds()
            print(f"⏰ Наступна тижнева виплата через {seconds_until/3600:.1f} годин")
            await asyncio.sleep(min(seconds_until, 3600))  # перевіряємо кожну годину


# =========================================================
# WELCOME — кнопка реєстрації в каналі вітання
# =========================================================
class WelcomeView(discord.ui.View):
    """Постійна кнопка в каналі вітання — без timeout."""
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="📋 Реєстрація в боті",
        style=discord.ButtonStyle.green,
        custom_id="welcome_register",
    )
    async def register_btn(self, interaction: discord.Interaction, _):
        # Перевірити чи вже зареєстрований
        u_any = await user_by_did(interaction.user.id, only_active=False)
        if u_any and u_any["is_active"] == 0:
            await interaction.response.send_message(
                "❌ Твій доступ відключений адміністратором.", ephemeral=True
            )
            return
        if u_any:
            await interaction.response.send_message(
                f"✅ Ти вже зареєстрований як **{u_any['game_name']}**!\nНапиши `/fly` щоб відкрити меню.",
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(WelcomeRegisterModal())


class WelcomeRegisterModal(discord.ui.Modal, title="Реєстрація в сім'ї"):
    game_name = discord.ui.TextInput(
        label="Ігрове ім'я (Ім'я Прізвище)", placeholder="Sam Fly", min_length=3, max_length=50
    )
    static_id = discord.ui.TextInput(
        label="Static ID (1-5 символів)", placeholder="12345", min_length=1, max_length=5
    )
    real_name = discord.ui.TextInput(
        label="Реальне ім'я", placeholder="Іван", min_length=1, max_length=50
    )

    async def on_submit(self, interaction: discord.Interaction):
        gn = self.game_name.value.strip()
        sid = self.static_id.value.strip()
        rn = self.real_name.value.strip()

        if len(gn.split()) != 2:
            await interaction.response.send_message(
                "❌ Ігрове ім'я: «Ім'я Прізвище» (два слова). Спробуй ще раз.",
                ephemeral=True,
            )
            return

        async with db() as cx:
            cx.row_factory = aiosqlite.Row
            if await (await cx.execute("SELECT id FROM users WHERE game_name=?", (gn,))).fetchone():
                await interaction.response.send_message(
                    f"❌ Ігрове ім'я «{gn}» вже зайнято.", ephemeral=True
                )
                return
            if await (await cx.execute("SELECT id FROM users WHERE static_id=?", (sid,))).fetchone():
                await interaction.response.send_message(
                    f"❌ Static ID «{sid}» вже зайнято.", ephemeral=True
                )
                return

        try:
            await add_user({
                "discord_id": interaction.user.id,
                "username": str(interaction.user),
                "game_name": gn, "static_id": sid, "real_name": rn,
            })
        except Exception:
            await interaction.response.send_message("Помилка реєстрації. Спробуй знову.", ephemeral=True)
            return

        # Змінити нік
        new_nick = f"{gn} | {rn}"
        try:
            await interaction.user.edit(nick=new_nick)
        except discord.Forbidden:
            pass

        # Видати ролі @FLY і 1 | Кандидат
        guild = interaction.guild
        if guild:
            fly_role = discord.utils.get(guild.roles, name="FLY")
            passenger_role = discord.utils.get(guild.roles, name=PASSENGER_ROLE_NAME)
            roles_to_add = [r for r in [fly_role, passenger_role] if r]
            try:
                if roles_to_add:
                    await interaction.user.add_roles(*roles_to_add)
            except discord.Forbidden:
                pass

        await interaction.response.send_message(
            f"✅ **Ласкаво просимо, {gn}!**\n\n"
            f"🎖️ Ранг: **{rank_label(1)}**\n"
            f"🆔 Static ID: `{sid}`\n\n"
            "Напиши `/fly` у будь-якому каналі щоб відкрити меню.",
            ephemeral=True,
        )
        await notify(
            f"👤 Новий учасник: **{gn}**\n"
            f"Static ID: {sid} | Реальне ім'я: {rn}\n"
            f"Discord: {interaction.user}"
        )


async def post_welcome_message():
    """Розміщує постійне повідомлення з кнопкою реєстрації в каналі вітання."""
    channel = bot.get_channel(WELCOME_CHANNEL_ID)
    if not channel:
        print(f"⚠️ Канал вітання {WELCOME_CHANNEL_ID} не знайдено")
        return

    # Перевіряємо чи вже є повідомлення з кнопкою від бота
    async for msg in channel.history(limit=20):
        if msg.author == bot.user and msg.components:
            print("✅ Повідомлення реєстрації вже існує")
            return

    view = WelcomeView()
    await channel.send(
        "╔══════════════════════╗\n"
        "   👋 **ЛАСКАВО ПРОСИМО**\n"
        "╚══════════════════════╝\n\n"
        "Щоб стати частиною сім'ї — натисни кнопку нижче і заповни форму реєстрації.\n\n"
        "Після реєстрації ти отримаєш:\n"
        f"🎖️ Ранг **{rank_label(1)}**\n"
        "🏷️ Роль **@FLY**\n"
        "📋 Доступ до меню бота через `/fly`",
        view=view,
    )
    print("✅ Повідомлення реєстрації розміщено")


# =========================================================
# ON MEMBER JOIN
# =========================================================
@bot.event
async def on_member_join(member: discord.Member):
    """Коли новий учасник заходить — надсилаємо йому повідомлення в ЛС."""
    try:
        await member.send(
            f"👋 **Привіт, {member.display_name}!**\n\n"
            "Ласкаво просимо на сервер!\n\n"
            "Щоб зареєструватись у системі сім'ї — зайди в канал реєстрації "
            "і натисни кнопку **📋 Реєстрація в боті**.\n\n"
            "Після реєстрації ти отримаєш доступ до всіх функцій через `/fly`."
        )
    except discord.Forbidden:
        pass  # ЛС закриті


# =========================================================
# ON READY
# =========================================================
@bot.event
async def on_ready():
    await init_db()
    for oid in ADMIN_IDS:
        async with db() as cx:
            await cx.execute("UPDATE users SET rank=10 WHERE discord_id=?", (oid,))
            await cx.commit()
    # Реєструємо persistent view щоб кнопка працювала після перезапуску
    bot.add_view(WelcomeView())
    await tree.sync()
    bot.loop.create_task(weekly_payout_loop())
    await post_welcome_message()
    print(f"✅ Бот запущено як {bot.user}")
    print("✅ Slash-команди синхронізовано")
    print("✅ Пиши /fly у будь-якому каналі сервера")
    print("✅ Тижневий таск запущено")


# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    if BOT_TOKEN == "ВСТАВЬ_СЮДА_DISCORD_TOKEN":
        print("❌ Вставте Discord-токен у BOT_TOKEN")
    else:
        bot.run(BOT_TOKEN)