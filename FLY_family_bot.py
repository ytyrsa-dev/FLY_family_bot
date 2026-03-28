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
from discord import app_commands
from discord.ext import commands

# =========================================================
# CONFIG
# =========================================================
import os
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS = {int(os.environ.get("ADMIN_IDS", "0"))}
NOTIFY_CHANNEL_ID = int(os.environ.get("NOTIFY_CHANNEL_ID", "0"))

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
    fam = int(total * 0.10)
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
        fam = int(total * 0.10)
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
                "Це не твоє меню. Напиши `/start` щоб відкрити своє.", ephemeral=True
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
        txt = "Ти не зареєстрований. Напиши `/start`"
        if edit:
            await interaction.response.edit_message(content=txt, view=None)
        else:
            await interaction.response.send_message(txt, ephemeral=True)
        return

    view = MainMenuView(interaction.user.id, u["rank"])
    txt = (
        f"👋 **Головне меню**\n\n"
        f"👤 {u['game_name']}  |  {rank_label(u['rank'])}  |  💰 {u['balance']}"
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

    @discord.ui.button(label="👤 Профіль", style=discord.ButtonStyle.secondary, row=0)
    async def profile(self, interaction: discord.Interaction, _):
        u = await user_by_did(interaction.user.id)
        txt = (
            "👤 **Профіль**\n\n"
            f"Ігрове ім'я: {u['game_name']}\n"
            f"Static ID: {u['static_id']}\n"
            f"Реальне ім'я: {u['real_name']}\n\n"
            f"Ранг: {rank_label(u['rank'])}\n"
            f"Баланс: {u['balance']}\n"
            f"Контракти: {u['contracts_count']}"
        )
        await interaction.response.edit_message(content=txt, view=BackView(interaction.user.id))

    @discord.ui.button(label="💰 Баланс", style=discord.ButtonStyle.secondary, row=0)
    async def balance(self, interaction: discord.Interaction, _):
        u = await user_by_did(interaction.user.id)
        await interaction.response.edit_message(
            content=f"💰 Твій баланс: **{u['balance']}**",
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
            txt = "📄 Ти ще не брав участі в жодному контракті."
        else:
            lines = "\n".join(
                f"#{r['id']} | {r['contract_title']} | +{r['payout_amount']} | {r['created_at'][:10]}"
                for r in rows
            )
            txt = f"📄 **Мої контракти** (останні {len(rows)}):\n```\n{lines}\n```"
        await interaction.response.edit_message(content=txt, view=BackView(interaction.user.id))

    @discord.ui.button(label="💸 Вивід коштів", style=discord.ButtonStyle.primary, row=1)
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

    @discord.ui.button(label="🛠 Адмін-панель", style=discord.ButtonStyle.danger, row=3)
    async def admin_panel(self, interaction: discord.Interaction, _):
        u = await user_by_did(interaction.user.id)
        if u["rank"] < 7:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return
        view = AdminMenuView(interaction.user.id, u["rank"])
        await interaction.response.edit_message(content="🛠 **Адмін-панель**:", view=view)


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
                "❌ Ігрове ім'я: «Ім'я Прізвище» (два слова). Натисни `/start` знову.", ephemeral=True
            )
            return

        async with db() as cx:
            cx.row_factory = aiosqlite.Row
            if await (await cx.execute("SELECT id FROM users WHERE game_name=?", (gn,))).fetchone():
                await interaction.response.send_message(
                    f"❌ Ігрове ім'я «{gn}» вже зайнято. Натисни `/start` знову.", ephemeral=True
                )
                return
            if await (await cx.execute("SELECT id FROM users WHERE static_id=?", (sid,))).fetchone():
                await interaction.response.send_message(
                    f"❌ Static ID «{sid}» вже зайнято. Натисни `/start` знову.", ephemeral=True
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
@tree.command(name="start", description="Відкрити меню")
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
                f"❌ Баланс: {u['balance']}. Недостатньо.", ephemeral=True
            )
            return
        wid = await create_wd(u["discord_id"], u["game_name"], amount)
        view = MainMenuView(interaction.user.id, u["rank"])
        await interaction.response.send_message(
            f"✅ Заявка **#{wid}** на вивід **{amount}** відправлена.\n\n👇 Головне меню:",
            view=view, ephemeral=True,
        )
        wd_view = WithdrawalAdminView(wid)
        await notify(
            f"💸 Нова заявка на вивід **#{wid}**\nГравець: {u['game_name']}\nСума: {amount}",
            view=wd_view,
        )


# =========================================================
# WITHDRAWAL ADMIN VIEW (в каналі сповіщень)
# =========================================================
class WithdrawalAdminView(discord.ui.View):
    """Кнопки у каналі сповіщень — без обмеження owner."""
    def __init__(self, wid: int):
        super().__init__(timeout=None)
        self.wid = wid

    @discord.ui.button(label="✅ Схвалити", style=discord.ButtonStyle.green)
    async def approve(self, interaction: discord.Interaction, _):
        rev = await user_by_did(interaction.user.id)
        if not rev or rev["rank"] < 8:
            await interaction.response.send_message("Нема доступу", ephemeral=True)
            return
        req = await wd_by_id(self.wid)
        if not req or req["status"] != "new":
            await interaction.response.send_message(f"Вже оброблено: {req['status'] if req else '?'}", ephemeral=True)
            return
        await wd_set_status(self.wid, "approved", rev["discord_id"])
        await interaction.response.edit_message(
            content=interaction.message.content + f"\n✅ Схвалено: {rev['game_name']}", view=None
        )
        target = bot.get_user(req["discord_id"])
        if target:
            try:
                await target.send(f"✅ Заявку на вивід **#{self.wid}** схвалено!")
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
        await interaction.response.edit_message(
            content=interaction.message.content + f"\n❌ Відхилено: {rev['game_name']}", view=None
        )
        target = bot.get_user(req["discord_id"])
        if target:
            try:
                await target.send(f"❌ Заявку на вивід **#{self.wid}** відхилено.")
            except Exception:
                pass

    @discord.ui.button(label="💵 Виплачено", style=discord.ButtonStyle.blurple)
    async def paid(self, interaction: discord.Interaction, _):
        rev = await user_by_did(interaction.user.id)
        if not rev or rev["rank"] < 9:
            await interaction.response.send_message("Нема доступу (ранг 9+)", ephemeral=True)
            return
        req = await wd_by_id(self.wid)
        if not req:
            await interaction.response.send_message("Заявку не знайдено", ephemeral=True)
            return
        if req["status"] == "paid":
            await interaction.response.send_message("Вже виплачено", ephemeral=True)
            return
        if req["status"] != "approved":
            await interaction.response.send_message("Спочатку схваліть", ephemeral=True)
            return
        player = await user_by_did(req["discord_id"])
        if not player or player["balance"] < req["amount"]:
            await interaction.response.send_message("Недостатньо балансу у гравця", ephemeral=True)
            return
        await deduct_balance(req["discord_id"], req["amount"])
        await wd_set_paid(self.wid, rev["discord_id"])
        await interaction.response.edit_message(
            content=interaction.message.content + f"\n💵 Виплачено: {rev['game_name']}", view=None
        )
        target = bot.get_user(req["discord_id"])
        if target:
            try:
                await target.send(f"💵 Заявку **#{self.wid}** відмічено як виплачену.")
            except Exception:
                pass


# =========================================================
# FILL CONTRACT FLOW
# =========================================================
class LevelSelectView(OwnedView):
    def __init__(self, owner_id: int, flow: str):
        super().__init__(owner_id)
        self.flow = flow

    @discord.ui.button(label="1️⃣ Рівень 1", style=discord.ButtonStyle.primary)
    async def lvl1(self, interaction: discord.Interaction, _):
        await self._pick(interaction, 1)

    @discord.ui.button(label="2️⃣ Рівень 2", style=discord.ButtonStyle.primary)
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
        fam = int(total * 0.10)
        per = (total - fam) // len(ids)
        names_txt = "\n".join(f"• {n}" for n in s["p_names"])

        await interaction.response.edit_message(
            content=(
                "📋 **Перевір контракт:**\n\n"
                f"Контракт: {s['ct_title']}\n"
                f"Сума: {total}\n"
                f"Сім'ї (10%): {fam}\n"
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
                f"👤 **Профіль гравця:**\n\n"
                f"Ігрове ім'я: {db_user['game_name']}\n"
                f"Static ID: {db_user['static_id']}\n"
                f"Реальне ім'я: {db_user['real_name']}\n"
                f"Discord: {member.mention if member else did}\n\n"
                f"Ранг: {rank_label(db_user['rank'])}\n"
                f"Баланс: {db_user['balance']}\n"
                f"Контракти: {db_user['contracts_count']}\n"
                f"Статус: {'✅ Активний' if db_user['is_active'] else '❌ Деактивований'}"
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
    @discord.ui.button(label="1️⃣ Рівень 1", style=discord.ButtonStyle.primary)
    async def lvl1(self, interaction: discord.Interaction, _):
        await self._pick(interaction, 1)

    @discord.ui.button(label="2️⃣ Рівень 2", style=discord.ButtonStyle.primary)
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

        # Відправити в канал нотифікацій
        channel = bot.get_channel(NOTIFY_CHANNEL_ID)
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

    @discord.ui.button(label="📋 Типи контрактів", style=discord.ButtonStyle.secondary, row=0)
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

    @discord.ui.button(label="👥 Гравці", style=discord.ButtonStyle.secondary, row=0)
    async def players_list(self, interaction: discord.Interaction, _):
        view = PlayerLookupView(interaction.user.id, self.rank)
        await interaction.response.edit_message(
            content="👥 **Гравці**\n\nВибери учасника через @ щоб переглянути його дані:",
            view=view,
        )

    @discord.ui.button(label="💳 Заявки на вивід", style=discord.ButtonStyle.secondary, row=0)
    async def wd_list(self, interaction: discord.Interaction, _):
        if self.rank < 8:
            await interaction.response.send_message("Нема доступу (ранг 8+)", ephemeral=True)
            return
        rows = await all_wd(20)
        if not rows:
            await interaction.response.send_message("Заявок немає", ephemeral=True)
            return
        lines = "\n".join(
            f"#{r['id']} | {r['game_name']} | {r['amount']} | {r['status']} | {r['created_at'][:10]}"
            for r in rows
        )
        await interaction.response.edit_message(
            content=f"💳 **Заявки на вивід:**\n```\n{lines}\n```",
            view=BackToAdminView(interaction.user.id, self.rank),
        )

    @discord.ui.button(label="🪖 Встановити ранг", style=discord.ButtonStyle.primary, row=1)
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
        if self.rank < 8:
            await interaction.response.send_message("Нема доступу (ранг 8+)", ephemeral=True)
            return
        bal = await family_balance()
        await interaction.response.edit_message(
            content=f"🏦 Баланс сім'ї: **{bal}**",
            view=BackToAdminView(interaction.user.id, self.rank),
        )

    @discord.ui.button(label="➕ Додати тип контракту", style=discord.ButtonStyle.green, row=2)
    async def add_ct(self, interaction: discord.Interaction, _):
        if self.rank < 8:
            await interaction.response.send_message("Нема доступу (ранг 8+)", ephemeral=True)
            return
        await interaction.response.send_modal(AddContractTypeModal())

    @discord.ui.button(label="🗑 Видалити тип контракту", style=discord.ButtonStyle.red, row=2)
    async def del_ct(self, interaction: discord.Interaction, _):
        if self.rank < 8:
            await interaction.response.send_message("Нема доступу (ранг 8+)", ephemeral=True)
            return
        await interaction.response.send_modal(DeleteContractTypeModal())

    @discord.ui.button(label="🗑 Видалити гравця", style=discord.ButtonStyle.red, row=3)
    async def del_player(self, interaction: discord.Interaction, _):
        if self.rank < 10:
            await interaction.response.send_message("Нема доступу (тільки ранг 10)", ephemeral=True)
            return
        view = UserPickView(interaction.user.id, action="del_player", caller_rank=self.rank)
        await interaction.response.edit_message(
            content="🗑 **Видалити гравця**\n\nВибери учасника зі списку:", view=view
        )

    @discord.ui.button(label="✏️ Змінити контракт", style=discord.ButtonStyle.primary, row=3)
    async def edit_cc(self, interaction: discord.Interaction, _):
        if self.rank < 8:
            await interaction.response.send_message("Нема доступу (ранг 8+)", ephemeral=True)
            return
        await interaction.response.send_modal(EditContractModal())

    @discord.ui.button(label="🗑 Видалити контракт", style=discord.ButtonStyle.red, row=4)
    async def del_cc(self, interaction: discord.Interaction, _):
        if self.rank < 8:
            await interaction.response.send_message("Нема доступу (ранг 8+)", ephemeral=True)
            return
        await interaction.response.send_modal(DeleteContractModal())

    @discord.ui.button(label="🎭 Видати роль гравцю", style=discord.ButtonStyle.primary, row=3)
    async def give_role(self, interaction: discord.Interaction, _):
        if self.rank < 6:
            await interaction.response.send_message("Нема доступу (ранг 6+)", ephemeral=True)
            return
        view = UserPickView(interaction.user.id, action="give_role", caller_rank=self.rank)
        await interaction.response.edit_message(
            content="🎭 **Видати роль**\n\nВибери учасника зі списку:", view=view
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
            for r in range(1, caller_rank)  # тільки ранги нижче свого
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



class EditContractModal(discord.ui.Modal, title="Змінити учасників контракту"):
    cc_id_f = discord.ui.TextInput(label="ID виконаного контракту", min_length=1, max_length=10)
    sids_f = discord.ui.TextInput(label="Нові static ID через кому", placeholder="111,222,333", min_length=1, max_length=100)

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
        ids = parse_static_ids(self.sids_f.value)
        if not ids or len(ids) > 10:
            await interaction.response.send_message("1-10 учасників", ephemeral=True)
            return
        try:
            result = await replace_contract_participants(cid, ids)
        except Exception as e:
            await interaction.response.send_message(f"Помилка: {e}", ephemeral=True)
            return
        utxt = "\n".join(f"- {u['game_name']} ({u['static_id']})" for u in result["participants"])
        view = BackToAdminView(interaction.user.id, caller["rank"])
        await interaction.response.edit_message(
            content=(
                f"✅ Контракт #{cid} оновлено\n\n"
                f"Контракт: {result['contract_title']}\n"
                f"Сума: {result['total_amount']} | Сім'ї: {result['family_amount']} | Кожному: {result['per_user_amount']}\n\n"
                f"Новий склад:\n{utxt}"
            ),
            view=view,
        )
        await notify(f"✏️ Контракт #{cid} змінено\nНові учасники:\n{utxt}")


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

    @discord.ui.button(label="7 — Командир", style=discord.ButtonStyle.primary, row=2)
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

        # Знімаємо всі ролі рангів, додаємо нову
        rank_role_names = list(RANK_NAMES.values())
        roles_to_remove = [r for r in self.target.roles if r.name in rank_role_names]
        try:
            if roles_to_remove:
                await self.target.remove_roles(*roles_to_remove)
            await self.target.add_roles(role)
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
# ON READY
# =========================================================
@bot.event
async def on_ready():
    await init_db()
    for oid in ADMIN_IDS:
        async with db() as cx:
            await cx.execute("UPDATE users SET rank=10 WHERE discord_id=?", (oid,))
            await cx.commit()
    await tree.sync()
    print(f"✅ Бот запущено як {bot.user}")
    print("✅ Slash-команди синхронізовано")
    print("✅ Пиши /start у будь-якому каналі сервера")


# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    if BOT_TOKEN == "ВСТАВЬ_СЮДА_DISCORD_TOKEN":
        print("❌ Вставте Discord-токен у BOT_TOKEN")
    else:
        bot.run(BOT_TOKEN)
