import asyncio
import aiosqlite

DB_NAME = "/data/database.db"

async def reset():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row

        ccs = await (await db.execute("""
            SELECT id, family_amount FROM completed_contracts
            WHERE created_at >= date('now', 'weekday 0', '-7 days')
        """)).fetchall()

        for cc in ccs:
            parts = await (await db.execute(
                "SELECT static_id, payout_amount FROM completed_contract_participants WHERE completed_contract_id=?",
                (cc["id"],)
            )).fetchall()

            for p in parts:
                await db.execute(
                    "UPDATE users SET balance=balance-?, contracts_count=MAX(0,contracts_count-1) WHERE static_id=?",
                    (p["payout_amount"], p["static_id"])
                )

            await db.execute(
                "UPDATE family_bank SET balance=MAX(0,balance-?) WHERE id=1",
                (cc["family_amount"],)
            )

        await db.execute("""
            DELETE FROM completed_contract_participants
            WHERE completed_contract_id IN (
                SELECT id FROM completed_contracts
                WHERE created_at >= date('now', 'weekday 0', '-7 days')
            )
        """)
        await db.execute("""
            DELETE FROM completed_contracts
            WHERE created_at >= date('now', 'weekday 0', '-7 days')
        """)
        await db.commit()
        print(f"Готово. Видалено {len(ccs)} контрактів з відкатом балансів.")

asyncio.run(reset())