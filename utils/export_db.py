from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, List
import json
import asyncpg
from dotenv import load_dotenv

if TYPE_CHECKING:
    pass

load_dotenv()
DB_IP = os.getenv("DB_IP")
DB_USER = os.getenv("DB_USER")
DB_PWD = os.getenv("DB_PWD")


class Database:

    pool: asyncpg.pool.Pool = None

    def __init__(self):
        self.str_keys = ['time_difference', 'capture_date']
        self.cached_guilds = {}

    @staticmethod
    async def get_pool():
        kwargs = {
            "host": DB_IP,
            "port": 5432,
            "user": DB_USER,
            "password": DB_PWD,
            "min_size": 3,
            "max_size": 10,
            "command_timeout": 60,
            "loop": asyncio.get_event_loop()
        }
        return await asyncpg.create_pool(**kwargs)

    def format_json(self, record: asyncpg.Record) -> dict:
        if record is None:
            return None
        return {key: (str(value) if key in self.str_keys else (round(value, 2) if isinstance(value, float) else value))
                for (key, value) in dict(record).items()}
    async def open(self):
        Database.pool = await self.get_pool()
        # write history table to json

        # with open("history_table.json", "w") as f:
        #     table = await Database.pool.fetch("SELECT * FROM history;", timeout=60)
        #     table = [self.format_json(row) for row in table]
        #
        #     json.dump(table, f)

        with open("guilds_table.json", "w") as f:
            table = await Database.pool.fetch("SELECT * FROM guilds;", timeout=60)
            table = [self.format_json(row) for row in table]

            json.dump(table, f)




        return self

    async def close(self):
        await Database.pool.close()
        return self



db = Database()
asyncio.run(db.open())

