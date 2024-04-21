from __future__ import annotations

import asyncio
import json
import os
from typing import TYPE_CHECKING

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
        return {
            key: (str(value) if key in self.str_keys else (round(value, 2) if isinstance(value, float) else value))
            for (key, value) in dict(record).items()
        }

    async def open(self):
        Database.pool = await self.get_pool()
        # write history table to json

        # with open("history_table.json", "w") as f:
        #     table = await Database.pool.fetch("SELECT * FROM history;", timeout=60)
        #     table = [self.format_json(row) for row in table]
        #
        #     json.dump(table, f)

        # with open("guilds_table.json", "w") as f:
        #     table = await Database.pool.fetch("SELECT * FROM guilds;", timeout=60)
        #     table = [self.format_json(row) for row in table]
        #
        #     json.dump(table, f)

        # with open("player_metrics.json", "w") as f:
        #     table = await Database.pool.fetch("SELECT * FROM player_metrics;", timeout=60)
        #     table = [self.format_json(row) for row in table]
        #
        #     json.dump(table, f)
        # do the thing above but spread it into mul tiple files
        for i in range(0, 10):
            with open(f"player_metrics_{i}.json", "w") as f:
                # player_metrics doesn't have a id column so we can't use the modulo operator its around 1.5m rows
                table = await Database.pool.fetch(f"SELECT * FROM player_metrics LIMIT 150000 OFFSET {i * 150000};", timeout=60)


                table = [self.format_json(row) for row in table]
                json.dump(table, f)

        return self

    async def close(self):
        await Database.pool.close()
        return self


db = Database()
asyncio.run(db.open())
