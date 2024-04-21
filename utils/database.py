from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, List

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

    def __init__(self, app):
        self.app = app
        self.str_keys = ['time_difference']
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

    async def open(self):
        self.app.logger.info('Initializing database connection...')
        Database.pool = await self.get_pool()
        self.app.logger.info('Database connection initialized.')
        return self

    async def close(self):
        await Database.pool.close()
        self.app.logger.info('Database connection closed.')
        return self

    def format_json(self, record: asyncpg.Record) -> dict:
        if record is None:
            return None
        return {key: (str(value) if key in self.str_keys else (round(value, 2) if isinstance(value, float) else value))
                for (key, value) in dict(record).items()}


    # Player

