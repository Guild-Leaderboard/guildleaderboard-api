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
    """
CREATE TABLE players (
    uuid TEXT UNIQUE,
    name TEXT,
    senither_weight REAL,
    average_skill REAL,
    catacombs REAL,
    catacomb_xp REAL,
    total_slayer REAL,
    capture_date TIMESTAMP,
    scam_reason TEXT,
    lily_weight REAL,
    networth BIGINT,
    sb_experience BIGINT
)

CREATE TABLE player_metrics (
    uuid TEXT,
    name TEXT,
    capture_date TIMESTAMP,

    senither_weight REAL,
    lily_weight REAL,
    networth BIGINT,
    sb_experience BIGINT,
    zombie_xp BIGINT,
    spider_xp BIGINT,
    wolf_xp BIGINT,
    enderman_xp BIGINT,
    blaze_xp BIGINT,

    catacombs_xp BIGINT,
    catacombs REAL,
    healer REAL,
    healer_xp BIGINT,
    mage REAL,
    mage_xp BIGINT,
    berserk REAL,
    berserk_xp BIGINT,
    archer REAL,
    archer_xp BIGINT,
    tank REAL,
    tank_xp BIGINT,

    average_skill REAL,
    taming REAL,
    taming_xp BIGINT,
    mining REAL,
    mining_xp BIGINT,
    farming REAL,
    farming_xp BIGINT,
    combat REAL,
    combat_xp BIGINT,
    foraging REAL,
    foraging_xp BIGINT,
    fishing REAL,
    fishing_xp BIGINT,
    enchanting REAL,
    enchanting_xp BIGINT,
    alchemy REAL,
    alchemy_xp BIGINT,
    carpentry REAL,
    carpentry_xp BIGINT
)
"""

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


    async def get_names(self, uuids):
        r = await self.pool.fetch("""
SELECT uuid, name FROM players WHERE uuid = ANY($1)""", uuids)
        return {row['uuid']: row['name'] for row in r}

    # Player

    async def get_player(self, uuid: str = None, name: str = None, conn=None) -> dict:
        query_str = f"""
SELECT * FROM player_metrics WHERE {'uuid = $1' if uuid else 'lower(name) = lower($1)'} ORDER BY capture_date DESC LIMIT 1
        """
        if conn:
            r = await conn.fetchrow(query_str, uuid if uuid else name)
        else:
            r = await self.pool.fetchrow(query_str, uuid if uuid else name)
        return self.format_json(r) if r else None

    async def get_players(self, uuids: List[str], conn=None):
        query_str = """
SELECT 
    uuid,
    name,
    ROUND(senither_weight::numeric, 2)::float AS senither_weight, 
    ROUND(lily_weight::numeric, 2)::float AS lily_weight, 
    ROUND(average_skill::numeric, 2)::float AS average_skill, 
    ROUND(catacombs::numeric, 2)::float AS catacombs, 
    ROUND(total_slayer::numeric, 2)::float AS total_slayer, 
    networth,
    sb_experience,
    capture_date, 
    scam_reason 
FROM players 
WHERE 
    uuid = ANY($1);
        """
        if conn:
            r = await conn.fetch(query_str, uuids)
        else:
            r = await self.pool.fetch(query_str, uuids)
        return [self.format_json(row) for row in r]

    async def get_player_metrics(self, uuid: str = None, name: str = None, conn=None) -> List[dict]:
        query_str = f"""
SELECT
    *
FROM player_metrics
WHERE {'uuid = $1' if uuid else 'lower(name) = lower($1)'}
ORDER BY capture_date    
        """
        if conn:
            r = await conn.fetch(query_str, uuid if uuid else name)
        else:
            r = await self.pool.fetch(query_str, uuid if uuid else name)
        return [self.format_json(row) for row in r] if r else []

    async def get_player_page(
            self, sort_by: str, reverse: bool = False, page=1, return_total=False, username: str = None
    ):
        if sort_by not in [
            "senither_weight", "lily_weight", "average_skill", "catacombs", "catacomb_xp", "total_slayer", "networth", 'sb_experience'
        ]:
            raise ValueError("Invalid sort_by value")

        offset = (page - 1) * 25
        # (SELECT guild_name FROM guilds WHERE uuid = ANY(players) ORDER BY capture_date DESC LIMIT 1) AS guild_name
        # (SELECT count(*) FROM players WHERE {sort_by} IS NOT NULL AND (NOW()::date - '3 day'::interval) < capture_date AND {sort_by} > p.{sort_by}) + 1 AS position

        # get the position of the player
        qry_str = f"""
SELECT
    *
    {f", (SELECT count(*) FROM players WHERE {sort_by} IS NOT NULL AND (NOW()::date - '3 day'::interval) < capture_date AND {sort_by} > p.{sort_by}) + 1 AS position" if username else ''}    
FROM players {f'p' if username else ''}
    WHERE {sort_by} IS NOT NULL AND (NOW()::date - '3 day'::interval) < capture_date {'AND starts_with(lower(name), lower($2))' if username else ''}
ORDER by {sort_by} {'' if reverse else 'DESC'}
OFFSET $1
LIMIT 25;
        """
        args = [int(offset), username] if username else [int(offset)]
        r = await self.pool.fetch(qry_str, *args)
        r_vals = [[self.format_json(row) for row in r] if r else []]
        # get the position of the players

        if return_total:
            args = [username] if username else []
            total = await self.pool.fetchval(f"""
SELECT 
    count(*) 
FROM players 
WHERE {sort_by} IS NOT NULL AND (NOW()::date - '3 day'::interval) < capture_date {'AND starts_with(lower(name), lower($1))' if username else ''}
""", *args)
            r_vals.append(total)

        return r_vals

