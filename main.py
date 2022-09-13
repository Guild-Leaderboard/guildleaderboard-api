import asyncio
import datetime
import logging
import os
import re
from logging.config import dictConfig
from math import sin, ceil

import aiohttp
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from custom_logger import LogConfig
from utils.database import Database

load_dotenv(".env")
dictConfig(LogConfig().dict())


def weight_multiplier(members):
    frequency = sin(members / (125 / 0.927296)) + 0.2
    return members / 125 + (1 - members / 125) * frequency


class Time:
    def __init__(self):
        self.datetime: datetime.datetime = self.utcnow()
        self.time: int = self.datetime.timestamp()

    def __repr__(self):
        return f"<Time {self.time}>"

    @staticmethod
    def utcnow() -> datetime.datetime:
        """A helper function to return an aware UTC datetime representing the current time.

        This should be preferred to :meth:`datetime.datetime.utcnow` since it is an aware
        datetime, compared to the naive datetime in the standard library.

        .. versionadded:: 2.0

        Returns
        --------
        :class:`datetime.datetime`
            The current aware datetime in UTC.
        """
        return datetime.datetime.utcnow().replace(tzinfo=None)
        # return datetime.datetime.now(tz=pytz.utc).replace(tzinfo=None)
        # return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        # return datetime.datetime.now().replace(tzinfo=None)


class DatabaseCache:
    def __init__(self, app):
        self.app: App = app
        self._all_guilds = None
        self._all_guilds_time = None
        self.guilds = {}
        self.guild_metrics = {}
        self.player_data = {}
        self.player_metrics = {}

    async def get_guilds(self):
        if not self._all_guilds or Time().time - self._all_guilds_time > 60:
            r = await self.app.db.get_guilds()
            res = []
            for guild in r:
                guild["multiplier"] = weight_multiplier(guild["members"])

                res.append(guild)
            self._all_guilds = res
            self._all_guilds_time = Time().time
        return self._all_guilds

    async def get_guild(self, guild_id_or_name):
        guild = self.guilds.get(guild_id_or_name, {})
        if Time().time - guild.get("get_time", 0) > 60:
            # [a-z-0-9]{24}
            async with app.db.pool.acquire() as conn:
                if re.match(r"[a-z-0-9]{24}", guild_id_or_name):
                    guild = await self.app.db.get_guild(guild_id=guild_id_or_name, conn=conn)
                    if not guild:
                        guild = await self.app.db.get_guild(guild_name=guild_id_or_name, conn=conn)
                else:
                    guild = await self.app.db.get_guild(guild_name=guild_id_or_name, conn=conn)
                    if not guild:
                        guild = await self.app.db.get_guild(guild_id=guild_id_or_name, conn=conn)
                if not guild:
                    return

                guild["members"] = await app.db.get_players(guild["members"], conn)
                guild["discord"] = await app.db.get_guild_discord(guild["id"], conn)
                guild["multiplier"] = weight_multiplier(len(guild["members"]))

                self.guilds[guild["id"]] = guild
                self.guilds[guild["id"]]["get_time"] = Time().time

                self.guilds[guild["name"]] = guild
                self.guilds[guild["name"]]["get_time"] = Time().time
                for player in guild["members"]:
                    for key, value in player.items():
                        if isinstance(value, datetime.timedelta):
                            player[key] = str(value)
        return guild

    async def get_guild_metrics(self, guild_id):
        guild_metrics = self.guild_metrics.get(guild_id, {})

        if not guild_metrics or Time().time - guild_metrics.get("get_time", 0) > 60:
            r = await self.app.db.get_guild_metrics(guild_id)
            if not r:
                return
            for guild_metric in r:
                weight_m = weight_multiplier(guild_metric["member_count"])
                guild_metric["senither_weight"] = round(guild_metric["senither_weight"] * weight_m, 2)
                if guild_metric["lily_weight"] is None:
                    del guild_metric["lily_weight"]
                else:
                    guild_metric["lily_weight"] = round(guild_metric["lily_weight"] * weight_m, 2)
            self.guild_metrics[guild_id] = {
                "d": r,
                "get_time": Time().time,
            }
        return self.guild_metrics[guild_id]["d"]

    async def get_player(self, uuid_or_name):
        player_data = self.player_data.get(uuid_or_name.lower(), {})

        if not player_data or Time().time - player_data.get("get_time", 0) > 60:
            async with app.db.pool.acquire() as conn:
                if re.match(r"[0-9a-f]{32}", uuid_or_name):
                    r = await self.app.db.get_player(uuid=uuid_or_name, conn=conn)
                    if not r:
                        r = await self.app.db.get_player(name=uuid_or_name, conn=conn)
                else:
                    r = await self.app.db.get_player(name=uuid_or_name, conn=conn)
                    if not r:
                        r = await self.app.db.get_player(uuid=uuid_or_name, conn=conn)
                if not r:
                    return

                # r1 = await self.app.db.get_guild_player_from_history(r["uuid"], conn=conn)
                # guild_name = r1["guild_name"] if r1 and r1["type"] == "1" else None
                #
                # if not r1:
                r2 = await self.app.db.get_guild_player_from_guilds(r["uuid"], conn=conn)
                time_difference: datetime.datetime = r2["capture_date"]

                guild_name = r2["guild_name"] if r2 else None
                if (Time().datetime - time_difference).total_seconds() >= 25 * 3600:
                    guild_name = None

                r["guild_name"] = guild_name

                player_data = {
                    "d": r,
                    "get_time": Time().time,
                }

                self.player_data[r["uuid"]] = player_data
                self.player_data[r["name"].lower()] = player_data

        return self.player_data[uuid_or_name.lower()]["d"]

    async def get_player_metrics(self, uuid_or_name):
        player_metrics = self.player_metrics.get(uuid_or_name, {})

        if not player_metrics or Time().time - player_metrics.get("get_time", 0) > 60:
            async with app.db.pool.acquire() as conn:
                if re.match(r"[0-9a-f]{32}", uuid_or_name):
                    r = await self.app.db.get_player_metrics(uuid=uuid_or_name, conn=conn)
                    if not r:
                        r = await self.app.db.get_player_metrics(name=uuid_or_name, conn=conn)
                else:
                    r = await self.app.db.get_player_metrics(name=uuid_or_name, conn=conn)
                    if not r:
                        r = await self.app.db.get_player_metrics(uuid=uuid_or_name, conn=conn)
            if not r:
                return None
            dic1 = {
                "d": r,
                "get_time": Time().time,
            }

            self.player_metrics[r[0]['name'].lower()] = dic1
            self.player_metrics[r[0]['uuid']] = dic1
        return self.player_metrics[uuid_or_name.lower()]["d"]

    async def clean_db(self):
        while True:
            await asyncio.sleep(30)
            for key, value in self.guilds.copy().items():
                if Time().time - value.get("get_time", 0) > 60:
                    del self.guilds[key]

            for key, value in self.player_data.copy().items():
                if Time().time - value.get("get_time", 0) > 60:
                    del self.player_data[key]

            for key, value in self.guild_metrics.copy().items():
                if Time().time - value.get("get_time", 0) > 60:
                    del self.guild_metrics[key]


class App(FastAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db: Database = None
        self.loop: asyncio.BaseEventLoop = None
        self.session: aiohttp.ClientSession = None
        self.database_cache: DatabaseCache = DatabaseCache(self)
        self.logger = logging.getLogger("mycoolapp")
        self.patrons = None
        self.patreon_last_get = 0

    async def get_patreon_members(self):
        async with self.session.get("https://shieldsio-patreon.vercel.app/api/?username=sbhub") as r:
            data = await r.json()
            return data["message"].replace(" patrons", "")


app: App = App()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://guildleaderboard.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/leaderboard")
async def leaderboard():
    r = await app.database_cache.get_guilds()
    return r


@app.get("/stats")
async def stats():
    r = await app.database_cache.get_guilds()
    guilds_tracked = len(r)
    players_tracked = sum([i["members"] for i in r])
    if Time().time - app.patreon_last_get >= 3600:
        app.patrons = await app.get_patreon_members()
        app.patreon_last_get = Time().time

    return {
        "guilds_tracked": guilds_tracked,
        "players_tracked": players_tracked,
        "patrons": app.patrons,
    }


@app.get("/guild/{guild_id}")
async def guild(guild_id: str):
    r = await app.database_cache.get_guild(guild_id)
    try:
        r2 = r.copy()
        del r2["get_time"]
    except:
        r2 = r
    return r2


@app.get("/metrics/{guild_id}")
async def metrics(guild_id: str):
    app.logger.error('Deprecated endpoint used: /metrics/<guild_id>')
    return await guild_metrics(guild_id)


@app.get("/metrics/guild/{guild_id}")
async def guild_metrics(guild_id: str):
    r = await app.database_cache.get_guild_metrics(guild_id)
    return r if r else []


@app.get("/metrics/player/{uuid}")
async def player_metrics(uuid):
    r = await app.database_cache.get_player_metrics(uuid)
    return r


def fix_history_order(history: list) -> list:
    new_history = []
    last_action = ''
    reverse_history = history[::-1]
    skip_next = False
    for i in range(len(reverse_history)):
        if skip_next:
            skip_next = False
            continue

        if i == 0:
            new_history.append(reverse_history[i])
            last_action = reverse_history[i]["type"]
            continue
        if last_action == reverse_history[i]["type"]:
            try:
                new_history.insert(i, reverse_history[i + 1])
                new_history.insert(i + 1, reverse_history[i])
                last_action = reverse_history[i]["type"]
                skip_next = True
                continue
            except IndexError:
                new_history.append(reverse_history[i])
                last_action = reverse_history[i]["type"]

        else:
            new_history.append(reverse_history[i])
            last_action = reverse_history[i]["type"]

    return list(reversed(new_history))


@app.get("/v2/history")
async def history_v2(guild_id: str = None, uuid: str = None, per_page: int = 10, page: int = 1):
    if page < 1:
        page = 1
    if per_page < 1:
        per_page = 10
    if not uuid and not guild_id:
        return None

    r, total_rows = await app.db.get_history_v2(guild_id, uuid, per_page, page, return_total=True)

    return {
        "data": fix_history_order(r) if uuid else r,
        "paginate": {
            "current_page": page,
            "last_page": ceil(total_rows / per_page),
            "total": total_rows
        }
    }


@app.get("/player/{uuidorname}")
async def player(uuidorname: str):
    r = await app.database_cache.get_player(uuidorname)
    return r


@app.get("/autocomplete")
async def autocomplete():
    r = await app.db.get_id_name_autocomplete()
    return r


@app.on_event('startup')
async def start_up():
    if not app.loop:
        app.loop = asyncio.get_event_loop()
        app.loop.create_task(app.database_cache.clean_db())
    if not app.db:
        app.db = await Database(app).open()
    if not app.session:
        app.session = aiohttp.ClientSession(loop=app.loop)
        app.patrons = await app.get_patreon_members()
        app.patreon_last_get = Time().time


if __name__ == "__main__":
    import uvicorn

    if os.name == 'nt':
        uvicorn.run("main:app", reload=True, port=8080, host="0.0.0.0")
    else:
        uvicorn.run("main:app", reload=True, port=80, host="0.0.0.0")
