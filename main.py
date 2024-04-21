import asyncio
import logging
import os
import re
import time
from logging.config import dictConfig
from math import sin, ceil

import aiohttp
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from custom_logger import LogConfig
from utils.cache import Cache
from utils.database2 import Database2

load_dotenv(".env")
dictConfig(LogConfig().dict())


def weight_multiplier(members):
    frequency = sin(members / (125 / 0.927296)) + 0.2
    return members / 125 + (1 - members / 125) * frequency


class App(FastAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db: Database2 = None
        self.cache: Cache = None
        self.loop: asyncio.BaseEventLoop = None
        self.session: aiohttp.ClientSession = None

        self.logger = logging.getLogger("mycoolapp")
        self.patrons = None
        self.patreon_last_get = 0

    async def get_patreon_members(self):
        async with self.session.get("https://shieldsio-patreon.vercel.app/api/?username=sbhub") as r:
            data = await r.json()
            return data["message"].replace(" patrons", "")


app: App = App(redoc_url="/randomurlthatnooneshouldbeabletofind",
               openapi_url='/randomurlthatnooneshouldbeabletofindopenapi', docs_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "https://guildleaderboard.com",
        'https://guildleaderboard-frontend-next.vercel.app/',
        '*'

    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/stats")
async def stats():
    return await app.cache.jget("stats")


@app.get("/leaderboard")
async def leaderboard():
    return await app.cache.jget("guilds")


@app.get("/leaderboard/player")
async def playerleaderboard(
        sort_by: str = 'senither_weight', page: int = 1, reverse: bool = False, username: str = None
):
    r, total_rows = app.db.get_player_page(sort_by, reverse, page, username=username)
    return {
        "data": r,
        "paginate": {
            "current_page": page,
            "last_page": ceil(total_rows / 25),
            "total": total_rows
        }
    }


async def determine_guild_input(guild_input: str) -> str:
    # check if guild_input is a id
    if re.match(r"[a-z-0-9]{24}", guild_input):
        return guild_input
    # check if guild_input is in the autocomplete list
    guild_name_id_list = await app.cache.jget("autocomplete")
    for i in guild_name_id_list:
        if i["name"].lower() == guild_input.lower():
            return i["id"]


@app.get("/guild/{guild_input}")
async def guild(guild_input: str):
    guild_id = await determine_guild_input(guild_input)
    return await app.cache.jget(f"guild_{guild_id}")


@app.get("/metrics/guild/{guild_id}")
async def guild_metrics(guild_id: str):
    return await app.cache.jget(f"guild_{guild_id}_metrics")


@app.get("/metrics/player/{uuid}")
async def player_metrics(uuid):
    return app.db.get_player_metrics(uuid)


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


@app.get("/history")
async def history_v2(guild_id: str = None, uuid: str = None, per_page: int = 10, page: int = 1):
    if page < 1:
        page = 1
    if per_page < 1:
        per_page = 10
    if not uuid and not guild_id:
        return None

    r, total_rows = await app.db.get_history(guild_id, uuid, per_page, page)

    return {
        "data": fix_history_order(r) if uuid else r,
        "paginate": {
            "current_page": page,
            "last_page": ceil(total_rows / per_page),
            "total": total_rows
        }
    }


def determine_player_input(player_input: str) -> str:
    if re.match(r"[0-9a-f]{32}", player_input):
        return 'uuid'
    return 'name'


@app.get("/player/{player_input}")
async def player(player_input: str):
    return app.db.get_player(**{
        determine_player_input(player_input): player_input
    })


@app.get("/autocomplete")
async def autocomplete():
    return await app.cache.jget("autocomplete")


@app.get("/sitemapurls")
async def sitemapurls():
    return await app.cache.jget("sitemap")


@app.on_event('startup')
async def start_up():
    if not app.loop:
        app.loop = asyncio.get_event_loop()
        # app.loop.create_task(app.database_cache.clean_db())
    if not app.db:
        app.db = Database2(app)
    if not app.cache:
        app.cache = Cache(app)
        app.loop.create_task(app.cache.update_cache())

    if not app.session:
        app.session = aiohttp.ClientSession(loop=app.loop)
        app.patrons = await app.get_patreon_members()
        app.patreon_last_get = time.time()


if __name__ == "__main__":
    import uvicorn

    if os.name == 'nt':
        uvicorn.run("main:app", reload=True, port=8080, host="0.0.0.0")
    else:
        uvicorn.run("main:app", reload=True, port=80, host="0.0.0.0")
