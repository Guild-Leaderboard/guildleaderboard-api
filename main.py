import asyncio
import datetime
import os
import re
from math import sin, ceil

import aiohttp
from dotenv import load_dotenv
from quart import Quart, jsonify, request

from utils.database import Database

load_dotenv(".env")


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
        self.app = app
        self._all_guilds = None
        self._all_guilds_time = None
        self.guilds = {}
        self.guild_metrics = {}
        self.guild_history = {}
        self.guild_history_v2 = {}

    async def get_guilds(self):
        if not self._all_guilds or Time().time - self._all_guilds_time > 60:
            r = await self.app.db.get_guilds()
            res = []
            for guild in r:
                if isinstance(guild["time_difference"], datetime.timedelta):
                    guild["time_difference"] = str(guild["time_difference"])
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
                self.guilds[guild["id"]]["time_difference"] = str(
                    self.guilds[guild["id"]]["time_difference"]
                )

                self.guilds[guild["name"]] = guild
                self.guilds[guild["name"]]["get_time"] = Time().time
                self.guilds[guild["name"]]["time_difference"] = str(
                    self.guilds[guild["id"]]["time_difference"]
                )
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
                guild_metric["time_difference"] = str(guild_metric["time_difference"])
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

    async def get_guild_history(self, guild_id):
        guild_history = self.guild_history.get(guild_id, {})

        if not guild_history or Time().time - guild_history.get("get_time", 0) > 60:
            r = await self.app.db.get_guild_history(guild_id)
            if not r:
                return
            all_members_ever_uuid = []
            previous_players = []
            logs = {}
            for i, guild_history in enumerate(r):
                time_difference = str(guild_history["time_difference"])
                # find when a player joined or left the guild
                joined_players = [player for player in guild_history["players"] if
                                  player not in previous_players] if i != 0 else []
                left_players = [player for player in previous_players if player not in guild_history["players"]]
                logs[time_difference] = {
                    "joined": joined_players,
                    "left": left_players,
                }
                previous_players = guild_history["players"]
                all_members_ever_uuid.extend(joined_players + left_players)

            all_members_ever = await self.app.db.get_names(all_members_ever_uuid)

            r_list = []

            for date, log in logs.items():
                r_list.append({
                    "date": date,
                    "joined": [all_members_ever.get(uuid, uuid) for uuid in log["joined"]],
                    "left": [all_members_ever.get(uuid, uuid) for uuid in log["left"]],
                })
            r_list = list(reversed(r_list))
            self.guild_history[guild_id] = {
                "d": r_list,
                "get_time": Time().time,
            }
            return r_list

        return self.guild_history[guild_id]["d"]


class App(Quart):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db: Database = None
        self.loop: asyncio.BaseEventLoop = None
        self.session: aiohttp.ClientSession = None
        self.database_cache: DatabaseCache = DatabaseCache(self)
        self.patrons = None
        self.patreon_last_get = 0

    async def get_patreon_members(self):
        async with self.session.get("https://shieldsio-patreon.vercel.app/api/?username=sbhub") as r:
            data = await r.json()
            return data["message"].replace(" patrons", "")


app = App(__name__)


@app.route("/leaderboard")
async def leaderboard():
    r = await app.database_cache.get_guilds()
    return jsonify(r)


@app.route("/stats")
async def stats():
    r = await app.database_cache.get_guilds()
    guilds_tracked = len(r)
    players_tracked = sum([i["members"] for i in r])
    if Time().time - app.patreon_last_get >= 86400:
        app.patrons = await app.get_patreon_members()
        app.patreon_last_get = Time().time

    return jsonify({
        "guilds_tracked": guilds_tracked,
        "players_tracked": players_tracked,
        "patrons": app.patrons,
    })


@app.route("/guild/<guild_id>")
async def guild(guild_id):
    r = await app.database_cache.get_guild(guild_id)
    try:
        r2 = r.copy()
        del r2["get_time"]
    except:
        r2 = r
    return jsonify(r2)


@app.route("/metrics/<guild_id>")
async def metrics(guild_id):
    r = await app.database_cache.get_guild_metrics(guild_id)
    try:
        r2 = r.copy()
        del r2["get_time"]
    except:
        r2 = r

    if request.args.get('format', False):
        guild_name = (await app.db.pool.fetchrow("""
SELECT guild_name AS name FROM guilds WHERE guild_id = $1 LIMIT 1;
        """, guild_id))["name"]

        r3 = {
            "catacombs_data": [],
            "skills_data": [],
            "slayer_data": [],
            "senither_weight_data": [],
            "lily_weight_data": [],
            "member_count_data": []
        }

        for guild_metric in r2:
            r3["catacombs_data"].append([guild_metric['time_difference'], guild_metric["catacombs"]])
            r3["skills_data"].append([guild_metric['time_difference'], guild_metric["skills"]])
            r3["slayer_data"].append([guild_metric['time_difference'], guild_metric["slayer"]])
            r3["senither_weight_data"].append([guild_metric['time_difference'], guild_metric["senither_weight"]])
            r3["member_count_data"].append([guild_metric['time_difference'], guild_metric["member_count"]])
            if guild_metric.get("lily_weight"):
                r3["lily_weight_data"].append([guild_metric['time_difference'], guild_metric["lily_weight"]])

        r4 = {}
        for key, value in r3.items():
            r4[key] = {
                "name": guild_name,
                "data": value
            }
        return jsonify(r4)

    return jsonify(r2)


@app.route("/history/<guild_id>")
async def history(guild_id):
    r = await app.database_cache.get_guild_history(guild_id)
    return jsonify(r)


@app.route("/v2/history")
async def history_v2():
    guild_id = request.args.get("guild_id", None)
    player = request.args.get("uuid", None)
    per_page = int(request.args.get("per_page", 10))
    page = int(request.args.get("page", 1))
    if page < 1:
        page = 1
    if per_page < 1:
        per_page = 10

    r, total_rows = await app.db.get_guild_history_v2(guild_id, player, per_page, page, return_total=True)

    return jsonify({
        "data": r,
        "paginate": {
            "current_page": page,
            "last_page": ceil(total_rows / per_page),
            "per_page": per_page,
            "total": total_rows
        }
    })


@app.route("/autocomplete")
async def autocomplete():
    r = await app.db.get_id_name_autocomplete()
    return jsonify(r)


@app.errorhandler(404)
async def page_not_found(e):
    return jsonify(404), 404


@app.before_serving
async def start_up():
    if not app.loop:
        app.loop = asyncio.get_event_loop()
    if not app.db:
        app.db = await Database(app).open()
    if not app.session:
        app.session = aiohttp.ClientSession(loop=app.loop)
        app.patrons = await app.get_patreon_members()
        app.patreon_last_get = Time().time


@app.after_request
def set_headers(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


if os.name == "nt":
    app.run(host="0.0.0.0", port=8080, use_reloader=False, debug=True)
else:
    app.run(host="0.0.0.0", port=80, debug=True, use_reloader=False)
