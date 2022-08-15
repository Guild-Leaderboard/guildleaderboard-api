import asyncio
import datetime
import os
import re

from dotenv import load_dotenv
from quart import Quart, jsonify, request

from utils.database import Database

load_dotenv(".env")


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

    async def get_guilds(self):
        if not self._all_guilds or Time().time - self._all_guilds_time > 60:
            self._all_guilds = await self.app.db.get_guilds()
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
                    return {"error": "Guild not found"}

                guild["players"] = await app.db.get_players(guild["players"], conn)
                guild["discord"] = await app.db.get_guild_discord(guild["guild_id"], conn)

                self.guilds[guild["guild_id"]] = guild
                self.guilds[guild["guild_id"]]["get_time"] = Time().time
                self.guilds[guild["guild_id"]]["time_difference"] = str(
                    self.guilds[guild["guild_id"]]["time_difference"]
                )

                self.guilds[guild["guild_name"]] = guild
                self.guilds[guild["guild_name"]]["get_time"] = Time().time
                self.guilds[guild["guild_name"]]["time_difference"] = str(
                    self.guilds[guild["guild_id"]]["time_difference"]
                )
                for player in guild["players"]:
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
        self.database_cache: DatabaseCache = DatabaseCache(self)


app = App(__name__)


@app.route("/leaderboard")
async def leaderboard():
    r = await app.database_cache.get_guilds()
    res = []
    for guild in r:
        if isinstance(guild["time_difference"], datetime.timedelta):
            guild["time_difference"] = str(guild["time_difference"])

        res.append(guild)
    return jsonify(res)


@app.route("/guild/<guild_id>")
async def guilds(guild_id):
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
SELECT guild_name FROM guilds WHERE guild_id = $1 LIMIT 1;
        """, guild_id))["guild_name"]
        r3 = {
            "average_catacombs_data": [],
            "average_skills_data": [],
            "average_slayer_data": [],
            "average_weight_data": [],
            "member_count_data": []
        }
        for guild_metric in r2:
            r3["average_catacombs_data"].append([guild_metric['time_difference'], guild_metric["average_catacombs"]])
            r3["average_skills_data"].append([guild_metric['time_difference'], guild_metric["average_skills"]])
            r3["average_slayer_data"].append([guild_metric['time_difference'], guild_metric["average_slayer"]])
            r3["average_weight_data"].append([guild_metric['time_difference'], guild_metric["average_weight"]])
            r3["member_count_data"].append([guild_metric['time_difference'], guild_metric["member_count"]])
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
    try:
        r2 = r.copy()
        del r2["get_time"]
    except:
        r2 = r
    return jsonify(r2)


@app.route("/autocomplete")
async def autocomplete():
    r = await app.db.get_id_name_autocomplete()
    return jsonify(r)


@app.route("/stats")
async def stats():
    r = await app.database_cache.get_guilds()
    guilds_tracked = len(r)
    players_tracked = sum([i["players"] for i in r])

    return jsonify({
        "guilds_tracked": guilds_tracked,
        "players_tracked": players_tracked
    })


@app.errorhandler(404)
async def page_not_found(e):
    return jsonify(404), 404


@app.before_serving
async def start_up():
    if not app.loop:
        app.loop = asyncio.get_event_loop()
    if not app.db:
        app.db = await Database(app).open()


@app.after_request
def set_headers(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


if os.name == "nt":
    app.run(host="0.0.0.0", port=8080, use_reloader=False, debug=True)
else:
    app.run(host="0.0.0.0", port=80, debug=True, use_reloader=False)
