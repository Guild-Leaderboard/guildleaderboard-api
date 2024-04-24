import asyncio
import json
import time
from typing import TYPE_CHECKING

import redis.asyncio as redis

if TYPE_CHECKING:
    from database2 import Database2

PWD = "Soj/v1twd66n8xtH1Pd8lMbDrVzYqwn4JdKHfvXtv/aYvK3NjyY0hhSNDgwLldJqj3HIDahmWMT5bKV2"


class Cache:
    def __init__(self, app=None):
        self.app = app
        self.db: Database2 = app.db
        self.rd = redis.Redis(host='195.201.43.165', port=6379, password=PWD, decode_responses=True)

        self.update_times = {
            "guilds": [300, 0],
            "stats": [3600, 0],
            "guild": [240, 0],
            "autocomplete": [600, 0],
            "sitemap": [3600, 0],
        }

    async def jget(self, key):
        return json.loads(await self.rd.get(key) or "null")

    async def update_cache(self):
        while True:
            if time.time() - self.update_times["guilds"][1] >= self.update_times["guilds"][0]:
                await self.rd.set("guilds", json.dumps(self.db.get_guilds(), default=str))
                self.update_times["guilds"][1] = time.time()
                self.app.logger.info("Updated guilds cache")

            if time.time() - self.update_times["stats"][1] >= self.update_times["stats"][0]:
                r = await self.jget("guilds")
                if time.time() - self.app.patreon_last_get >= 3600:
                    self.app.patrons = await self.app.get_patreon_members()
                    self.app.patreon_last_get = time.time()

                await self.rd.set("stats", json.dumps({
                    "guilds_tracked": len(r),
                    "players_tracked": sum([i["members"] for i in r]),
                    "patrons": self.app.patrons,
                    "top_guilds": sorted(r, key=lambda x: x["weighted_stats"].split(",")[6], reverse=True)[:3],
                }, default=str))
                self.update_times["stats"][1] = time.time()
                self.app.logger.info("Updated stats cache")

            if time.time() - self.update_times["guild"][1] >= self.update_times["guild"][0]:
                guild_id_list = []
                for guild in await self.jget("guilds"):
                    await self.rd.set(f'guild_{guild["_id"]}', json.dumps(self.db.get_guild(guild["_id"]), default=str))
                    await self.rd.set(f'guild_{guild["_id"]}_metrics', json.dumps(self.db.get_guild_metrics(guild["_id"]), default=str))
                    guild_id_list.append(guild["_id"])

                # delete guilds that are no longer in the database
                keys = await self.rd.keys("guild_*")
                for key in keys:
                    if key.split("_")[1] not in guild_id_list:
                        await self.rd.delete(key)
                self.update_times["guild"][1] = time.time()
                self.app.logger.info("Updated guild cache")

            if time.time() - self.update_times["autocomplete"][1] >= self.update_times["autocomplete"][0]:
                await self.rd.set("autocomplete", json.dumps(self.db.get_id_name_autocomplete(), default=str))
                self.update_times["autocomplete"][1] = time.time()
                self.app.logger.info("Updated autocomplete cache")

            if time.time() - self.update_times["sitemap"][1] >= self.update_times["sitemap"][0]:
                await self.rd.set("sitemap", json.dumps(self.db.get_sitemap_links(), default=str))
                self.update_times["sitemap"][1] = time.time()
                self.app.logger.info("Updated sitemap cache")

            await asyncio.sleep(5)


if __name__ == "__main__":
    c = Cache("1")
    t = time.time()
    c.rd.set("test", "test")
    print(c.rd.get("test"))
    print(time.time() - t)
