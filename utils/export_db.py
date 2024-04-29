from __future__ import annotations

import asyncio
from operator import itemgetter

import asyncpg
from pymongo import MongoClient

DB_IP = "75.119.153.245"
DB_USER = "postgres"
DB_PWD = "0uUrWeWM96u17zUvO"


class Database:
    pool: asyncpg.pool.Pool = None

    def __init__(self):
        self.str_keys = ['time_difference', 'capture_date']
        self.cached_guilds = {}

        self.client = MongoClient('195.201.43.165', 27017, username='root', password='R8xC7rdEE8')
        self.db = self.client.guildleaderboard

        self.history = self.db.history
        self.history.create_index([("_id", 1)])
        self.history.create_index([("uuid", 1)])
        self.history.create_index([("guild_id", 1)])

        self.guilds = self.db.guilds
        self.guilds.create_index([("_id", 1)])

        self.players = self.db.players
        self.players.create_index([("_id", 1)])
        self.players.create_index([("name", 1)])

        self.players.create_index([("latest_senither", -1)])
        self.players.create_index([("latest_nw", -1)])
        self.players.create_index([("latest_sb_xp", -1)])
        self.players.create_index([("latest_slayer", -1)])
        self.players.create_index([("latest_cata", -1)])
        self.players.create_index([("latest_asl", -1)])

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
        import time

        Database.pool = await self.get_pool()
        # Get a list of all the player uuids
        # players = await Database.pool.fetch("SELECT uuid FROM players;", timeout=60)
        # Select only unique uuids from the player_metrics table
        self.players.delete_many({})
        players = await Database.pool.fetch("SELECT DISTINCT uuid FROM player_metrics;", timeout=60)

        remaining_players = [row['uuid'] for row in players]
        count = 0
        saved_players = []
        t = time.time()
        # loop through remaining players in chunks of 100
        for i in range(0, len(remaining_players), 100):
            uuids = remaining_players[i:i + 100]
            players_data = {}
            # Get the latest record for each player
            records = await Database.pool.fetch(
                "SELECT * FROM player_metrics WHERE uuid = ANY($1) ORDER BY capture_date DESC;", uuids, timeout=60)

            for metric in records:
                if metric["uuid"] not in players_data:
                    players_data[metric["uuid"]] = {
                        "_id": metric["uuid"], "name": metric["name"], "metrics": []
                    }
                total_slayer = round(
                    metric["zombie_xp"] + metric["spider_xp"] + metric["wolf_xp"] + metric["enderman_xp"]
                    + metric["blaze_xp"]
                )
                players_data[metric["uuid"]]["metrics"].append({
                    "capture_date": metric["capture_date"],
                    "general_stats": f"{round(metric['senither_weight'])},{round(metric['lily_weight'])},{round(metric['networth'])},{round(metric['sb_experience'])}",
                    "slayer_stats": f"{total_slayer},{round(metric['zombie_xp'])},{round(metric['spider_xp'])},{round(metric['wolf_xp'])},{round(metric['enderman_xp'])},{round(metric['blaze_xp'])},{round(metric.get('vampire_xp', 0))}",
                    "dungeon_stats": f"{round(metric['catacombs_xp'])},{round(metric['healer_xp'])},{round(metric['mage_xp'])},{round(metric['berserk_xp'])},{round(metric['archer_xp'])},{round(metric['tank_xp'])}",
                    "skill_stats": f"{round(metric['average_skill'], 2)},{round(metric['taming_xp'])},{round(metric['mining_xp'])},{round(metric['farming_xp'])},{round(metric['combat_xp'])},{round(metric['foraging_xp'])},{round(metric['fishing_xp'])},{round(metric['enchanting_xp'])},{round(metric['alchemy_xp'])},{round(metric['carpentry_xp'])}"
                })


            for record in players_data.values():
                sorted_metrics = sorted(record["metrics"], key=itemgetter("capture_date"), reverse=True)

                player_data = {
                    "_id": record["_id"], "name": record["name"], "metrics": sorted_metrics,
                    "latest_senither": round(int(sorted_metrics[0]["general_stats"].split(",")[0])),
                    "latest_lily": round(int(sorted_metrics[0]["general_stats"].split(",")[1])),
                    "latest_nw": round(int(sorted_metrics[0]["general_stats"].split(",")[2])),
                    "latest_sb_xp": round(int(sorted_metrics[0]["general_stats"].split(",")[3]), 1),
                    "latest_slayer": round(int(sorted_metrics[0]["slayer_stats"].split(",")[0])),
                    "latest_cata": round(int(sorted_metrics[0]["dungeon_stats"].split(",")[0])),
                    "latest_asl": round(float(sorted_metrics[0]["skill_stats"].split(",")[0]), 2),
                    "latest_capture_date": sorted_metrics[0]["capture_date"]
                }
                saved_players.append(player_data)
                count += 1
                print(f"{count}/{len(players)}")

            self.players.insert_many(saved_players)
            print("Inserted")
            print(time.time() - t)
            t = time.time()
            saved_players = []

        # for uuid in players:
        #     if len(saved_players) >= 100:
        #         self.players.insert_many(saved_players)
        #         print("Inserted")
        #         print(time.time() - t)
        #         t = time.time()
        #
        #         saved_players = []
        #
        #     # Get the latest record for each player
        #     record = await Database.pool.fetch(
        #         "SELECT * FROM player_metrics WHERE uuid = $1 ORDER BY capture_date DESC;", uuid, timeout=60)
        #
        #     if record is None:
        #         print(uuid, "was null")
        #         continue
        #     new_metrics = []
        #     for metric in record:
        #         total_slayer = round(
        #             metric["zombie_xp"] + metric["spider_xp"] + metric["wolf_xp"] + metric["enderman_xp"]
        #             + metric["blaze_xp"]
        #         )
        #         # print(metric["capture_date"])
        #         new_metrics.append({
        #             "capture_date": metric["capture_date"],
        #             "general_stats": f"{round(metric['senither_weight'])},{round(metric['lily_weight'])},{round(metric['networth'])},{round(metric['sb_experience'])}",
        #             "slayer_stats": f"{total_slayer},{round(metric['zombie_xp'])},{round(metric['spider_xp'])},{round(metric['wolf_xp'])},{round(metric['enderman_xp'])},{round(metric['blaze_xp'])},{round(metric.get('vampire_xp', 0))}",
        #             "dungeon_stats": f"{round(metric['catacombs_xp'])},{round(metric['healer_xp'])},{round(metric['mage_xp'])},{round(metric['berserk_xp'])},{round(metric['archer_xp'])},{round(metric['tank_xp'])}",
        #             "skill_stats": f"{round(metric['average_skill'], 2)},{round(metric['taming_xp'])},{round(metric['mining_xp'])},{round(metric['farming_xp'])},{round(metric['combat_xp'])},{round(metric['foraging_xp'])},{round(metric['fishing_xp'])},{round(metric['enchanting_xp'])},{round(metric['alchemy_xp'])},{round(metric['carpentry_xp'])}"
        #         })
        #
        #     sorted_metrics = sorted(new_metrics, key=itemgetter("capture_date"), reverse=True)
        #
        #     player_data = {
        #         "_id": uuid, "name": record[0]["name"], "metrics": sorted_metrics,
        #         "latest_senither": round(int(sorted_metrics[0]["general_stats"].split(",")[0])),
        #         "latest_lily": round(int(sorted_metrics[0]["general_stats"].split(",")[1])),
        #         "latest_nw": round(int(sorted_metrics[0]["general_stats"].split(",")[2])),
        #         "latest_sb_xp": round(int(sorted_metrics[0]["general_stats"].split(",")[3]), 1),
        #         "latest_slayer": round(int(sorted_metrics[0]["slayer_stats"].split(",")[0])),
        #         "latest_cata": round(int(sorted_metrics[0]["dungeon_stats"].split(",")[0])),
        #         "latest_asl": round(float(sorted_metrics[0]["skill_stats"].split(",")[0]), 2),
        #         "latest_capture_date": sorted_metrics[0]["capture_date"]
        #     }
        #
        #     saved_players.append(player_data)
        #
        #     count += 1
        #     print(f"{count}/{len(players)}")

        # write history table to json
        # with open("history_table.json", "w") as f:
        #     table = await Database.pool.fetch("SELECT * FROM history;", timeout=60)
        #     table = [self.format_json(row) for row in table]
        #
        #     json.dump(table, f)
        #
        # with open("guilds_table.json", "w") as f:
        #     table = await Database.pool.fetch("SELECT * FROM guilds;", timeout=60)
        #     table = [self.format_json(row) for row in table]
        #
        #     json.dump(table, f)

        # for i in range(0, 10):
        #     with open(f"player_metrics_{i}.json", "w") as f:
        #         # player_metrics doesn't have a id column so we can't use the modulo operator its around 1.5m rows
        #         table = await Database.pool.fetch(f"SELECT * FROM player_metrics LIMIT 150000 OFFSET {i * 150000};", timeout=60)
        #
        #
        #         table = [self.format_json(row) for row in table]
        #         json.dump(table, f)

        return self

    async def close(self):
        await Database.pool.close()
        return self


db = Database()
asyncio.run(db.open())
