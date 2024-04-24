from math import *
from operator import itemgetter

from pymongo import MongoClient


def weight_multiplier(members):
    frequency = sin(members / (125 / 0.927296)) + 0.2
    return members / 125 + (1 - members / 125) * frequency


"""
history {
    _id str, index
    
    type str,
    uuid str,
    name str,
    capture_date datetime,
    guild_id str,
    guild_name str
}

guilds {
    _id str, # guild id index
    guild_name str,
    position_change int,
    discord str,   
     
    metrics: [ # latest metrics first
        {
            capture_date TIMESTAMP, index
            players TEXT[],
            multiplier REAL,            
            weighted_stats str # comma separated list of stats
                    {
                        senither_weight int,
                        skills float,                    
                        catacombs float,
                        slayer int,
                        lily_weight int,
                        networth int,
                        sb_experience int
                    }
        },
        ...
    ],
    positions str # comma separated list of positions 
                {
                    senither_weight int,
                    skills float,                    
                    catacombs float,
                    slayer int,
                    lily_weight int,
                    networth int,
                    sb_experience int  
                }    
}
guilds.players is an aray of uuids

players {
    _id str, index # uuid
    name str,
    
    latest_senither int,
    latest_lily int,
    latest_nw int,
    latest_sb_xp int,
    latest_slayer int,
    latest_cata int,
    latest_asl float,
    latest_capture_date TIMESTAMP,
    

    
    metrics: [ # latest metrics first
        {
            capture_date TIMESTAMP, index
            
            general_stats str # comma separated list of stats
                    {
                        senither_weight REAL,
                        lily_weight REAL,
                        networth BIGINT,
                        sb_experience BIGINT,
                    }
            slayer_stats str # comma separated list of stats
                    {
                        total_slayer REAL,
                        zombie_xp BIGINT,
                        spider_xp BIGINT,
                        wolf_xp BIGINT,
                        enderman_xp BIGINT,
                        blaze_xp BIGINT,
                        vampire_xp BIGINT,
                    }
            dungeon_stats str # comma separated list of stats
                    {
                        catacombs_xp BIGINT,
                        healer_xp BIGINT,
                        mage_xp BIGINT,
                        berserk_xp BIGINT,
                        archer_xp BIGINT,
                        tank_xp BIGINT,
                    }
            skill_stats str # comma separated list of stats
                    {
                        average_skill REAL,
                        taming_xp BIGINT,
                        mining_xp BIGINT,
                        farming_xp BIGINT,
                        combat_xp BIGINT,
                        foraging_xp BIGINT,
                        fishing_xp BIGINT,
                        enchanting_xp BIGINT,
                        alchemy_xp BIGINT,
                        carpentry_xp BIGINT
                    }
        },
        ...
    ]

"""


class Database2:
    def __init__(self, app=None):
        self.client = MongoClient('195.201.43.165', 27017, username='root', password='R8xC7rdEE8')
        self.db = self.client.guildleaderboard
        self.app = app

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

    """
    History
    """

    def get_history(self, guild_id=None, player=None, per_page=10, page=1):
        # remove _id from query and remove
        query = {}
        if guild_id:
            query["guild_id"] = guild_id
            # only query for type, uuid, name, capture_date
            filter_1 = {"type": 1, "uuid": 1, "name": 1, "capture_date": 1, "_id": 0}
        else:  # player
            query["uuid"] = player
            # only query for type, guild_id, guild_name, capture_date
            filter_1 = {"type": 1, "guild_id": 1, "guild_name": 1, "capture_date": 1, "_id": 0}

        r = self.history.find(query, filter_1).sort("capture_date", -1).skip((page - 1) * per_page).limit(per_page)

        return list(r), self.history.count_documents(query)


    def get_id_name_autocomplete(self):
        r = self.guilds.find({}, {"_id": 1, "guild_name": 1})
        return [{"id": row["_id"], "name": row["guild_name"]} for row in r]

    """
    Guilds
    """

    def get_guilds(self):
        r = self.guilds.aggregate([
            {"$unwind": "$metrics"},
            {"$group": {
                "_id": "$_id",
                "guild_name": {"$first": "$guild_name"},
                "position_change": {"$first": "$position_change"},
                "positions": {"$first": "$positions"},

                "weighted_stats": {"$first": "$metrics.weighted_stats"},
                "members": {"$first": {"$size": "$metrics.players"}},
                "capture_date": {"$first": "$metrics.capture_date"}
            }},
        ])
        return list(r)


    def get_guild(self, guild_id=None, guild_name=None):
        query = {}
        if guild_id:
            query["_id"] = guild_id
        else:
            query["guild_name"] = {"$regex": f"^{guild_name}$", "$options": "i"}

        # Add player information for each guild member to the guild document from the player database

        r = self.guilds.find_one(query, {"metrics": {"$slice": 1}})
        if not r:
            return None

        query = {
            "_id": {"$in": r["metrics"][0]["players"]}
        }
        r["metrics"][0]["players"] = list(self.players.find(query, {"name": 1, "_id": 1, "metrics": {"$slice": 1}}))
        return r

    def get_guild_metrics(self, guild_id):
        return list(self.guilds.aggregate([
            {"$match": {"_id": guild_id}},
            {"$unwind": "$metrics"},
            {"$project": {
                "_id": 0,
                "weighted_stats": "$metrics.weighted_stats",
                "member_count": {"$size": "$metrics.players"},
                "capture_date": "$metrics.capture_date"
            }}
        ]))

    def get_guild_player_from_guilds(self, uuid: str):
        r = self.guilds.aggregate([
            {"$unwind": "$metrics"},
            {"$group": {
                "_id": "$_id",
                "guild_name": {"$first": "$guild_name"},
                "capture_date": {"$first": "$metrics.capture_date"},
                "players": {"$first": "$metrics.players"}
            }},
            {"$match": {"players": uuid}},

            # remove players and _id from the result
            {"$project": {"players": 0, "_id": 0}}
        ])
        return list(r)[0] if r else None

    """
    Players
    """

    def get_player(self, uuid=None, name=None):
        # Get the player and query the guild from the history collection
        query = {}
        if uuid:
            query["_id"] = uuid
        else:
            query["name"] = {"$regex": f"^{name}$", "$options": "i"}

        r = self.players.find_one(query, {"metrics": {"$slice": 1}})
        if not r:
            return None

        query = {
            "uuid": r['_id'],
            "type": "1"
        }
        r1 = self.history.find_one(
            query, {"guild_id": 1, "guild_name": 1, "capture_date": 1, "_id": 0}, sort=[("capture_date", -1)]
        )

        # Make sure there is not a leave record in the history at the same day or later
        # capture_date is a string like "2022-07-16 13:24:55.054404"
        if r1 and not self.history.find_one({
            "uuid": r['_id'],
            "type": "0",
            "guild_id": r1["guild_id"],
            "capture_date": {"$gte": r1["capture_date"]}
        }):
            r["guild_id"] = r1["guild_id"]
            r["guild_name"] = r1["guild_name"]

        return r

    def get_player_metrics(self, uuid):
        return self.players.find_one({"_id": uuid}, {"metrics": 1})

    def get_player_page(self, sort_by: str, reverse: bool = False, page=1, username: str = None):
        if sort_by not in [
            "latest_senither", "latest_lily", "latest_nw", "latest_sb_xp", "latest_slayer", "latest_cata", "latest_asl"
        ]:
            raise ValueError("Invalid sort_by value")

        offset = (page - 1) * 25
        query = {}
        if username:
            query["name"] = {"$regex": f"^{username}", "$options": "i"}
            total = self.players.count_documents(query)
        else:
            total = self.players.estimated_document_count()

        return list(self.players.find(query, {
            "name": 1,
            "latest_sb_xp": 1,
            "latest_nw": 1,
            "latest_senither": 1,
            "latest_asl": 1,
            "latest_slayer": 1,
            "latest_cata": 1,
        }).sort([(sort_by, 1 if reverse else -1)]).skip(offset).limit(25)), total

    """
    Other
    """

    def get_sitemap_links(self):
        return {
            "guilds": [
                row["guild_name"] for row in self.guilds.find({}, {"_id": 0, "guild_name": 1})
            ],
            "players": [
                row["name"] for row in self.players.find({}, {"_id": 0, "name": 1})
            ]
        }

    # Other

    def sort_guild_metrics(self):
        """
Fixes the metrics array in the guilds collection by sorting the array by capture_date if it somehow got unsorted.
Latest should be first
        """
        # Get all guild documents
        guilds = self.guilds.find()

        for guild in guilds:
            # Sort the metrics array by capture_date
            sorted_metrics = sorted(guild['metrics'], key=itemgetter('capture_date'), reverse=True)

            # Update the guild document with the sorted metrics array
            self.guilds.update_one(
                {'_id': guild['_id']},
                {'$set': {'metrics': sorted_metrics}}
            )

    def sort_player_metrics(self):
        """
Fixes the metrics array in the player collection by sorting the array by capture_date if it somehow got unsorted.
Latest should be first
        """
        # Get all guild documents
        players = self.players.find()

        for player in players:
            # Sort the metrics array by capture_date
            sorted_metrics = sorted(player['metrics'], key=itemgetter('capture_date'), reverse=True)

            # Update the guild document with the sorted metrics array
            self.players.update_one(
                {'_id': player['_id']},
                {'$set': {'metrics': sorted_metrics}}
            )

    def sort_guilds(self, key):
        guilds = self.guilds.aggregate([
            {"$unwind": "$metrics"},
            {"$group": {
                "_id": "$_id",
                "weighted_stats": {"$first": "$metrics.weighted_stats"},
            }},
        ])  # [{'_id': '5e264d398ea8c9feb3f0bdd6', 'weighted_stats': '15745,47.81,40.66,5540951,16819,19771424325,28944.6'}, ...]

        sorted_guilds = sorted(guilds, key=lambda x: x["weighted_stats"].split(",")[key], reverse=True)
        return [guild["_id"] for guild in sorted_guilds]

    def update_positions(self):
        """
        Updates the positions field in the guilds collection with the latest metrics
        """
        guilds = self.guilds.aggregate([
            {"$unwind": "$metrics"},
            {"$group": {
                "_id": "$_id",
                "weighted_stats": {"$first": "$metrics.weighted_stats"},
            }},
        ])

        sorted_guilds_dict = {}
        for key in range(len([
            "catacombs", "skills", "slayer", "senither_weight", "lily_weight", "networth", "sb_experience"
        ])):
            sorted_guilds_dict[key] = self.sort_guilds(key)

        for i in guilds:
            _id, weighted_stats = i["_id"], i["weighted_stats"]

            # Calculate the position of the guild for each stat
            positions = []
            for key in range(len([
                "catacombs", "skills", "slayer", "senither_weight", "lily_weight", "networth", "sb_experience"
            ])):
                position = sorted_guilds_dict[key].index(_id) + 1
                positions.append(position)

            # Update the guild document with the new positions
            self.guilds.update_one(
                {'_id': _id},
                {'$set': {'positions': ','.join(map(str, positions))}}
            )

    def main(self):
        pass
        # self.sort_player_metrics()
        # self.sort_guild_metrics()

# # Guild import
# import json
# import datetime
# self.guilds.delete_many({})
#
# with open("guilds_table.json", "r") as f:
#     guilds_table = json.load(f)
#
# guilds = {}
# for guild in guilds_table:
#     guilds[guild["guild_id"]] = guilds.get(guild["guild_id"], {
#         "_id": guild["guild_id"], "guild_name": guild["guild_name"],
#         "position_change": guild["position_change"], "metrics": [], "positions": "0,0,0,0,0,0,0", "discord": ""
#     })
#     multiplier = weight_multiplier(len(guild["players"]))
#     guilds[guild["guild_id"]]["metrics"].append({
#         "capture_date": datetime.datetime.fromisoformat(guild["capture_date"]),
#         "players": guild["players"],
#         "multiplier": round(multiplier, 2),
#         "weighted_stats": f"{round(guild['senither_weight'] * multiplier)},{round(guild['skills'], 2)},{round(guild['catacombs'], 2)},{round(guild['slayer'])},{round(guild['lily_weight'] * multiplier)},{round(guild['networth'])},{round(guild['sb_experience'] * multiplier)}"
#     })
#
# self.guilds.insert_many(list(guilds.values()))


# # Player import
# import json
# player_metrics = []
# for i in range(0, 10):
#     with open(f"player_metrics_{i}.json", "r") as f:
#         player_metrics += json.load(f)
#
# players = {}
# for player in player_metrics:
#     players[player["uuid"]] = players.get(player["uuid"], {
#         "_id": player["uuid"], "name": player["name"], "metrics": []
#     })
#     total_slayer = round(player["zombie_xp"] + player["spider_xp"] + player["wolf_xp"] + player["enderman_xp"] + player["blaze_xp"])
#     try:
#         players[player["uuid"]]["metrics"].append({
#             "capture_date": player["capture_date"],
#             "general_stats": f"{round(player['senither_weight'])},{round(player['lily_weight'])},{round(player['networth'])},{round(player['sb_experience'])}",
#             "slayer_stats": f"{total_slayer},{round(player['zombie_xp'])},{round(player['spider_xp'])},{round(player['wolf_xp'])},{round(player['enderman_xp'])},{round(player['blaze_xp'])},{round(player.get('vampire_xp', 0))}",
#             "dungeon_stats": f"{round(player['catacombs_xp'])},{round(player['healer_xp'])},{round(player['mage_xp'])},{round(player['berserk_xp'])},{round(player['archer_xp'])},{round(player['tank_xp'])}",
#             "skill_stats": f"{round(player['average_skill'], 2)},{round(player['taming_xp'])},{round(player['mining_xp'])},{round(player['farming_xp'])},{round(player['combat_xp'])},{round(player['foraging_xp'])},{round(player['fishing_xp'])},{round(player['enchanting_xp'])},{round(player['alchemy_xp'])},{round(player['carpentry_xp'])}"
#         })
#     except KeyError as e:
#         print(player)
#         raise e
# print("Sorting")
# # Sort metrics on capture_date
# for player in players.values():
#     player["metrics"] = sorted(player["metrics"], key=itemgetter("capture_date"), reverse=True)
# print("updating latest info")
# # Get the latest metrics
# for player in players.values():
#     player["latest_senither"] = round(int(player["metrics"][0]["general_stats"].split(",")[0]))
#     player["latest_lily"] = round(int(player["metrics"][0]["general_stats"].split(",")[1]))
#     player["latest_nw"] = round(int(player["metrics"][0]["general_stats"].split(",")[2]))
#     player["latest_sb_xp"] = round(int(player["metrics"][0]["general_stats"].split(",")[3]), 1)
#     player["latest_slayer"] = round(int(player["metrics"][0]["slayer_stats"].split(",")[0]))
#     player["latest_cata"] = round(int(player["metrics"][0]["dungeon_stats"].split(",")[0]))
#     player["latest_asl"] = round(float(player["metrics"][0]["skill_stats"].split(",")[0]), 2)
#     player["latest_capture_date"] = player["metrics"][0]["capture_date"]
#
#
#
# print("Inserting")
# players_table = list(players.values())
# self.players.insert_many(players_table)


## History Import
# guilds_table = list(guilds.values())
# self.guilds.insert_many(guilds_table)


# with open("history_table.json", "r") as f:
#     history_table = json.load(f)
#     self.history.insert_many(history_table)

if __name__ == "__main__":
    db = Database2()
    db.main()
    # db.sort_guild_metrics()
    # db.update_positions()
    # print(db.get_guild_discord("5ff092f18ea8c9724b8e55f7"))
