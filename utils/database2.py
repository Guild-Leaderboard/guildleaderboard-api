import time
from math import *
from operator import itemgetter

from pymongo import MongoClient


def weight_multiplier(members):
    frequency = sin(members / (125 / 0.927296)) + 0.2
    return members / 125 + (1 - members / 125) * frequency


"""
guild_information {
    "_id": str, # guild id index
    "discord: str
}

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
     
    metrics: [ # latest metrics first
        {
            capture_date TIMESTAMP, index
            players TEXT[],
            multiplier REAL,            
            weighted_stats str # comma separated list of stats
                    {
                        catacombs float,
                        skills float,
                        slayer int,
                        senither_weight int,
                        lily_weight int,
                        networth int,
                        sb_experience int 
                    }
        },
        ...
    ],
    positions str # comma separated list of positions 
                {
                    catacombs float,
                    skills float,
                    slayer int,
                    senither_weight int,
                    lily_weight int,
                    networth int,
                    sb_experience int    
                }    
}
guilds.players is an aray of uuids


"""


class Database2:
    def __init__(self, app=None):
        self.client = MongoClient('195.201.43.165', 27017, username='root', password='R8xC7rdEE8')
        self.db = self.client.guildleaderboard
        self.app = app

        self.guild_information = self.db.guild_information
        self.guild_information.create_index([("_id", 1)])

        self.history = self.db.history
        self.history.create_index([("_id", 1)])

        self.guilds = self.db.guilds
        self.guilds.create_index([("_id", 1)])

    def get_guild_discord(self, guild_id: str):
        r = self.guild_information.find_one({"_id": guild_id})
        return r["discord"] if r else None

    def upsert_guild_info(self, guild_id: str, discord_invite: str):
        self.guild_information.update_one(
            {"_id": guild_id},
            {"$set": {"discord": discord_invite}},
            upsert=True
        )

    def get_history(self, guild_id=None, player=None, per_page=10, page=1, return_total=False):
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

        r_vals = [list(r), self.history.count_documents(query)] if return_total else list(r)
        return r_vals

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

        return self.guilds.find_one(query, {"metrics": {"$slice": 1}})

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

    def sort_metrics(self):
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
        # # self.sort_metrics()
        # import time
        t = time.time()
        e = self.get_guild_player_from_guilds("4316ec4fd9ea465f84a30ad6be769ecd")
        print(time.time() - t)
        print(e)


# import json
# self.guilds.delete_many({})
#
# with open("guilds_table.json", "r") as f:
#     guilds_table = json.load(f)
#
# guilds = {}
# for guild in guilds_table:
#     guilds[guild["guild_id"]] = guilds.get(guild["guild_id"], {
#         "_id": guild["guild_id"], "guild_name": guild["guild_name"],
#         "position_change": guild["position_change"], "metrics": [], "positions": "0,0,0,0,0,0,0"
#     })
#     multiplier = weight_multiplier(len(guild["players"]))
#     guilds[guild["guild_id"]]["metrics"].append({
#         "capture_date": guild["capture_date"],
#         "players": guild["players"],
#         "multiplier": round(multiplier, 2),
#         "weighted_stats": f"{round(guild['senither_weight'] * multiplier)},{round(guild['skills'], 2)},{round(guild['catacombs'], 2)},{round(guild['slayer'])},{round(guild['lily_weight'] * multiplier)},{round(guild['networth'])},{round(guild['sb_experience'] * multiplier, 1)}"
#     })
#
# guilds_table = list(guilds.values())
# self.guilds.insert_many(guilds_table)


# with open("history_table.json", "r") as f:
#     history_table = json.load(f)
#     self.history.insert_many(history_table)

if __name__ == "__main__":
    db = Database2()
    db.main()
    # db.sort_metrics()
    # db.update_positions()
    # print(db.get_guild_discord("5ff092f18ea8c9724b8e55f7"))
