# uuid = "f837856ad9f44e3c86e730e2495e52ed"
# key = "cd721f35-480c-4c93-a993-304a90de3acd"
#
# import asyncio
# import aiohttp
#
#
# async def main():
#     async with aiohttp.ClientSession() as session:
#         async with session.get(
#                 "https://api.hypixel.net/skyblock/profiles",
#                 params={"key": key, "uuid": uuid}
#         ) as resp:
#             player_data = await resp.json()
#             profile = sorted(player_data.get("profiles", []), key=lambda x: x["members"][uuid]["last_save"])[-1]
#         print(profile["cute_name"])
#         async with session.get(
#                 'https://nwapi.guildleaderboard.com/networth', json={
#                     "profileData": profile["members"][uuid],
#                     "bankBalance": profile.get("banking", {}).get("balance", 0),
#                     "options": {
#                         "onlyNetworth": True,
#                     }
#                 },
#                 headers={
#                     "authorization": 'TIMNOOT_IS_AWESOME'
#                 }
#         ) as resp:
#             try:
#                 networth = await resp.json()
#             except:
#                 print(await resp.text())
#         print(networth)
#
#
# asyncio.run(main())

import requests


r = requests.get("https://skykings.net/leaderboard/data/guilds?sort_by=skyblock_xp&sort_order=desc")
rj = r.json()

skykings_dict = {}

for g in rj:
    skykings_dict[g["name"]] = g["skyblock_xp"]

r = requests.get("https://apiv2.guildleaderboard.com/leaderboard")
rj = r.json()

guildleaderboard_dict = {}

for g in rj:
    guildleaderboard_dict[g["guild_name"]] = g["weighted_stats"].split(",")[-1]


for skykings_guild in skykings_dict:
    if skykings_guild not in guildleaderboard_dict:
        print(skykings_guild, skykings_dict[skykings_guild], "Not in guildleaderboard")