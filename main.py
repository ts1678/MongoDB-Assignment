import pymongo
import yaml
import json
from warnings import warn

client = pymongo.MongoClient("localhost" , 27017)

db = client["tim_data"]

db.team_collection.drop()
db.results_collection.drop()

col = db["team_collection"]
res = db["results_collection"]

with open("example_tim_data.json", "r") as tim_data_file:
    data = json.load(tim_data_file)
    with open("example_tim_data.yaml", "r") as tim_yaml_file:
        yaml_data = yaml.load(tim_yaml_file, yaml.Loader)
        for tim_element in data:
            for key in tim_element:
                if key not in yaml_data:
                    warn("The Key from the TIM data is not in the YAML!")
                    continue
                if type(tim_element[key]).__name__ != yaml_data[key]:
                    raise ValueError
    col.insert_many(data)


team_results = {}

for team_match in col.find():
    if team_match["team_num"] not in team_results:
        team_results[team_match["team_num"]]["matches_played"] = 0
        team_results[team_match["team_num"]]["balls_scored"] = 0
        team_results[team_match["team_num"]]["least_balls_scored"] = team_match["team_num"]["balls_scored"]
        team_results[team_match["team_num"]]["most_balls_scored"] = team_match["team_num"]["balls_scored"]
        team_results[team_match["team_num"]]["successful_climbs"] = 0
    team_results[team_match["team_num"]]["matches_played"] += 1
    team_results[team_match["team_num"]]["balls_scored"] += team_match["balls_scored"]
    team_results[team_match["team_num"]]["least_balls_scored"] = min(team_results[team_match["team_num"]]["least_balls_scored"], team_match["team_num"]["balls_scored"])
    team_results[team_match["team_num"]]["most_balls_scored"] = max(team_results[team_match["team_num"]]["most_balls_scored"], team_match["team_num"]["balls_scored"])
    team_results[team_match["team_num"]]["successful_climbs"] += team_match["team_num"]["climbed"]

for team in team_results:
    res.insert_one({"team_number:": team,
                    "average_balls_scored": team["balls_scored"] / team["matches_played"],
                    "least_balls_scored": team["least_balls_scored"],
                    "most_balls_scored": team["most_balls_scored"],
                    "percent_climb_success": team["successful_climbs"] / team["matches"]
                    })




"""
unique_teams = []

for doc in col.find():
    if not doc['team_num'] in unique_teams:
        unique_teams.append(doc['team_num'])

for team in unique_teams:
    this_match_count = 0
    most_balls_scored = 0
    least_balls_scored = 0
    average_balls_scored = 0
    percent_climb_success = 0
    for i in col.find({'team_num': team}):
        this_match_count += 1
        percent_climb_success += i["climbed"]
        if i["num_balls"] > most_balls_scored:
            most_balls_scored = i["num_balls"]
        if i["num_balls"] < least_balls_scored or least_balls_scored == 0:
            least_balls_scored = i["num_balls"]
        average_balls_scored += i["num_balls"]
    res.insert_one({"team_number": team,
        "average_balls_scored": average_balls_scored/this_match_count,
        "least_balls_scored": least_balls_scored,
        "most_balls_scored": most_balls_scored,
        "number_of_matches_played": this_match_count,
        "percent_climb_success": percent_climb_success/this_match_count})
"""

