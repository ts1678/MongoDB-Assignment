import pymongo
import yaml
import json

client = pymongo.MongoClient("localhost" , 27017)

db = client["tim_data"]

# there's probably a better way to do this
db.team_collection.drop()
db.results_collection.drop()

col = db["team_collection"]
res = db["results_collection"]

unique_teams = []

with open("example_tim_data.json") as f:
    data = json.load(f)
    with open("example_tim_data.yaml", "r") as y:
        yaml_data = yaml.load(y, yaml.Loader)
        for i in data:
            for j in i:
                if type(i[j]).__name__ != yaml_data[j]:
                    raise ValueError
    col.insert_many(data)

for doc in col.find():
    if not doc['team_num'] in unique_teams:
        unique_teams.append(doc['team_num'])

for team in unique_teams:
    this_match_count = 0 # these variables will all be incorrect until their final operations are done at the end
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