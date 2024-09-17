import simplejson as json
import mysql.connector
from mainScraper.mainScraper.var import *
from datetime import datetime, timedelta
import pytz

user, password, host = 'root', 'kian1381', 'localhost'
teamsDataBase = "teams_datas"
from tqdm import tqdm
global id_array


def readAllStoredTeamsDataFromDB():
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=teamsDataBase)
    con = cnx.cursor(buffered=True)
    sql = 'SELECT DISTINCT MatchID  FROM teams_datas.teams_data where ((MatchID > 9000000) and not(HomeResult = -404));'
    con.execute(sql)
    stored_match_ids = con.fetchall()
    stored_match_ids = [int(match_id[0]) for match_id in stored_match_ids]
    stored_match_ids = list(set(stored_match_ids))
    return stored_match_ids

stored_match_ids = readAllStoredTeamsDataFromDB()
import numpy as np
id_array = np.array(stored_match_ids, dtype=np.int32, copy=False)


def readAllTeamsDataFromJsonFile(next_or_last='last'):
    teams_data = []
    path = last_teams_path if next_or_last == 'last' else next_teams_path
    for file in os.listdir(path):
        with open(f'{path}/{file}', 'r') as f:
            teams_data.append(json.load(f))
    return teams_data


def timeOfTeam(data):
    if 'startTimestamp' in data:
        timeStamp = float(data['startTimestamp'])
        return datetime.fromtimestamp(timeStamp, tz=pytz.timezone('Asia/Tehran'))
    else:
        print("its different time : %s" % data['startTimestamp'])
        return None


def get_next_match_ids(jsonfile):
    matches_ids = []
    supported_date = datetime.now(tz=pytz.timezone('Asia/Tehran')) + timedelta(days=3)
    begin = datetime(2023,1,1, tzinfo=pytz.timezone('Asia/Tehran'))
    for match in jsonfile['onePageOfMainJson']:
            try:
                if begin < timeOfTeam(match) < supported_date:
                    matches_ids.append(int(match['id']))
            except:
                print("i cant compare them",timeOfTeam(match),supported_date)
    return matches_ids


def get_last_match_ids(jsonfile):
    matches_ids = []
    supported_date = datetime.now(tz=pytz.timezone('Asia/Tehran')) + timedelta(hours=10)
    begin = datetime(2023 , 1 , 1, tzinfo=pytz.timezone('Asia/Tehran'))
    for match in jsonfile['onePageOfMainJson']:
        try:
            if begin < timeOfTeam(match) < supported_date:
                if match['id'] not in id_array:
                    matches_ids.append(int(match['id']))
        except:
            print("i cant compare them", timeOfTeam(match), supported_date)
    matches_ids = list(set(matches_ids))
    return matches_ids


def insert_last_match_id_to_text_file(match_id):
    with open(last_match_ids_path, 'a') as f:
        f.write(f'{match_id}\n')


def insert_next_match_id_to_text_file(match_id):
    with open(next_match_ids_path, 'a') as f:
        f.write(f'{match_id}\n')


def select_next_matches_Ids():
    print("__________selecting_next_matchesIDs__________")
    teams_data = readAllTeamsDataFromJsonFile('next')
    new_matches_id = []
    with tqdm(total=len(teams_data + new_matches_id)) as pbar:
        for team_data in teams_data:
            new_matches_id.extend(get_next_match_ids(team_data))
            pbar.update(1)
        new_matches_id = list(set(new_matches_id))
        for match_id in new_matches_id:
            insert_next_match_id_to_text_file(match_id)
            pbar.update(1)


def last_collect_next_matches_ids():
    try:
        datas_folder = main_path
        datas_folder_last_folder = os.listdir(datas_folder)[-2] + "/"
        next_match_ids_of_last_collect_path = os.path.join(datas_folder, datas_folder_last_folder, 'next_match_ids.txt')
        with open(next_match_ids_of_last_collect_path, 'r+') as f:
            next_match_ids = f.readlines()
        return [int(match_id) for match_id in next_match_ids]
    except Exception as e:
        print(datetime.now(), 'nothing in last_collect_next_matches_ids', e, file=open(error_path, 'a'))
        return None


def select_last_matches_Ids():
    print("__________selecting_last_matchesIDs__________")
    last_collect_next_matches_ids_list = last_collect_next_matches_ids()
    if last_collect_next_matches_ids_list :
        for match_id in last_collect_next_matches_ids_list:
            insert_last_match_id_to_text_file(match_id)
        return
    stored_match_ids = readAllStoredTeamsDataFromDB()
    teams_data = readAllTeamsDataFromJsonFile('last')
    new_matches_id = []
    for team_data in teams_data:
        new_matches_id += get_last_match_ids(team_data)
    new_matches_id = list(set(new_matches_id))
    with tqdm(total=len(new_matches_id)) as pbar:
        for match_id in new_matches_id:
            if match_id not in stored_match_ids:
                insert_last_match_id_to_text_file(match_id)
            pbar.update(1)


def retrieve_matches_ids():
    return retrieve_last_matches_ids() + retrieve_next_matches_ids()


def retrieve_next_matches_ids():
    with open(next_match_ids_path, 'r+') as f:
        matches_ids = f.readlines()
    return [int(match_id) for match_id in matches_ids]


def retrieve_last_matches_ids():
    with open(last_match_ids_path, 'r+') as f:
        matches_ids = f.readlines()
    return [int(match_id) for match_id in matches_ids]


def loadAndSave(source, url):
    try:
        if '"code":404,"message":"Not Found"' in source:
            return None
        mainJson = json.loads(str(source).encode('utf-8'))
        return mainJson
    except Exception as e:
        print(datetime.now(), 'error in loadJsonFile', url, e, file=open(error_path, 'a'))
