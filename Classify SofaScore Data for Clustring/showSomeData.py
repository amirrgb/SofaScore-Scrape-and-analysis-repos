import numpy as np
import matplotlib.pyplot as plt
import time
from datetime import datetime
import pandas as pd
import mysql.connector
from tqdm import tqdm
import multiprocessing
import asyncio


def str_to_int(s):
    #replace all : with Y     #replace all '<>' with Z #remove all * part (after 90') from string
    s = s.replace(":","Y").replace("<>","Z")
    replaced = ""
    for i in s.split('Z'):
        if "*" not in i:
            replaced += i + "Z"
    s = replaced
    s = int(s,36)
    return s

def int_to_str(i):
    string = np.base_repr(i, 36)
    return string

def convertDateToTimestamp(date:str):
    return int(time.mktime(datetime.strptime(date, '%Y-%m-%d').timetuple()))

def readAllMainObjects():
    cnx = mysql.connector.connect(user='root', password='kian1381',host='localhost',database='teams_datas')
    cursor = cnx.cursor()
    query = ("SELECT * FROM main_database.references;")
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    cnx.close()
    print("length of data is: ",len(data))
    return data

def getDetailsOfPreviousMatch(match):
    # match = Date, Position, LeagueLink, HomeTeam, AwayTeam, HomeID, AwayID, HomeResult, AwayResult, HomeOdd, AwayOdd, Goals, RedCards, Result
    match = match.split('<=>')
    date = convertDateToTimestamp(match[0])
    home_id = int(match[5])
    away_id = int(match[6])
    home_result = int(match[7]) 
    away_result = int(match[8])
    home_odd = float(match[9]) if match[9] != "null" else 0
    away_odd = float(match[10]) if match[10] != "null" else 0
    goals = str_to_int(match[11])
    red_cards = str_to_int(match[12])
    result = 1 if match[13] == 'Win' else 0 if match[13] == 'Draw' else -1
    return [date,home_id,away_id,home_result,away_result,home_odd,away_odd,goals,red_cards,result]

async def convertMainObjectToListForNumpy(main_object):
    #main_object = Date, Position, LeagueLink, HomeTeam, AwayTeam, HomeID, AwayID, HomeResult, AwayResult, HomeOdd, AwayOdd, HomePreGameScore, AwayPreGameScore, Goals, RedCards, EH1, EH2, EH3, RH1, RH2, SH1, SH2, SH3, SH4, EA1, EA2, EA3, RA1, RA2, SA1, SA2, SA3, SA4, IsFlowChangedDueCupMatch, IsOver18DaysDelayInFlow, IsGoalKeeperChangedInEMatches, IsPlayoffMatch
    timestamp = convertDateToTimestamp(main_object[0])
    home_id = int(main_object[5])
    away_id = int(main_object[6])
    home_result = int(main_object[7])
    away_result = int(main_object[8])
    home_odd = float(main_object[9]) if main_object[9] is not None else 0
    away_odd = float(main_object[10]) if main_object[10] is not None else 0
    home_pre_game_score = int(main_object[11])
    away_pre_game_score = int(main_object[12])
    goals = str_to_int(main_object[13]) if main_object[13] is not None else 0
    red_cards = str_to_int(main_object[14]) if main_object[14] is not None else 0
    list_for_numpy = [timestamp,home_id,away_id,home_result,away_result,home_odd,away_odd,home_pre_game_score,away_pre_game_score,goals,red_cards]  
    eh1 = getDetailsOfPreviousMatch(main_object[15])
    eh2 = getDetailsOfPreviousMatch(main_object[16])
    eh3 = getDetailsOfPreviousMatch(main_object[17])
    rh1 = getDetailsOfPreviousMatch(main_object[18])
    rh2 = getDetailsOfPreviousMatch(main_object[19])
    sh1 = getDetailsOfPreviousMatch(main_object[20])
    sh2 = getDetailsOfPreviousMatch(main_object[21])
    sh3 = getDetailsOfPreviousMatch(main_object[22])
    sh4 = getDetailsOfPreviousMatch(main_object[23])
    ea1 = getDetailsOfPreviousMatch(main_object[24])
    ea2 = getDetailsOfPreviousMatch(main_object[25])
    ea3 = getDetailsOfPreviousMatch(main_object[26])
    ra1 = getDetailsOfPreviousMatch(main_object[27])
    ra2 = getDetailsOfPreviousMatch(main_object[28])
    sa1 = getDetailsOfPreviousMatch(main_object[29])
    sa2 = getDetailsOfPreviousMatch(main_object[30])
    sa3 = getDetailsOfPreviousMatch(main_object[31])
    sa4 = getDetailsOfPreviousMatch(main_object[32])
    is_flow_changed_due_cup_match = 1 if main_object[33] == 'True' else 0
    is_over_18_days_delay_in_flow = 1 if main_object[34] == 'True' else 0
    is_goal_keeper_changed_in_e_matches = 1 if main_object[35] == 'True' else 0
    is_playoff_match = 1 if main_object[36] == 'True' else 0
    for prevoius_match in [eh1,eh2,eh3,rh1,rh2,sh1,sh2,sh3,sh4,ea1,ea2,ea3,ra1,ra2,sa1,sa2,sa3,sa4]:
        list_for_numpy.extend(prevoius_match)
    list_for_numpy.extend([is_flow_changed_due_cup_match,is_over_18_days_delay_in_flow,is_goal_keeper_changed_in_e_matches,is_playoff_match])
    makeNumpyArrayFromList(list_for_numpy)

def makeNumpyArrayFromList(list_for_numpy):
    global np_arrays
    numpy_array = np.array(list_for_numpy)
    np_arrays.append(numpy_array)

def convertDataToPandas(data):
    df = pd.DataFrame(data)
    print(df)
    return df

def showInGraph(dates,goals):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.title("daily goals")
    plt.xlabel("days")
    plt.ylabel("goals")
    ax.scatter(dates,goals)
    plt.show()

async def main():
    global np_arrays
    start = datetime.now()
    print("Loading DB .... ")
    data = readAllMainObjects()
    np_arrays = []
    print("DB loaded waiting for numpy ....")
    with tqdm(total=len(data)) as pbar:
        for object in data:
            pbar.update(1)
            await convertMainObjectToListForNumpy(object)
    print("Numpy is ready")
    df = pd.DataFrame(np_arrays)
    print(df)
    print("total time: ",datetime.now()-start)

asyncio.run(main())

