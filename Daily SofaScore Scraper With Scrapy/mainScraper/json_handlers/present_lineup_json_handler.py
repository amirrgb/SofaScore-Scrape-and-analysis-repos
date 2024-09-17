import json
from ..var import *


def getGoalKeeperPlayerIds(mainjson2):
    mainjson = mainjson2['lineups']
    keys = []
    for key in ['home', 'away']:
        if key in mainjson:
            if 'players' in mainjson[key]:
                for player in mainjson[key]['players']:
                    if len(keys) == 2:
                        return keys
                    if 'position' in player['player']:
                        if player['player']['position'] == 'G':
                            if 'statistics' in player:
                                if player['statistics'] == {} and player['substitute'] == False:
                                    keys.append(str(player['player']['id']) + "<>" + player['player']['name'])
                                elif player['substitute'] == False and 'minutesPlayed' in player['statistics']:
                                    if player['statistics']['minutesPlayed'] == 90:
                                        keys.append(str(player['player']['id']) + "<>" + player['player']['name'])
                                    else:
                                        keys.append(str(player['player']['id']) + "<>" + player['player'][
                                            'name'] + "<>substitute")
                                elif player['substitute'] == True and 'minutesPlayed' in player['statistics']:
                                    if player['statistics']['minutesPlayed'] > 75:
                                        keys.append(str(player['player']['id']) + "<>" + player['player'][
                                            'name'] + "<>substitute")
                            elif player['substitute'] == False:
                                keys.append(str(player['player']['id']) + "<>" + player['player']['name'])
                            if len(keys) == 1 and key == "home":
                                break
    return None, None


def get_lineup_json_file(match_Id):
    if not os.path.exists(lineups_path + "/match%s.json" % match_Id):
        return None
    with open(lineups_path + "/match%s.json" % match_Id, 'r') as f:
        mainjson = json.load(f)
    return mainjson


def lineupDataCollector(match_Id):
    mainjson = get_lineup_json_file(match_Id)
    if mainjson is None:
        return None, None
    homeGoalKeeper, awayGoalKeeper = getGoalKeeperPlayerIds(mainjson)
    return homeGoalKeeper, awayGoalKeeper
