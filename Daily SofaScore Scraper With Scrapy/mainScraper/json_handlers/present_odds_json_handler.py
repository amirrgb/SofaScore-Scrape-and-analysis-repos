from ..var import odds_path
import simplejson as json
import os
def calculateOddToDecimal(odds):
    try:
        parts = odds.split('/')
        if len(parts) == 1:
            return "null"
        return str((float(parts[0]) / float(parts[1])) + 1)[:5]
    except:
        return "null"


def getOdds(mainjson):
    homeTeam, awayTeam = "null", "null"
    for Odds in mainjson['featured']['choices']:
        if 'name' in Odds:
            if Odds['name'] == '1':
                homeTeam = calculateOddToDecimal(Odds['fractionalValue'])
            if Odds['name'] == '2':
                awayTeam = calculateOddToDecimal(Odds['fractionalValue'])
            # if Odds['name'] == 'X':
            #     draw =calculateOddToDecimal(Odds['fractionalValue'])

    # output="<=>".join(list(map(str,[homeTeam, draw, awayTeam])))
    if homeTeam == "null" or awayTeam == "null":
        return "null", "null"
    return homeTeam, awayTeam

def get_odd_json_file(match_Id):
    if not os.path.exists(odds_path+"/match%s.json"%match_Id):
        return None
    with open(odds_path+"/match%s.json"%match_Id, 'r') as f:
        mainjson = json.load(f)
    return mainjson

def oddsDataCollector(match_Id):
    if get_odd_json_file(match_Id) is None:
        return None,None
    homeOdd,awayOdd = getOdds(get_odd_json_file(match_Id))
    return homeOdd,awayOdd