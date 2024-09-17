import simplejson as json
from ..var import *


def get_incidents_json_file(match_Id):
    if not os.path.exists(incidents_path + "/match%s.json" % match_Id):
        return None
    with open(incidents_path + "/match%s.json" % match_Id, 'r') as f:
        mainjson = json.load(f)
    return mainjson


def getIncidents(mainjson):
    redCards = []
    'HR70:Home(A:Away) Goal Regular(P:penalty,O:ownGoal) in time = 70'
    selectedIncidents = []
    missed = 0
    for incident in mainjson['incidents'][::-1]:
        timer = "time" in incident
        key = ""
        if incident["incidentType"] == "period" and incident['time'] == 90 and incident[
            'text'] == "FT": break  # end of match
        extraTime = 0
        if "incidentClass" in incident:
            if incident["incidentClass"] == "missed":  # missed penalti
                if "inGamePenalty" != incident["incidentType"]:
                    print(incident["incidentType"], "\ttthis is missed type if need ?", incident['incidentType'], mainjson['matchId'])
                if incident['isHome'] == True:
                    key += "HM"
                else:
                    key += "AM"
                if 'addedTime' in incident:
                    extraTime = incident['addedTime']
                    key += "E"
                else:
                    key += "N"
                if timer:
                    if incident['time'] == -1: continue
                    key += (str(incident['time'] + extraTime)) if timer else "*0"
                selectedIncidents.append(key)
                missed += 1
            if incident['incidentType'] == 'goal' or incident['incidentClass'] == 'scored':  # goals
                if incident['isHome'] == True:
                    key += "H"
                else:
                    key += "A"
                if incident['incidentClass'] == 'regular':
                    key += "R"
                elif incident['incidentClass'] == 'penalty':
                    key += "P"
                elif incident['incidentClass'] == 'ownGoal':
                    key += "O"
                if incident['incidentType'] == "penaltyShootout":
                    key += "P"
                if 'addedTime' in incident:
                    extraTime = incident['addedTime']
                    key += "E"
                else:
                    key += "N"
                if timer:
                    if incident['time'] == -1: continue
                    key += (str(incident['time'] + extraTime)) if timer else "*0"
                selectedIncidents.append(key)
            elif incident['incidentType'] == 'card' and (
                    incident['incidentClass'] == "red" or incident['incidentClass'] == "yellowRed"):  # red cards
                if incident['isHome'] == True:
                    key += "H"
                else:
                    key += "A"
                if 'addedTime' in incident:
                    extraTime = incident['addedTime']
                    key += "E"
                else:
                    key += "N"
                if timer:
                    if incident['time'] == -1: continue
                key += (str(incident['time'] + extraTime)) if timer else "*0"
                redCards.append(key)
    redCards = "%s:" % len(redCards) + "<>".join(redCards)
    selectedIncidents = "%s:" % (len(selectedIncidents) - missed) + "<>".join(selectedIncidents)
    return selectedIncidents, redCards


def incidentsDataCollector(match_Id):
    mainjson = get_incidents_json_file(match_Id)
    if mainjson is None:
        return None, None
    incidents, redCards = getIncidents(mainjson)
    return incidents, redCards
