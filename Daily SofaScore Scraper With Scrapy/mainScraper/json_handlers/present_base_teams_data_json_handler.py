import pytz
from datetime import datetime,timedelta
import mysql.connector

user, password, host, teamsDataBase, teamsLinkDataBase = 'root', 'kian1381', 'localhost', "teams_datas", "updated_teams_links"


def getCurrentTeamLink(team_Id):
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=teamsLinkDataBase)
    con = cnx.cursor(buffered=True)
    sql = f'SELECT TeamLink FROM updated_teams_links.teams_link where TeamLink like "%/{team_Id}";'
    con.execute(sql)
    teamLink = con.fetchone()
    if teamLink is not None:
        return teamLink[0]


# data collecting  from  json file
def getMatchId(data):
    if 'id' in data:
        id = data['id']
        return str(id)
    else:
        return "null"


def leagueOfTeam(data):
    if 'tournament' in data:
        if 'category' in data['tournament']:
            if 'slug' in data['tournament']['category']:
                leagueLink: str = "https://www.sofascore.com/tournament/football/" + data['tournament']['category'][
                    'slug'] + "/"
        if 'uniqueTournament' in data['tournament']:
            if 'slug' in data['tournament']['uniqueTournament']:
                leagueLink += data['tournament']['uniqueTournament']['slug'] + "/"
            if 'id' in data['tournament']['uniqueTournament']:
                leagueLink += str(data['tournament']['uniqueTournament']['id'])
        else:
            leagueLink = leagueLink.replace("tournament", "standings")
            if 'slug' in data['tournament']:
                leagueLink += data['tournament']['slug'] + "/"
            if 'id' in data['tournament']:
                leagueLink += str(data['tournament']['id'])
        return leagueLink
    return "null"


def isPlayoff(data):
    stringOfTournament = str(data['tournament']).lower() + str(data['roundInfo']).lower() if "roundInfo" in data else ""
    for key in ["semifinal", "Quarterfinals", "Playoffs", "Promotion", "Relagation", "final", "Qualification"]:
        if key.lower() in stringOfTournament:
            return True
    return False


def dateIsValid(item):
    try:
        date = datetime.strptime(item[:10], '%Y-%m-%d')
        begin = datetime.strptime("2018-06-01", "%Y-%m-%d")
        end = datetime.now() + timedelta(days=3)
        if end >= date >= begin:
            return True
    except Exception as e:
        print(e)
    return False


def getCoverage(data):
    if 'coverage' in data:
        return str(data['coverage'])
    return "0"


def positionOfTeam(data):
    if "isAwarded" in data:
        return "Awarded"
    if 'status' in data:
        if 'description' in data['status']:
            if data['status']['description'] == 'Ended':
                return "FT"
            elif data['status']['description'] == 'AP':
                return "AP"
            elif data['status']['description'] == 'AET':
                return "AET"
            elif data['status']['description'] == 'Postponed':
                return "Postponed"
            elif data['status']['description'] == "Coverage canceled":
                return "FT"
            elif data['status']['description'] == 'Canceled':
                return "Canceled"
            elif data['status']['description'] == 'Suspended':
                return "Suspended"
            elif data['status']['description'] == 'Interrupted':
                return "Interrupted"
            elif data['status']['description'] == 'Abandoned':
                return "Abandoned"
            elif data['status']['description'] == 'Extra Time':
                return "ET"
            elif data['status']['description'] == 'Removed':
                return "Removed"
            elif data['status']['description'] == 'Walkover':
                return "Walkover"
            elif data['status']['description'] == 'Retired':
                return "Retired"
            elif data['status']['description'] == 'Start delayed':
                return "FT"
            elif data['status']['description'] == 'Halftime':
                return "HT"
            elif data['status']['description'] == '1st half':
                return "HT"
            elif data['status']['description'] == '2nd half':
                return "FT"
            else:
                position = data['status']['description']
        else:
            position = data['status']
    else:
        position = "null"
    return position


def timeOfTeam(data):
    if 'startTimestamp' in data:
        timeStamp = data['startTimestamp']
        return str(datetime.strftime(datetime.fromtimestamp(timeStamp, tz=pytz.timezone('Asia/Tehran')), '%Y-%m-%d'))
    else:
        print("its different time : %s" % data['time'])
        return "null"


def idOfTeams(data):
    if 'homeTeam' in data:
        if 'id' in data['homeTeam']:
            homeId = data['homeTeam']['id']
    if 'awayTeam' in data:
        if 'id' in data['awayTeam']:
            awayId = data['awayTeam']['id']
    return str(homeId), str(awayId)


# score collecting functions
def extraScores(data):
    extra1Check = 'extra1' in data['homeScore'] and 'extra1' in data['awayScore']
    extra2Check = 'extra2' in data['homeScore'] and 'extra2' in data['awayScore']
    overtimeCheck = 'overtime' in data['homeScore'] and 'overtime' in data['awayScore']
    homeOver, awayOver, homeExtra1, awayExtra1, homeExtra2, awayExtra2 = 0, 0, 0, 0, 0, 0
    if overtimeCheck:
        homeOver, awayOver = int(data['homeScore']['overtime']), int(data['awayScore']['overtime'])
    if extra1Check:
        homeExtra1, awayExtra1 = int(data['homeScore']['extra1']), int(data['awayScore']['extra1'])
    if extra2Check:
        homeExtra2, awayExtra2 = int(data['homeScore']['extra2']), int(data['awayScore']['extra2'])
    return max(homeOver, homeExtra1 + homeExtra2), max(awayOver, awayExtra1 + awayExtra2)


def periodScoreGetter(data):
    period2Check = 'period2' in data['homeScore'] and 'period2' in data['awayScore']
    period1Check = 'period1' in data['homeScore'] and 'period1' in data['awayScore']
    if period2Check and period1Check:
        homeTeamScore = int(data['homeScore']['period1']) + int(data['homeScore']['period2'])
        awayTeamScore = int(data['awayScore']['period1']) + int(data['awayScore']['period2'])
    else:
        homeTeamScore = "null"
        awayTeamScore = "null"
    return homeTeamScore, awayTeamScore


def currentScoreGetter(data):
    currentCheck = 'current' in data['homeScore'] and 'current' in data['awayScore']
    overtimeCheck = 'overtime' in data['homeScore'] and 'overtime' in data['awayScore']
    if overtimeCheck:
        if int(data['homeScore']["overtime"]) == -1 or int(data['awayScore']["overtime"]) == -1:
            homeTeamScore = int(data['homeScore']['normaltime'])
            awayTeamScore = int(data['awayScore']['normaltime'])
            return homeTeamScore, awayTeamScore
    if currentCheck:
        homeTeamScore = int(data['homeScore']['current'])
        awayTeamScore = int(data['awayScore']['current'])
        homeExtraScore, awayExtraScore = extraScores(data)
        if 'penalties' in data['homeScore']:
            homeTeamScore -= int(data['homeScore']['penalties'])
        if 'penalties' in data['awayScore']:
            awayTeamScore -= int(data['awayScore']['penalties'])
        homeTeamScore -= homeExtraScore
        awayTeamScore -= awayExtraScore
    else:
        homeTeamScore = "null"
        awayTeamScore = "null"
    return homeTeamScore, awayTeamScore


def isPeriodWrong(data):
    period1Check = 'period1' in data['homeScore'] and 'period1' in data['awayScore']
    period2Check = 'period2' in data['homeScore'] and 'period2' in data['awayScore']
    normalTiemeCheck = 'normaltime' in data['homeScore'] and 'normaltime' in data['awayScore']
    if period1Check and period2Check and normalTiemeCheck:
        if int(data['homeScore']['period1']) + int(data['homeScore']['period2']) < int(
                data['homeScore']['normaltime']) or int(data['awayScore']['period1']) + int(
            data['awayScore']['period2']) < int(data['awayScore']['normaltime']):
            return True
    return False


def isDisplayWrong(data):
    displayCheck = 'display' in data['homeScore'] and 'display' in data['awayScore']
    normalTimeCheck = 'normaltime' in data['homeScore'] and 'normaltime' in data['awayScore']
    if displayCheck and normalTimeCheck:
        if int(data['homeScore']['display']) < int(data['homeScore']['normaltime']) or int(
                data['awayScore']['display']) < int(data['awayScore']['normaltime']):
            return True
    else:
        return True
    return False


def isDisplayCorrect(data):
    displayCheck = 'display' in data['homeScore'] and 'display' in data['awayScore']
    normalTimeCheck = 'normaltime' in data['homeScore'] and 'normaltime' in data['awayScore']
    if displayCheck and normalTimeCheck:
        if int(data['homeScore']['display']) == int(data['homeScore']['normaltime']) and int(
                data['awayScore']['display']) == int(data['awayScore']['normaltime']):
            return True
    return False


def isPeriodCorrect(data):
    period2Check = 'period2' in data['homeScore'] and 'period2' in data['awayScore']
    period1Check = 'period1' in data['homeScore'] and 'period1' in data['awayScore']
    if period2Check and period1Check:
        if int(data['homeScore']['period1']) + int(data['homeScore']['period2']) == int(
                data['homeScore']['normaltime']) and int(data['awayScore']['period1']) + int(
            data['awayScore']['period2']) == int(data['awayScore']['normaltime']):
            return True
    return False


def isCurrentWrong(data):
    currentCheck = 'current' in data['homeScore'] and 'current' in data['awayScore']
    normalTimeCheck = 'normaltime' in data['homeScore'] and 'normaltime' in data['awayScore']
    if currentCheck and normalTimeCheck:
        if int(currentScoreGetter(data)[0]) < int(data['homeScore']['normaltime']) or int(
                currentScoreGetter(data)[1]) < int(data['awayScore']['normaltime']):
            return True
    else:
        return True
    return False


def isCurrentCorrect(data):
    currentCheck = 'current' in data['homeScore'] and 'current' in data['awayScore']
    normalTimeCheck = 'normaltime' in data['homeScore'] and 'normaltime' in data['awayScore']
    if currentCheck and normalTimeCheck:
        if int(currentScoreGetter(data)[0]) == int(data['homeScore']['normaltime']) and int(
                currentScoreGetter(data)[1]) == int(data['awayScore']['normaltime']):
            return True
    return False


def scoreOfTeam(data, position, coverage):
    way = []
    if position == 'Awarded' or position == 'Canceled' or position == 'Postponed' or position == 'Suspended' or position == 'Interrupted' or position == 'Abandoned' or position == 'Removed' or position == 'Walkover' or position == 'Retired':
        return "null", "null"
    way.append("1")
    normalTimeCheck = 'normaltime' in data['homeScore'] and 'normaltime' in data['awayScore']
    displayCheck = 'display' in data['homeScore'] and 'display' in data['awayScore']
    overtimeCheck = 'overtime' in data['homeScore'] and 'overtime' in data['awayScore']
    homeTeamScore = "null"
    awayTeamScore = "null"
    if 'homeScore' in data and 'awayScore' in data:
        way.append("2")
        if normalTimeCheck:
            way.append("3")
            if isDisplayCorrect(data):
                way.append("4")
                homeTeamScore = int(data['homeScore']['display'])
                awayTeamScore = int(data['awayScore']['display'])
            elif isCurrentCorrect(data):
                way.append("5")
                homeTeamScore, awayTeamScore = currentScoreGetter(data)
            else:
                way.append("6")
                if str(coverage) == '1':
                    way.append("7")
                    if isPeriodCorrect(data):
                        homeTeamScorec, awayTeamScorec = currentScoreGetter(data)
                        if homeTeamScorec == int(data['homeScore']['display']) and awayTeamScorec == int(
                                data['awayScore']['display']) and position == "FT":
                            way.append("19")
                            homeTeamScore, awayTeamScore = currentScoreGetter(data)
                        else:
                            way.append("8")
                            homeTeamScore, awayTeamScore = periodScoreGetter(data)
                    else:
                        way.append("9")
                        homeTeamScore = int(data['homeScore']['normaltime'])
                        awayTeamScore = int(data['awayScore']['normaltime'])
                else:
                    way.append("10")
                    if isPeriodCorrect(data) and (position == 'AP' or position == 'AET'):
                        way.append("11")
                        homeTeamScore, awayTeamScore = periodScoreGetter(data)
                    elif not isDisplayWrong(data):
                        if position == "AET" or position == "AP":
                            if int(data['homeScore']['normaltime']) == int(data['awayScore']['normaltime']):
                                way.append("16")
                                homeTeamScore = int(data['homeScore']['normaltime'])
                                awayTeamScore = int(data['awayScore']['normaltime'])
                            elif int(data['homeScore']['display']) == int(data['awayScore']['display']):
                                way.append("12")
                                homeTeamScore = int(data['homeScore']['display'])
                                awayTeamScore = int(data['awayScore']['display'])
                            else:
                                way.append("17")
                                homeTeamScore, awayTeamScore = currentScoreGetter(data)
                        else:
                            way.append("18")
                            homeTeamScore = int(data['homeScore']['display'])
                            awayTeamScore = int(data['awayScore']['display'])
                    elif not isCurrentWrong(data):
                        way.append("13")
                        homeTeamScore, awayTeamScore = currentScoreGetter(data)
                    elif isPeriodCorrect(data):
                        way.append("14")
                        homeTeamScore = int(data['homeScore']['display'])
                        awayTeamScore = int(data['awayScore']['display'])
                    else:
                        way.append("15")
                        homeTeamScore = int(data['homeScore']['normaltime'])
                        awayTeamScore = int(data['awayScore']['normaltime'])
        elif displayCheck:
            way.append("16")
            homeTeamScore = int(data['homeScore']['display'])
            awayTeamScore = int(data['awayScore']['display'])
        else:
            way.append("17")
            homeTeamScore, awayTeamScore = currentScoreGetter(data)
    return str(homeTeamScore), str(awayTeamScore)


# data collecting ...


def baseDataCollector(match,teamId):
    currentTeamLink = getCurrentTeamLink(teamId)
    leagueLink = leagueOfTeam(match)
    homeTeam = match['homeTeam']['name']
    awayTeam = match['awayTeam']['name']
    coverage = getCoverage(match)
    date = timeOfTeam(match)
    position = positionOfTeam(match)
    homeTeamScore, awayTeamScore = scoreOfTeam(match, position, coverage)
    if homeTeamScore == "null" or awayTeamScore == "null":
        position = "Canceled"
    matchId = getMatchId(match)
    homeId, awayId = idOfTeams(match)
    listt = list(map(str, [matchId, date, position, leagueLink, homeTeam, awayTeam, homeId, awayId, homeTeamScore,
                           awayTeamScore, currentTeamLink]))
    output = '<=>'.join(listt)
    output += "<=>" + str(isPlayoff(match))
    if not dateIsValid(date):
        return None
    if position == 'HT':
        return None
    if date == "1970-01-01" or date == "null" or position == "null" or leagueLink == "null" or homeTeam == "null" or awayTeam == "null":
        print("problem : ", output)
    output = str(output).replace("  ", " ").replace("b'", "", 1).replace('b"', '', 1).replace('"', "").replace("'", "")
    return output
