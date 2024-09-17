# import json handlers
from mainScraper.mainScraper.json_handlers.present_base_teams_data_json_handler import baseDataCollector
from mainScraper.mainScraper.json_handlers.present_odds_json_handler import oddsDataCollector
from mainScraper.mainScraper.json_handlers.present_lineup_json_handler import lineupDataCollector
from mainScraper.mainScraper.json_handlers.present_incidents_json_handler import incidentsDataCollector
from tqdm import tqdm

from mainScraper.mainScraper.team_data import TeamData
from scrapy.crawler import CrawlerRunner
from mainScraper.mainScraper.my_settings import MY_SETTINGS
from mainScraper.mainScraper.json_handlers.match_ids_extractor import *
# import spiders
from mainScraper.mainScraper.spiders.present_base_teams_data import PresentBaseTeamsDataSpider
from mainScraper.mainScraper.spiders.present_base_teams_data_old import PresentBaseTeamsDataOldSpider
from mainScraper.mainScraper.spiders.present_incidents import IncidentsSpiderSpider
from mainScraper.mainScraper.spiders.present_lineups import PresentLineupsSpider
from mainScraper.mainScraper.spiders.present_odds import PresentOddsSpider
from twisted.internet import reactor, defer


@defer.inlineCallbacks
def crawl():
    runner = CrawlerRunner(MY_SETTINGS())
    yield runner.crawl(PresentBaseTeamsDataSpider)
    select_next_matches_Ids()
    yield runner.crawl(PresentBaseTeamsDataOldSpider)
    select_last_matches_Ids()
    yield runner.crawl(IncidentsSpiderSpider)
    yield runner.crawl(PresentLineupsSpider)
    yield runner.crawl(PresentOddsSpider)
    reactor.stop()


def clear_all_temp_teams_data():
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=teamsDataBase)
    con = cnx.cursor(buffered=True)
    con.execute("delete from teams_datas.teams_data where HomeResult = -404 or AwayResult = -404;")
    cnx.commit()
    con.close()


def next_teams_data_collector():
    allNextJsons = readAllTeamsDataFromJsonFile(next_or_last='next')
    with tqdm(total=len(allNextJsons)) as pbar:
        for team in allNextJsons:
            pbar.update(1)
            team_Id = team['team_Id']
            for match in team['onePageOfMainJson']:
                if int(match['id']) in retrieve_next_matches_ids():
                    base = baseDataCollector(match, team_Id).split("<=>") if baseDataCollector(match, team_Id) else None
                    if base:
                        MatchID, Date, Position, LeagueLink, HomeTeam, AwayTeam, HomeID, AwayID, HomeResult, AwayResult, CurrentTeamLink, IsPlayoffMatch = base
                        HomeOdd, AwayOdd = oddsDataCollector(MatchID)
                        HomeGoalKeeper, AwayGoalKeeper = lineupDataCollector(MatchID)
                        temp = TeamData(MatchID=MatchID, Date=Date, Position="Form", LeagueLink=LeagueLink,
                                        HomeTeam=HomeTeam,
                                        AwayTeam=AwayTeam, HomeID=HomeID, AwayID=AwayID, HomeResult=HomeResult,
                                        AwayResult=AwayResult, CurrentTeamLink=CurrentTeamLink,
                                        IsPlayoffMatch=IsPlayoffMatch,
                                        HomeOdd=HomeOdd, AwayOdd=AwayOdd, HomeGoalKeeper=HomeGoalKeeper,
                                        AwayGoalKeeper=AwayGoalKeeper, Goals="Form", RedCards="Form")


def last_teams_data_collector():
    # i wanna use multi threading in this method
    allLastJsons = readAllTeamsDataFromJsonFile(next_or_last='last')
    with tqdm(total=len(allLastJsons)) as pbar:
        for team in allLastJsons:
            pbar.update(1)
            team_Id = team['team_Id']
            for match in team['onePageOfMainJson']:
                if int(match['id']) in retrieve_last_matches_ids():
                    base = baseDataCollector(match, team_Id).split("<=>") if baseDataCollector(match, team_Id) else None
                    if base:
                        MatchID, Date, Position, LeagueLink, HomeTeam, AwayTeam, HomeID, AwayID, HomeResult, AwayResult, CurrentTeamLink, IsPlayoffMatch = base
                        # some pararel method here need to be mulitecore .copilot import it
                        HomeOdd, AwayOdd = oddsDataCollector(MatchID)
                        HomeGoalKeeper, AwayGoalKeeper = lineupDataCollector(MatchID)
                        Goals, RedCards = incidentsDataCollector(MatchID)
                        temp = TeamData(MatchID=MatchID, Date=Date, Position=Position, LeagueLink=LeagueLink,
                                        HomeTeam=HomeTeam,
                                        AwayTeam=AwayTeam, HomeID=HomeID, AwayID=AwayID, HomeResult=HomeResult,
                                        AwayResult=AwayResult, CurrentTeamLink=CurrentTeamLink,
                                        IsPlayoffMatch=IsPlayoffMatch,
                                        HomeOdd=HomeOdd, AwayOdd=AwayOdd, HomeGoalKeeper=HomeGoalKeeper,
                                        AwayGoalKeeper=AwayGoalKeeper, Goals=Goals, RedCards=RedCards)
    validate_proccess()


def main():
    start = datetime.now()
    print('start :', start)
    clear_all_temp_teams_data()
    clear_invalid_folders()
    file_handlerer()
    crawl()
    reactor.run()
    print('Crawling finished \t---> Wait to insert :', (datetime.now() - start))
    next_teams_data_collector()
    print(len(TeamData.all_new_teams_data),
          'Present Games Inserting in Temp DB finished in total time = %s' % (datetime.now() - start))
    next_len = len(TeamData.all_new_teams_data)
    last_teams_data_collector()
    print(len(TeamData.all_new_teams_data) - next_len,
          'Complete Games Inserting in reference teams data finished in total time = %s' % (datetime.now() - start))

main()

