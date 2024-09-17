from .var import *
import simplejson as json


def saveInOddsFolder(item):
    try:
        match_Id = item['matchId']
        with open(f'{odds_path}/match{match_Id}.json', 'w') as f:
            f.write(json.dumps(item, indent=5, ensure_ascii=True))
    except Exception as e:
        print(datetime.now(), 'error in saveInOddsFolder', e, file=open(error_path, 'a'))


def saveInLineupsFolder(item):
    try:
        match_Id = item['matchId']
        with open(f'{lineups_path}/match{match_Id}.json', 'w') as f:
            f.write(json.dumps(item, indent=5, ensure_ascii=True))
    except Exception as e:
        print(datetime.now(), 'error in saveInLineupsFolder', e, file=open(error_path, 'a'))


def saveInIncidentsFolder(item):
    try:
        match_Id = item['matchId']
        with open(f'{incidents_path}/match{match_Id}.json', 'w') as f:
            f.write(json.dumps(item, indent=5, ensure_ascii=True))
    except Exception as e:
        print(datetime.now(), 'error in saveInIncidentsFolder', e, file=open(error_path, 'a'))


def saveInTeamsFolder(item,next_or_last='last'):
    try:
        team_Id = item['team_Id']
        path = last_teams_path if next_or_last == 'last' else next_teams_path
        with open(f'{path}/team{team_Id}.json', 'w') as f:
            f.write(json.dumps(item, indent=5, ensure_ascii=True))
    except Exception as e:
        print(datetime.now(), 'error in saveInTeamsFolder', e, file=open(error_path, 'a'))

class MainscraperPipeline:
    def process_item(self, item, spider):
        if spider.name == "present_base_teams_data":
            saveInTeamsFolder(dict(item),'next')
        if spider.name == "present_base_teams_data_old":
            saveInTeamsFolder(dict(item),'last')
        if spider.name == "present_incidents":
            saveInIncidentsFolder(dict(item))
        if spider.name == "present_odds":
            saveInOddsFolder(dict(item))
        if spider.name == "present_lineups":
            saveInLineupsFolder(dict(item))
        return item
