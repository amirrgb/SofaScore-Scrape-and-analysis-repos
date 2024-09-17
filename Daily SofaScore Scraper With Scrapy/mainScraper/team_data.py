import mysql.connector
user, password, host, teamsDataBase, teamsLinkDataBase = 'root', 'kian1381', 'localhost', "teams_datas", "updated_teams_links"


class TeamData:
    all_new_teams_data = []

    def __init__(self,MatchID=None, Date=None, Position=None, LeagueLink=None,
                 HomeTeam=None, AwayTeam=None, HomeID=None, AwayID=None, HomeResult=None, AwayResult=None,
                 HomeOdd=None, AwayOdd=None, Goals=None, RedCards=None, HomeGoalKeeper=None, AwayGoalKeeper=None,
                 IsPlayoffMatch=None, CurrentTeamLink=None):
        self.MatchID = MatchID
        self.Date = Date
        self.Position = Position
        self.LeagueLink = LeagueLink
        self.HomeTeam = HomeTeam
        self.AwayTeam = AwayTeam
        self.HomeID = HomeID
        self.AwayID = AwayID
        self.HomeResult = HomeResult
        self.AwayResult = AwayResult
        self.HomeOdd = HomeOdd
        self.AwayOdd = AwayOdd
        self.Goals = Goals
        self.RedCards = RedCards
        self.HomeGoalKeeper = HomeGoalKeeper
        self.AwayGoalKeeper = AwayGoalKeeper
        self.IsPlayoffMatch = IsPlayoffMatch
        self.CurrentTeamLink = CurrentTeamLink
        TeamData.all_new_teams_data.append(self)
        if self.Position == "Form":
            self.insert_next_teams_data_to_db()
        else:
            self.insert_last_teams_data_to_db()



    def __str__(self):
        return f"____________complete is : {self.MatchID}--->{self.Date}--->{self.Position}--->{self.LeagueLink}--->" \
               f"{self.HomeTeam}--->{self.AwayTeam}--->{self.HomeID}--->{self.AwayID}--->\n" \
               f"{self.HomeResult}--->{self.AwayResult}--->{self.HomeOdd}--->{self.AwayOdd}--->{self.Goals}--->{self.RedCards}--->{self.HomeGoalKeeper}--->{self.AwayGoalKeeper}--->" \
               f"{self.IsPlayoffMatch}--->{self.CurrentTeamLink}_____________________"

    def insert_next_teams_data_to_db(self):
        cnx = mysql.connector.connect(user=user, password=password, host=host, database=teamsDataBase)
        con = cnx.cursor(buffered=True)
        insertSql = "INSERT INTO teams_datas.teams_data (MatchID, Date, Position, LeagueLink, HomeTeam, AwayTeam, HomeID, AwayID, HomeResult, AwayResult, HomeOdd, AwayOdd, Goals, RedCards, HomeGoalKeeper, AwayGoalKeeper, IsPlayoffMatch, CurrentTeamLink) VALUES (%s, %s, %s,    %s, %s,%s,    %s, %s, %s,     %s, %s ,%s   ,%s,%s,%s,  %s,%s,%s);"
        val = (int(self.MatchID), self.Date, self.Position, self.LeagueLink, self.HomeTeam, self.AwayTeam, int(self.HomeID), int(self.AwayID), -404, -404, None if self.HomeOdd == "null" else self.HomeOdd, None if self.AwayOdd == "null" else self.AwayOdd, self.Goals, self.RedCards, self.HomeGoalKeeper, self.AwayGoalKeeper, self.IsPlayoffMatch, self.CurrentTeamLink)
        con.execute(insertSql, val)
        cnx.commit()
        con.close()


    def insert_last_teams_data_to_db(self):
        cnx = mysql.connector.connect(user=user, password=password, host=host, database=teamsDataBase)
        con = cnx.cursor(buffered=True)
        insertSql = "INSERT INTO teams_datas.teams_data (MatchID, Date, Position, LeagueLink, HomeTeam, AwayTeam, HomeID, AwayID, HomeResult, AwayResult, HomeOdd, AwayOdd, Goals, RedCards, HomeGoalKeeper, AwayGoalKeeper, IsPlayoffMatch, CurrentTeamLink) VALUES (%s, %s, %s,    %s, %s,%s,    %s, %s, %s,     %s, %s ,%s   ,%s,%s,%s,  %s,%s,%s);"
        val = (int(self.MatchID), self.Date, self.Position, self.LeagueLink, self.HomeTeam, self.AwayTeam, int(self.HomeID), int(self.AwayID), int(self.HomeResult) if ((self.HomeResult is not None) and (self.HomeResult != "null")) else None, int(self.AwayResult) if ((self.AwayResult is not None) and (self.AwayResult != "null")) else None, None if self.HomeOdd == "null" else self.HomeOdd, None if self.AwayOdd == "null" else self.AwayOdd, self.Goals, self.RedCards, self.HomeGoalKeeper, self.AwayGoalKeeper, self.IsPlayoffMatch, self.CurrentTeamLink)
        con.execute(insertSql, val)
        cnx.commit()
        con.close()

