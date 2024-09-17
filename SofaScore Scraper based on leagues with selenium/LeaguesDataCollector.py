import time
import mysql.connector
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from datetime import datetime, timedelta
from selenium.webdriver.common.keys import Keys
from functools import partial

executablePath = "C://Python/ChromeDriver/ChromeDriver.exe"
logFile = 'C://python/log.txt'
leaguesLinkDatabase, leaguesDataBase = "leagues_links", "leagues_datas"


def loadUrl(driver, url):
    n = 0
    while True:
        n += 1
        if n == 21:
            try:
                driver.refresh()
            except:
                pass
        try:
            driver.set_page_load_timeout(300)
            driver.get(url)
            time.sleep(3)
            while True:
                try:
                    currentUrl = driver.current_url
                    if url in currentUrl or currentUrl in url:
                        break
                except Exception as e:
                    if n % 4 == 1:
                        insertToLogFile("loading %s....." % url, e)
                    time.sleep(1)
            driver = acceptAllCookies(driver)
            return driver
        except Exception as e:
            if n % 4 == 1:
                insertToLogFile("connecting %s....." % url, e)
            time.sleep(1)

def workOption(option):  # chromeDriverOptions
    option.add_argument("--start-maximized")
    option.add_argument("disable-infobars")
    option.add_argument("--disable-dev-shm-usage")
    option.add_argument("--disable-gpu")
    option.add_argument("--no-sandbox")
    option.add_experimental_option('excludeSwitches', ['enable-logging'])
    chromePrefers = {}
    option.experimental_options["prefs"] = chromePrefers
    chromePrefers["profile.default_content_settings"] = {"images": 2}
    chromePrefers["profile.managed_default_content_settings"] = {"images": 2}
    return option

def closeHelpUsImproveWindow(driver):
    xAskMeLaterButton = '/html/body/div[1]/div[@class="sc-2a240de0-0 czLNBh"]/div[1]/div[2]'
    for i in range(4):
        try:
            askMeLater = driver.find_element(By.XPATH, xAskMeLaterButton)
            time.sleep(0.5)
            if askMeLater.is_displayed():
                askMeLater.click()
                time.sleep(2)
                return driver
        except:
            pass
    return driver

def acceptAllCookies(driver):
    xAcceptAllCookiesButton = '//*[@id="onetrust-accept-btn-handler"]'
    for i in range(4):
        try:
            accept = driver.find_element(By.XPATH, xAcceptAllCookiesButton)
            time.sleep(0.5)
            if accept.is_displayed():
                accept.click()
                time.sleep(2)
                return driver
        except:
            pass
    return driver

def Tab(driver: webdriver, add: int = 0):
    global tabsList
    count = add
    if count == 0:
        tabsList = []
        m = driver.find_element(By.XPATH, '/html/body')
        time.sleep(2)
        m.click()
        tabsList.append(driver.window_handles[0])
        return driver
    else:
        for i in range(count):
            driver.execute_script("window.open('about:blank', 'tab%s');" % (i + 2))
            time.sleep(1)
            tabsList.append(driver.window_handles[i + 1])
        print(["one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"][count])
    driver.switch_to.window(tabsList[0])
    time.sleep(2)
    return driver

def findElement(self, xxPath: str, level: str, finds: bool = False, refreshTime: int = 16, Get_None: bool = False,
                timeout: int = 20):
    n = 0
    while True:
        n += 1
        if n % timeout == 0 and refreshTime != 16:
            self.refresh()
            time.sleep(refreshTime)
            if n == 60 and Get_None == True:
                return None
        elif Get_None == True and n == timeout:
            return None
        try:
            if finds == True:
                element = self.find_elements(By.XPATH, xxPath)
            else:
                element = self.find_element(By.XPATH, xxPath)
            if element is not None or element.is_displayed():
                self = self
                return element
        except Exception as e:
            if n % 4 == 1:
                insertToLogFile(level, e)
            time.sleep(1)
        if n % 4 == 0:
            print(int(n / 4))

def start(startUrl="https://www.sofascore.com"):
    option = webdriver.ChromeOptions()
    s = Service(executablePath)
    caps = DesiredCapabilities().CHROME
    caps["pageLoadStrategy"] = "normal"
    driver = webdriver.Chrome(desired_capabilities=caps, service=s, options=workOption(option))
    driver.maximize_window()
    driver = loadUrl(driver, startUrl)
    driver = Tab(driver)
    driver.findElement = partial(findElement, driver)
    return driver

def scrollDown(driver, length: int = 330, t: float = 1):
    driver.execute_script("window.scrollTo(0,window.scrollY + %s );" % length)
    time.sleep(t)
    return driver

def leaguesExtractor():
    list = []
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=leaguesLinkDatabase)
    cursor = cnx.cursor(buffered=True)
    cursor.execute("SELECT LeagueLink FROM leagues_link where isCollected = '0';")
    for league in cursor:
        list.append(league[0])
    list.sort()
    cnx.close()
    return list

def isMatchForMenAndAdults(country, league):
    inValidKeyWord = ["Women", "Youth", "U17", "U19", "U21", "U22", "U23"]
    for key in inValidKeyWord:
        if key in league:
            return False
    if ("France" in country) and ("National 2" in league):
        return False
    if ("Germany" in country) and ("Junioren" in league):
        return False
    if ("England" in country) and ("Premier League Cup" in league):
        return False
    if ("England" in country) and ("Premier League 2" in league):
        return False
    if ("England" in country) and ("Northern Premier League" in league):
        return False
    if ("England" in country) and ("Southern Football League" in league):
        return False
    if ("Spain" in country) and ("Primera División Femenina" in league):
        return False
    if ("Spain" in country) and ("Primera Division Femenina" in league):
        return False
    if ("Morocco" in country) and ("Botola 2" in league):
        return False
    return True

def insertToLogFile(level, exceptText):
    report = "@@@@\nTime : %s \nLevel :\t>>>\t>>>\t>>>\t>>> %s \nError : %s \n@@@@\n" % (
    datetime.now(), level, str(exceptText).splitlines()[0])
    with open(logFile, "a", encoding="utf-8") as f:
        f.write(report)

def insertLeagueLink(country: str, league: str, link: str):
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=leaguesDataBase)
    cursor = cnx.cursor(buffered=True)
    sql = ('UPDATE studied_league SET leagueLink = "%s" where leagueCountry = "%s" and leagueName = "%s";' % (
    link, country, league))
    cursor.execute(sql)
    cnx.commit()
    cnx.close()


def insertToLeaguesData(output):
    t = output.split("<=>")
    if len(t) == 5:
        LeagueLink, LaegueYears, LeagueStartDate, NumberOfLeagueTeams, LeagueTeams = t[0], t[2], t[1], t[3], t[4]
        cnx = mysql.connector.connect(user=user, password=password, host=host, database=leaguesDataBase)
        con = cnx.cursor(buffered=True)
        sql = "INSERT INTO leagues_data (LeagueLink, LaegueYears, LeagueStartDate, NumberOfLeagueTeams, LeagueTeams) VALUES (%s,%s,%s,%s,%s)"
        val = (LeagueLink, LaegueYears, LeagueStartDate, NumberOfLeagueTeams, LeagueTeams)
        con.execute(sql, val)
        cnx.commit()
        cnx.close()


def getLeagueTeams(driver):
    xPanel = '/html/body/div[1]/div/main/div/div[2]/div[1]/div[2]/div/div[1]/div/div[@data-panelid="1"]/div'
    panel = driver.findElement(xPanel, "cant get teams in goToLeague method", refreshTime=25)
    teams = panel.find_elements(By.XPATH, "./a")
    teamsList = []
    for team in teams:
        teamLink = team.get_attribute("href")
        teamLink = teamLink.split("/")
        teamsList.append(teamLink[-2] + "/" + teamLink[-1])
    return driver, teamsList


def switchToPrevoiusYear(driver):
    xOtherYearButton = '/html/body/div[1]/div/main/div/div[2]/div[1]/div[1]/div[@elevation=",3"]/div[1]/div[div[@elevation=",2"]]/div[2]/div[1]/div//button'
    xOtherYears = '/html/body/div[1]/div/main/div/div[2]/div[1]/div[1]/div[@elevation=",3"]/div[1]/div[div[@elevation=",2"]]/div[2]/div[1]/div//div[1]/ul//li'
    i = 0
    while i <= 3:
        otherYearButton = driver.findElement(xOtherYearButton, "cant find other years button")
        global currentYear
        currentYear, situation = otherYearButton.text, False
        time.sleep(1)
        otherYearButton.click()
        time.sleep(2)
        otheryears = driver.findElement(xOtherYears, "cant load other years", finds=True)
        for year in otheryears:
            if year.is_displayed():
                if year.text == currentYear and not situation:
                    situation = True
                elif situation:
                    if "2018" in currentYear or "18/19" in currentYear or "2017" in currentYear or "17/18" in currentYear:
                        return driver, True
                    try:
                        year.click()
                        return driver, False
                    except:
                        pass
            else:
                i -= 1
                break
        i += 1
    return driver, True


def getFirstRoundDate(driver):
    try:
        xFirstGame = '//div[@class="sc-hKwDye gtOvrf"]/a[1]'
        firstGame = driver.findElement(xFirstGame, "get first game of league text", Get_None=True)
        if firstGame is None:
            date = None
        else:
            firstGameText = firstGame.text
            date = str(datetime.now().date())
            if firstGameText.find(":") >= 1:
                return driver, date
            if len(firstGameText.splitlines()) >= 1:
                t1 = firstGameText.splitlines()[0].split("/")
                if len(t1) == 3:
                    date = ('20' + t1[2] + '-' + t1[1] + '-' + t1[0])
    except Exception as e:
        insertToLogFile("get first round date", e)
    return driver, date


def goToFirstRound(driver, output):
    xByRound = '//div[@class="sc-hKwDye fxquVd"]/div[@data-tabid="2"]'
    xWhichRound = '//button[@class="sc-fotOHu gDuGmB"]'
    xFirstRound = '//div[@class="sc-hBUSln vlIDo"]//ul/li[@role]'
    needGoFirst, noRound = 8, 2
    driver.find_element(By.XPATH, '/html/body').send_keys(Keys.END)
    while True:
        needGoFirst -= 1
        driver = scrollDown(driver, -350, 1)
        if needGoFirst == 0:
            driver.find_element(By.XPATH, '/html/body').send_keys(Keys.END)
            needGoFirst = 8
            noRound -= 1
        try:
            byRound = findElement(driver, xByRound, "finding byRound", Get_None=True, timeout=10)
            if byRound is not None:
                if "BY ROUND" not in byRound.text:
                    return driver, output + "<=>NULL"
                while True:
                    if byRound.is_displayed():
                        driver.execute_script("window.scrollTo(0, %s)" % (byRound.location['y'] - 100))
                        while True:
                            try:
                                time.sleep(2)
                                byRound.click()
                                time.sleep(2)
                                break
                            except:
                                pass
                        whichRound = driver.findElement(xWhichRound, "finding for which round ...")
                        time.sleep(2)
                        whichRound.click()
                        time.sleep(2)
                        firstRounds = driver.findElement(xFirstRound, "finding first Round in Rounds", finds=True,
                                                         Get_None=True)
                        if firstRounds is not None:
                            for firstRound in firstRounds:
                                if firstRound.text == "Round 1":
                                    while True:
                                        try:
                                            firstRound.click()
                                            time.sleep(2)
                                            break
                                        except:
                                            pass
                                    time.sleep(2)
                                    break
                            driver, firstDate = getFirstRoundDate(driver)
                            if firstDate is not None:
                                output += "<=>" + firstDate
                                return driver, output
                        else:
                            driver.find_element(By.XPATH, '/html/body').send_keys(Keys.END)
                            break
            elif noRound == 0:
                print("no round detected")
                return driver, output + "<=>NULL"
        except Exception as e:
            insertToLogFile("in goToFirstRound method ", e)


def leagueDuplicateCheck(leagueLink):
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=leaguesDataBase)
    cursor = cnx.cursor(buffered=True)
    cursor.execute('DELETE from leagues_data WHERE LeagueLink = "%s";' % (leagueLink))
    cnx.commit()
    cnx.close()


def isCollected(leagueLink):
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=leaguesLinkDatabase)
    cursor = cnx.cursor(buffered=True)
    cursor.execute('UPDATE leagues_link SET isCollected = 1 WHERE LeagueLink = "%s";' % (leagueLink))
    cnx.commit()
    cnx.close()


def goToLeague(driver, leagueLink):
    driver = loadUrl(driver, leagueLink)
    time.sleep(5)
    isEnd = False
    while not isEnd:
        output = leagueLink
        driver, teamsList = getLeagueTeams(driver)
        driver, output = goToFirstRound(driver, output)
        driver.find_element(By.XPATH, '/html/body').send_keys(Keys.HOME)
        time.sleep(2)
        driver, isEnd = switchToPrevoiusYear(driver)
        driver.find_element(By.XPATH, '/html/body').send_keys(Keys.HOME)
        if currentYear.find("/") >= 1:
            standardYear = "20" + currentYear[:3] + "20" + currentYear[3:]
        else:
            standardYear = currentYear
        output += "<=>" + standardYear + "<=>" + str(len(teamsList)) + "<=>"
        for team in teamsList[:-1]:
            output += team + "<>"
        output += teamsList[-1]
        print("\n" + output)
        insertToLeaguesData(output)
    return driver


def main():
    leaguesLink = leaguesExtractor()
    driver = start()
    for link in leaguesLink:
        leagueDuplicateCheck(link)
        print("\ncollecting >>>", link)
        print(str(datetime.now()))
        driver = goToLeague(driver, link)
        isCollected(link)
    print("end")


main()
