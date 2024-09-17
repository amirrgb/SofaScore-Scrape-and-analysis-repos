import time
import mysql.connector
from selenium import webdriver
from datetime import datetime
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from datetime import datetime, timedelta
from selenium.webdriver.common.keys import Keys
from functools import partial
import json

teamsDataBase, teamsLinksDataBase, leaguesDataBase = "teams_datas", "teams_links", "leagues_datas"

executablePath = "C://SofaScore/ChromeDriver/ChromeDriver.exe"
teamsDataLogFile = 'C://SofaScore/teamsDataLog.txt'
teamsDataTerminalLogFile = 'C://SofaScore/teamsDataTerminalLog.txt'
screenshotFilePath = "C:/SofaScore/MatchesWithoutPopupResultScreenshots/"


def loadUrl(driver, url):
    firstTimeLoad = True
    while True:
        try:
            driver.set_page_load_timeout(10)
            driver.get(url)
            time.sleep(3)
            try:
                newurl = driver.current_url
            except Exception as e:
                insertToLogFile("Error while accessing to driver", e)
                continue
            page_status = driver.execute_script('return document.readyState;')
            if get_response(driver) and 'complete' == page_status:
                if isSofaScoreLoadCompletely(driver):
                    for i in range(2):
                        driver = closeHelpUsImproveWindow(driver)
                        time.sleep(1)
                        driver = acceptAllCookies(driver)
                        time.sleep(1)
                        driver = clickOnConsentButton(driver)
                        time.sleep(1)
                    driver.execute_script("document.body.style.zoom='67%'")
                    time.sleep(5)
                    return
            else:
                driver.refresh()
        except Exception as e:
            insertToLogFile("Error While Loading Url : %s" % url, e)
            try:
                if not firstTimeLoad:
                    driver.refresh()
                    time.sleep(2)
                    try:
                        loadUrl(driver, url)
                        return
                    except:
                        pass
                else:
                    firstTimeLoad = False
            except:
                pass
            time.sleep(3)


def get_response(driver):
    logs = driver.get_log('performance')
    for log in logs:
        if log['message']:
            d = json.loads(log['message'])
            try:
                content_type = 'text/html' in d['message']['params']['response']['headers']['content-type']
                response_received = d['message']['method'] == 'Network.responseReceived'
                if content_type and response_received:
                    if "200" == str(d['message']['params']['response']['status']):
                        return True
                    else:
                        printAndInsertToTerminalLogFile(str(d['message']['params']['response']['status']))
                        return False
            except:
                pass
    return False


def deleteXsFromTeamName(teamName: str):
    allXs = [" x11", " x10", " x9", " x8", " x7", " x6", " x5", " x4", " x3", " x2", " x1"]

    for x in allXs:
        if x in teamName:
            return teamName.replace(x, "").strip()

    return teamName.strip()


def isSofaScoreLoadCompletely(driver):
    try:
        body = driver.find_element(By.XPATH, '/html/body//main')
        tt = body.text
    except:
        return False
    return True


def takeScreenShot(driver: webdriver, rowText: str):
    try:
        now = "".join([i if i != ":" else "." for i in str(datetime.now())[:19]])
        t = rowText.split("\n")
        t1 = t[0].split("/")
        matchDate = str("20" + t1[2] + "-" + t1[1] + "-" + t1[0])
        name = " ".join([matchDate, t[1], t[2] + "-" + t[3], t[4] + "-" + t[5]])
        if len(t) == 7:
            name = name + " " + t[6]
        thisFilePath = str(screenshotFilePath + "%s (%s).png" % (name, now))
        driver.save_screenshot(thisFilePath)
        time.sleep(5)
    except:
        try:
            now = "".join([i if i != ":" else "." for i in str(datetime.now())[:19]])
            thisFilePath = str(screenshotFilePath + "%s (%s).png" % (name, now))
            driver.get_screenshot_as_file(thisFilePath)
            time.sleep(5)
        except Exception as e:
            insertToLogFile("Error while taking Screenshot : %s" % thisFilePath, e)


def workOption(option):
    option.add_argument("--start-maximized")
    option.add_argument("disable-infobars")
    option.add_argument("--disable-dev-shm-usage")
    option.add_argument("--disable-gpu")
    option.add_argument("--no-sandbox")
    # option.add_argument('--ignore-certificate-errors')
    # option.add_argument("headless")
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
                time.sleep(3)
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
                time.sleep(3)
                return driver
        except:
            pass
    return driver


def clickOnConsentButton(driver):
    xConsentButtonButton = '//button[@class="fc-button fc-cta-consent fc-primary-button"]'
    for i in range(4):
        try:
            accept = driver.find_element(By.XPATH, xConsentButtonButton)
            time.sleep(0.5)
            if accept.is_displayed():
                accept.click()
                time.sleep(3)
                return driver
        except:
            pass
    return driver


def findElement(self, xxPath: str, level: str, finds: bool = False, refreshTime: int = 16, Get_None: bool = False,
                time_out=20, text_check=False, timer=True):
    n = 0
    while True:

        n += 1
        if n % time_out == 0 and refreshTime != 16:
            self = loadUrl(self, self.current_url)
            time.sleep(refreshTime)
            if n == 60 and Get_None is True:
                return None
        elif Get_None is True and n == time_out:
            return None

        try:
            if finds:
                element = self.find_elements(By.XPATH, xxPath)
            else:
                element = self.find_element(By.XPATH, xxPath)

            if (element is not None) or (element.is_displayed()):
                self = self
                try:
                    if text_check:
                        if element.text is not None:
                            text = element.text
                    return element
                except Exception as e:
                    insertToLogFile("Error While getting text of  Element", e)
        except Exception as e:
            if n % 15 == 1 and not Get_None:
                insertToLogFile("Error While Finding Element in level : " + level, e)
            time.sleep(1)

        if n % 4 == 0 and timer:
            printAndInsertToTerminalLogFile(int(n / 4))


def start(startUrl="https://www.sofascore.com", needLoad: bool = True):
    global currentTeamLink
    currentTeamLink = startUrl
    option = webdriver.ChromeOptions()
    s = Service(executablePath)
    caps = DesiredCapabilities().CHROME
    caps["pageLoadStrategy"] = "normal"
    caps["goog:loggingPrefs"] = {"performance": "ALL"}
    driver = webdriver.Chrome(service=s, options=workOption(option), desired_capabilities=caps)
    driver.maximize_window()
    if needLoad: loadUrl(driver, startUrl)
    driver.findElement = partial(findElement, driver)
    return driver


def scrollDown(driver, length: int = 330, t: float = 1):
    driver.execute_script("window.scrollTo(0,window.scrollY + %s );" % length)
    time.sleep(t)
    return driver


def insertToLogFile(level, exceptText, element=None):
    exceptText = "\n".join(str(exceptText).split("\n")[:3])
    if element is not None:
        level = "%s ----- > %s" % (level, element)
    report = "\nCurrentTeam : %s \nTime : %s \nLevel : %s \nError : %s \n" % (
        currentTeamLink, datetime.now(), level, exceptText)
    with open(teamsDataLogFile, "a", encoding="utf-8") as f:
        f.write(report)


def printAndInsertToTerminalLogFile(text, end="\n"):
    print(text, end=end)

    with open(teamsDataTerminalLogFile, "a", encoding="utf-8") as f:
        f.write("%s%s" % (str(text), end))


def teamsLinksExtractor(numberOfTeamsLinks):
    linksList = []

    cnx = mysql.connector.connect(
        user=user, password=password, host=host, database=teamsLinksDataBase)
    cursor = cnx.cursor(buffered=True)
    cursor.execute(
        "SELECT *  FROM teams_link WHERE IsCollected = 'False' OR IsCollected = 'false'")

    for link in cursor:
        if numberOfTeamsLinks <= 0:
            break
        linksList.append(link[0])
        numberOfTeamsLinks -= 1

    return linksList


def teamIsCollected(teamLink, mode="True"):
    cnx = mysql.connector.connect(
        user=user, password=password, host=host, database=teamsLinksDataBase)
    con = cnx.cursor(buffered=True)
    sql = "UPDATE teams_link SET isCollected = '%s' WHERE  teamLink = '%s' ;" % (
        mode, teamLink)
    con.execute(sql)
    cnx.commit()
    cnx.close()


def previous(driver, t: float):
    while True:
        xPreviousButton = '//div[@elevation=",3"]/div[div[@color="onSurface.nLv1"]]/div[2][@display="flex"]/div/div/div[@display="flex"]//button'
        try:
            previousButtons = driver.find_elements(By.XPATH, xPreviousButton)
            for previousButton in previousButtons:
                if previousButton.text == "PREVIOUS":
                    previousButton.send_keys(Keys.ENTER)
                    time.sleep(t)
                    return driver, True
            else:
                return driver, False
        except Exception as e:
            insertToLogFile("Error While Clicking On Previous Button", e)


def insertToTeamsDataBase(output):
    cnx = mysql.connector.connect(
        user=user, password=password, host=host, database=teamsDataBase)
    con = cnx.cursor(buffered=True)
    t = output.split("<=>")

    if len(t) == 8:
        Date, Position, LeagueLink, HomeTeam, AwayTeam, HomeResult, AwayResult, TeamLink = t
        if "null" in t[6] or "null" in t[5]:
            sql = "INSERT INTO teams_data (Date, Position, LeagueLink, HomeTeam, AwayTeam, HomeResult, AwayResult, TeamLink) VALUES (%s, %s,%s, %s, %s,NULL,NULL,%s)"
            val = (Date, Position, LeagueLink, HomeTeam, AwayTeam, TeamLink)
        else:
            sql = "INSERT INTO teams_data (Date, Position, LeagueLink, HomeTeam, AwayTeam, HomeResult, AwayResult, TeamLink) VALUES (%s, %s,%s, %s, %s, %s, %s,%s)"
            val = (Date, Position, LeagueLink, HomeTeam,
                   AwayTeam, HomeResult, AwayResult, TeamLink)

        con.execute(sql, val)
        cnx.commit()


def collector(rowText, homeTeamScore="null", awayTeamScore="null"):
    t = rowText.splitlines()
    t1 = t[0].split('/')
    date = '20' + t1[2] + '-' + t1[1] + '-' + t1[0]
    if ("null" in homeTeamScore) or ("null" in awayTeamScore):
        if len(t) > 5:
            homeTeamScore, awayTeamScore = t[4], t[5]
        else:
            homeTeamScore, awayTeamScore = "null", "null"
    homeTeam = deleteXsFromTeamName(t[2])
    awayTeam = deleteXsFromTeamName(t[3])
    output = date + '<=>' + t[1] + '<=>' + currentLeague + '<=>' + \
             homeTeam + '<=>' + awayTeam + '<=>' + homeTeamScore + '<=>' + awayTeamScore

    return output.replace("  ", " ")


def isTherePopup(driver):
    xPopup = '//div[@display="none,block"][div[@style="overflow: hidden;"]]/div/div/div/div'  # ends with widget in inspect
    popup = driver.findElement(xPopup, "Finding Popup", time_out=12, text_check=True, timer=False, Get_None=True)
    return True if popup is not None else False


def realScoreCollect(driver):
    xPopup = '//div[@display="none,block"][div[@style="overflow: hidden;"]]/div/div/div/div'  # ends with widget in inspect
    homeTeamScore, awayTeamScore = 'null', 'null'
    popup = driver.findElement(xPopup, "Searching for Popup", time_out=10, text_check=True, timer=False, Get_None=True)
    lastPopupText = ""
    popup_data_id = popup.find_element(By.XPATH, "./a").get_attribute("data-id")
    global popupIsOpen
    popupIsOpen, firstTry = False, True
    if row_data_id == popup_data_id:
        popupIsOpen = True
        popupStartTime = datetime.now()
        while datetime.now() - popupStartTime < timedelta(seconds=60):  # net
            popupText = driver.findElement(xPopup, "Searching for Popup", time_out=10, text_check=True, timer=False,
                                           Get_None=True).text
            if lastPopupText == popupText and not firstTry:
                break
            else:
                lastPopupText = popupText
            try:
                for line in popupText.splitlines():
                    if "FT" in line and len(line) == 8 and " - " in line:
                        line = line[3:]
                        homeTeamScore, awayTeamScore = line.split(" - ")[0], line.split(" - ")[1]
                        printAndInsertToTerminalLogFile(line)
                        return homeTeamScore, awayTeamScore
                try:
                    widget = driver.findElement(
                        '//div[@display="none,block"][div[@style="overflow: hidden;"]]/div/div[3]/div',
                        "Popup Scrolling", Get_None=True, time_out=20, timer=False)
                    if widget is None:
                        continue
                    widget.send_keys(Keys.PAGE_DOWN)
                    time.sleep(2)
                except Exception as e:
                    if firstTry:
                        firstTry = False
                    else:
                        insertToLogFile("Error in Scrolling Popup", e)
            except Exception as e:
                insertToLogFile("Error While working with popup text", e)
                time.sleep(1)
    if homeTeamScore == "null" or awayTeamScore == "null":
        try:
            widget = driver.findElement(
                '//div[@display="none,block"][div[@style="overflow: hidden;"]]/div/div[3]/div',
                "Popup Scrolling", Get_None=True, time_out=20, timer=False)
            if widget is not None:
                widget.send_keys(Keys.HOME)
                time.sleep(2)
        except:
            pass
    return homeTeamScore, awayTeamScore


def getTextOf(row):
    for i in range(10):
        try:
            rowText = row.text
            if rowText is not None:
                return rowText
        except Exception as e:
            insertToLogFile("Error While getting row text", e)
            time.sleep(3)
    return None


def popupWork(driver, rowText, j: int):
    xRows = '//div[@style="overflow:hidden"][a[@data-id]]//a'
    rows = driver.findElement(xRows, "Getting matches table", finds=True)
    global row_data_id
    row_data_id = rows[j].get_attribute("data-id")
    global popupIsOpen
    popupIsOpen = False
    while True:
        try:
            rows = driver.findElement(xRows, "Getting matches table", finds=True)
            rows[j].send_keys(Keys.ENTER)
            setPage(driver)
            time.sleep(2)
            if isTherePopup(driver):
                homeTeamScore, awayTeamScore = realScoreCollect(driver)
                if homeTeamScore != "null" and awayTeamScore != "null":
                    return homeTeamScore, awayTeamScore
            if popupIsOpen:
                break
        except Exception as e:
            insertToLogFile("Error while row click (AP and AET)", e)
            time.sleep(3)
    printAndInsertToTerminalLogFile("(Result Can not get from Popup !)")
    takeScreenShot(driver, rowText)
    insertToLogFile("Error while getting match result from Popup",
                    "Result Can not get from Popup For Match : \n" + "<=>".join(rowText.splitlines()))
    return "null", "null"


def collectTenMatches(driver):
    xRows = '//div[@style="overflow:hidden"][a[@data-id]]//a'
    global situations
    situations, tens = [], []
    rows = driver.findElement(xRows, "Getting matches table", finds=True)
    for j in (range(len(rows))):
        rows = driver.findElement(xRows, "Getting matches table", finds=True)
        row = rows[j]
        rowText = getTextOf(row)
        if rowText is None:
            situations.append("imperfect")
            break
        if rowText.find(':') >= 0:
            situations.append("firstPage")
        elif len(rowText.splitlines()) <= 3:  # its league
            global currentLeague
            currentLeague = row.get_attribute("href")
        else:
            while True:
                position = rowText.split("\n")[1]
                if (position == "AP" or position == "AET") and not (
                        "listDuplicate" in situations or "DBDuplicate" in situations):
                    printAndInsertToTerminalLogFile("Need to open Popup (AET & AP Matches)...", end=" ")
                    homeTeamScore, awayTeamScore = popupWork(driver, rowText, j)
                    output = collector(rowText, homeTeamScore, awayTeamScore)
                else:
                    output = collector(rowText)
                output += "<=>" + currentTeamLink
                if len(output.split("<=>")) == 8:
                    break
            thisDate = datetime.strptime(
                output.split("<=>")[0], '%Y-%m-%d').date()
            if thisDate < beginningDate:
                situations.append("lastPage")
            elif output in teamData:
                situations.append("listDuplicate")
            else:
                tens.append(output)

    if lastPageCheck == 9:
        tens = tens[::-1]
        situations.append("lastPage")
        printAndInsertToTerminalLogFile("<<<<<<   Last Page because of No previous   >>>>>>")
    elif (0 < len(tens) < 10) and ("firstPage" not in situations) and ("lastPage" not in situations) and not situations.count("listDuplicate") == 1:
        situations.append("imperfect")
        time.sleep(5)  # net
        printAndInsertToTerminalLogFile("Retry to collect this page...")
        time.sleep(5)  # net
        tens = []
    elif not len(tens) > 10 and situations.count("listDuplicate") > 1:
        tens = []
    else:
        tens = tens[::-1]
    if tens != []:
        printAndInsertToTerminalLogFile("%s Game in " % len(tens), end="")
    return driver, tens


def hasPreviousButton(driver, teamLink):
    xPreviousButton = '//div[@elevation=",3"]/div[div[@color="onSurface.nLv1"]]/div[2][@display="flex"]/div/div/div[@display="flex"]//button'
    noPrevious = 5
    try:
        driver = scrollDown(driver, 400)
    except:
        return False
    while noPrevious != 0:
        previousButton = driver.findElement(xPreviousButton, "Searching for Previous button ", Get_None=True)
        if previousButton == None:
            driver = scrollDown(driver, 400)
            noPrevious -= 1
        elif previousButton.is_displayed():
            global previousButtonY
            previousButtonY = previousButton.location['y'] - 100
            driver.execute_script("window.scrollTo(0, %s)" % previousButtonY)
            break
    else:
        teamIsCollected(teamLink, "No Previous")
        return False
    return True


def setPage(driver):
    while True:
        xPreviousButton = '//div[@elevation=",3"]/div[div[@color="onSurface.nLv1"]]/div[2][@display="flex"]/div/div/div[@display="flex"]//button'
        previousButton = driver.findElement(xPreviousButton, "Searching for Previous button", Get_None=True)
        if previousButton is None: driver = scrollDown(driver, -150)
        try:
            driver.execute_script("arguments[0].scrollIntoView(true);", previousButton)
            break
        except:
            pass
    driver = scrollDown(driver, length=-30, t=1)


def oneTeamCollect(driver):
    global teamData
    global currentTeam
    global beginningDate
    global lastPageCheck
    global situations
    global p
    p, teamData, lastPageCheck, situations = 1, [], 0, []
    currentTeam = currentTeamLink
    beginningDate = datetime.strptime("2018-06-01", '%Y-%m-%d').date()
    printAndInsertToTerminalLogFile("\nCollecting >>> %s" % currentTeamLink)
    loadUrl(driver, currentTeamLink)
    startTime = datetime.now()
    printAndInsertToTerminalLogFile("Start Time : %s" % startTime)
    # printAndInsertToTerminalLogFile("first date : %s\n"%str(beginningDate))
    while True:
        if not hasPreviousButton(driver, currentTeamLink):
            loadUrl(driver, currentTeamLink)
        else:
            break
    setPage(driver)
    while 'lastPage' not in situations:
        if datetime.now() - startTime >= timedelta(minutes=10):
            loadUrl(driver, driver.current_url)
            setPage(driver)
            startTime, p = datetime.now(), 1
            printAndInsertToTerminalLogFile("Start Time : %s" % startTime)
        driver, tensList = collectTenMatches(driver)
        teamData.extend(tensList)
        teamData = list(set(teamData))
        if "imperfect" not in situations:
            if "lastPage" in situations:
                printAndInsertToTerminalLogFile("Last Page")
                break
            else:
                if "DBDuplicate" in situations or "listDuplicate" in situations:
                    driver, buttonCheck = previous(driver, 1.5)
                else:
                    driver, buttonCheck = previous(driver, 3.5)
                if buttonCheck:
                    printAndInsertToTerminalLogFile("Page %s" % p)
                    p += 1
                    lastPageCheck = 0
                else:
                    lastPageCheck += 1
                setPage(driver)
        else:
            lastPageCheck += 1
    teamData.sort()
    for output in teamData:
        insertToTeamsDataBase(output)
    printAndInsertToTerminalLogFile(
        str(len(teamData)) + " Matches in " + str(p) + " Pages in " + str(datetime.now() - startTime)[:7])
    return driver


def main():
    driver = start()
    while True:
        try:
            driver.find_element(By.XPATH, '/html/body').send_keys(Keys.HOME)
            break
        except:
            loadUrl(driver, driver.current_url)

    global currentTeamLink

    while True:
        choice = input(
            "\nWhat do you want to do?\n\t1.Extract Data From Teams Links Data Base\n\t2.Extracts Data From Entered Links\n")
        if choice == "1":
            numbersOfTeamsLinks = int(
                input("\nHow many not Collected links do you want the data to be extracted?\n0 = Back\n"))

            if 0 != numbersOfTeamsLinks:
                extractedLinks = teamsLinksExtractor(numbersOfTeamsLinks)
                for link in extractedLinks:
                    currentTeamLink = link
                    driver = oneTeamCollect(driver)
                    teamIsCollected(currentTeamLink)
                    printAndInsertToTerminalLogFile(str("\n%s is Collected\n" % link), end="")
                    printAndInsertToTerminalLogFile("%s teams is remaining !" % str(numbersOfTeamsLinks - 1))
                    numbersOfTeamsLinks -= 1

        if choice == "2":
            links = [input("Please Enter All Urls! \ne = End\n")]

            while True:
                n = input()
                if n == "e":
                    break
                links.append(n)
            for link in links:
                currentTeamLink = link
                driver = oneTeamCollect(driver)
                printAndInsertToTerminalLogFile("%s is Collected" % link)


main()
