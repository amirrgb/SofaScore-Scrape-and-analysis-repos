import mysql.connector
from datetime import datetime, timedelta
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium import webdriver


#need to initialize :
#global user;global password;global host;global database;global teamsDataTable;global faceMatchTable;global teamsLinkTable;global mainDataBase

#pinnig part


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
    # chromePrefers["profile.managed_default_content_settings"] = {"images": 2}
    return option


def isMatchForMenAndAdults(text):
    lines = text.splitlines()
    if "Women" in text:
        return 0
    if "Youth" in text:
        return 0
    if "U17" in text or "U19" in text or "U21" in text or "U22" in text or "U23" in text:
        return 0
    if ("France" in lines[0]) and ("National 2" in lines[1]):
        return 0
    if ("Germany" in lines[0]) and ("Junioren" in lines[1]):
        return 0
    if ("England" in lines[0]) and ("Premier League Cup" in lines[1]):
        return 0
    if ("England" in lines[0]) and ("Premier League 2" in lines[1]):
        return 0
    if ("England" in lines[0]) and ("Northern Premier League" in lines[1]):
        return 0
    if ("England" in lines[0]) and ("Southern Football League" in lines[1]):
        return 0
    if ("Spain" in lines[0]) and ("Primera División Femenina" in lines[1]):
        return 0
    if ("Spain" in lines[0]) and ("Primera Division Femenina" in lines[1]):
        return 0
    return 1


def isLeagueAlreadyPinned(pin_button_txt):
    if 'rotate(' in pin_button_txt:
        return 0
    elif 'translate' in pin_button_txt:
        return 1


def isLeagueMustBePinned(currentCountry, currentLeague, list_file):
    for i in list_file:
        fileCountry = i.split("<=>")[0]
        fileLeague = i.split("<=>")[1]
        if ((fileCountry in currentCountry) and (fileLeague in currentLeague)):
            return 1
    return 0


def pinner(mainPageLeagueAndCountry, list_file):
    mainPageLeagueAndCountryText = mainPageLeagueAndCountry.text
    try:
        pin_button = mainPageLeagueAndCountry.find_element(By.XPATH, './/button')
    except:
        pin_button = None
    if pin_button != None:
        pin_button_txt = pin_button.find_element(By.XPATH, './/*').get_attribute('innerHTML')
        lines = mainPageLeagueAndCountryText.splitlines()
        item = lines[0] + '<=>' + lines[1]
        if ((isLeagueMustBePinned(currentCountry=lines[0], currentLeague=lines[1], list_file=list_file)) and (not isLeagueAlreadyPinned(pin_button_txt))):
            if (isMatchForMenAndAdults(mainPageLeagueAndCountryText)):
                bool = True
                while (bool):
                    pin_button.click()
                    bool = False
                print(item + " >>> pinned")
                time.sleep(2)
        if (not isLeagueMustBePinned(lines[0], lines[1], list_file)) and (isLeagueAlreadyPinned(pin_button_txt)):
            bool = True
            while bool:
                pin_button.click()
                bool = False
            print(item + " >>> unpinned")
            time.sleep(2)
        elif (not isMatchForMenAndAdults(mainPageLeagueAndCountryText)) and (isLeagueAlreadyPinned(pin_button_txt)):
            bool = True
            while bool:
                pin_button.click()
                bool = False
            print(item + " >>> Exception Leagues unpinned")
            time.sleep(2)


def pagePin(driver, filePath):
    allMatchMode,list_file =False,[]
    xCountries = '/html/body/div[1]/main/div/div[2]/div[1]/div[3]/div[2]/div/div[2]/div/div[@role="rowgroup"]//div[div[div[2][a[div]]]]'
    xCountries2 = '/html/body/div[1]/main/div/div[2]/div[1]/div[3]/div[2]/div/div[3]/div/div[@role="rowgroup"]//div[div[div[2][a[div]]]]'
    with open(filePath, 'r+', encoding="utf-8") as f:
        for i in f.readlines():
            list_file.append(i.strip())
    driver = openOrClosePinnedLeagues(driver,'CLOSE')
    driver, screenY,allMatchY = openAllMatch(driver)
    while(True):
        currentY=driver.execute_script("return window.scrollY")
        if currentY>=screenY-200:
            print("end of page")
            break
        if currentY>allMatchY+480 and not allMatchMode:
            print(">>>>>>>>>>>>>>      After All Matches      >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            driver = scrollDown(driver,-1480)
            time.sleep(1.5)
            allMatchMode=True
        if allMatchMode:countries = driver.find_elements(By.XPATH, xCountries2)
        else:countries = driver.find_elements(By.XPATH, xCountries)
        for h in range(len(countries)):
            for l in range(3):
                try:
                    pinner(countries[h], list_file)
                except:
                    pass
        driver = scrollDown(driver, 480)
    return driver


def oneDayPin(driver, url, filePath):
    b = True
    while (b):
        try:
            driver.refresh()
            time.sleep(2)
            driver.get(url)
            time.sleep(2)
            b = False
        except:
            b = True
    print("Pinnig >>> %s" % url)
    pagePin(driver, filePath)
    return driver

#collecting


def openOrClosePinnedLeagues(driver,char):
    try:
        xPinnedLeagues = '/html/body/div[1]/main/div/div[2]/div[1]/div[3]/div[2]/div/div[1]/div[1]/div[1]/div[2]'
        pinnedLeagues = driver.find_element(By.XPATH, xPinnedLeagues)
        pinnedLeaguesHtml = pinnedLeagues.find_element(By.XPATH, './/*').get_attribute('innerHTML')
        if ('rotate(0deg)' in pinnedLeaguesHtml and char=='OPEN'):
            pinnedLeagues.click()
            time.sleep(2)
            driver.refresh()
            time.sleep(2)
        if ('rotate(180deg)' in pinnedLeaguesHtml and char=='CLOSE'):
            pinnedLeagues.click()
            time.sleep(2)
            driver.refresh()
            time.sleep(2)
    except:
        pass
    return driver


def mainSqlCheck(user, password, host, database, rowText):
    cnx = mysql.connector.connect(user=user,host=host, password=password,  database=database)
    con = cnx.cursor(buffered=True)
    v = rowText.splitlines()
    today=datetime.now().date()
    sql = ('SELECT *  FROM main_database.database WHERE  Date = "%s" and HomeTeam = "%s" and AwayTeam = "%s";' % (str(today),v[2], v[3]))
    con.execute(sql)
    for x in con:
        return 0
    return 1


def names_score_positionGetter(presentFaceMatch, rowText):
    t = rowText.splitlines()
    today=datetime.now().date()
    if (len(t)==6 and ("FT" in t[1]) or "AP" in t[1] or "AET" in t[1]):
        presentFaceMatch += '<=>' + t[2] + '<=>' + t[3] + '<=>'+t[4]+'<=>'+t[5]+'<=>'+str(today)+'<=>'+t[1]
    else:
        presentFaceMatch += '<=>' + t[2] + '<=>' + t[3] + '<=>-2<=>-2<=>'+str(today)+'<=>form'
    return presentFaceMatch


def faceMatchPopupCheck(popupText, presentFaceMatch):
    homeTeamName, awayTeamName = "zzzz", "zzzz"
    if (popupText != None):
        lines = popupText.splitlines()
        try:
            if 'Created' not in lines[0]:
                b = lines[2].split(" - ")
                homeTeamName, awayTeamName = b[0].strip(), b[1].strip()
            elif 'Created' in lines[0]:
                b = lines[3].split(" - ")
                homeTeamName, awayTeamName = b[0].strip(), b[1].strip()
        except:
            ""
    t = presentFaceMatch.split("<=>")
    # t[2]=home_name
    if t[2] in homeTeamName:
        return 1
    elif t[3] in awayTeamName:
        return 1
    elif homeTeamName in t[2]:
        return 1
    elif awayTeamName in t[3]:
        return 1
    return 0


def country_leagueGetter(presentFaceMatch, popupText):
    if (popupText != None):
        lines = popupText.splitlines()
        try:
            if 'Created' not in lines[0]:
                presentFaceMatch += lines[0] + '<=>' + lines[1]
                return presentFaceMatch
            elif 'Created' in lines[0]:
                presentFaceMatch += lines[1] + '<=>' + lines[2]
                return presentFaceMatch
        except:
            presentFaceMatch += "null<=>null"
            return presentFaceMatch


def pregameScoreCheck(pregameFormLines, presentFaceMatch):
    time.sleep(1)
    firstLine = pregameFormLines[0].get_attribute('innerHTML')
    t = presentFaceMatch.split("<=>")
    # t[2]=homeName
    if t[2] in firstLine:
        return 1
    elif t[3] in firstLine:
        return -1
    elif firstLine in t[2]:
        return 1
    elif firstLine in t[3]:
        return -1
    return 0


def pregameScoreGetter(presentFaceMatch, pregameForm):
    if pregameForm is not None:
        pregameFormNames = pregameForm.find_elements(By.XPATH, './/div[2]/div[3]/div')
        pregameFormScores = pregameForm.find_elements(By.XPATH, './div//div/div[6]/div')
        if (len(pregameFormNames) >= 1):
            if (len(pregameFormScores) == 2):
                for i in range(2):
                    if (pregameScoreCheck(pregameFormNames, presentFaceMatch) > 0):
                        for scores in pregameFormScores:
                            score = scores.get_attribute('innerHTML')
                            presentFaceMatch += '<=>' + score
                        return presentFaceMatch
                    elif (pregameScoreCheck(pregameFormNames, presentFaceMatch) < 0):
                        for scores in reversed(pregameFormScores):
                            score = scores.get_attribute('innerHTML')
                            presentFaceMatch += '<=>' + score
                        return presentFaceMatch
                    time.sleep(2)
                else:
                    presentFaceMatch += "<=>null<=>null"
                return presentFaceMatch
    else:
        presentFaceMatch += "<=>null<=>null"
        return presentFaceMatch


def teamHrefGetter(presentFaceMatch, teamsLink):
    for teamLink in teamsLink:
        link = teamLink.get_attribute('href')
        presentFaceMatch += '<=>' + link
    return presentFaceMatch


def collect(driver):
    counter = 1
    xRowGroup = '/html/body/div[1]/main/div/div[2]/div[1]/div[3]/div[2]/div/div[1]/div[1]/div[@id="pinned-list-fade-target"]/div/div/div[@role="rowgroup"]'
    xPopup = '/html/body/div[1]/main/div/div[2]/div[1]/div[5]'
    xPregameForm = '/html/body/div[1]/main/div/div[2]/div[1]/div[5]/div/div[1]/div/div[2]/div[2]/div/div//div[h3[text() = "Pregame form"]]'
    xTeamsLink = '/html/body/div[1]/main/div/div[2]/div[1]/div[5]/div/div[1]/div/div[1]/div[2]/div//a'
    driver = openOrClosePinnedLeagues(driver, 'OPEN')
    driver, screen ,y= openAllMatch(driver)
    for j in range(0, 60):
        if (counter >= 4):
            break
        if (j >= 1):  # scroll
            driver = scrollDown(driver, 330)
            counter += 1
        try:
            rowGroup = driver.find_element(By.XPATH, xRowGroup)
        except:
            rowGroup = None
            break
        if (rowGroup != None):
            rows = rowGroup.find_elements(By.XPATH, './div/a')
            p = False
            for c in range(min(12,len(rows))):
                row = rows[c]
                if mainSqlCheck(user, password, host, database, row.text):
                    presentFaceMatch = ""
                    bool = True
                    connectionCheck = True
                    while (bool):
                        time.sleep(1)
                        row.click()
                        p = True
                        # collecting_data:
                        time.sleep(2)  # script_load
                        popupText = driver.find_element(By.XPATH, xPopup).text
                        presentFaceMatch = country_leagueGetter(presentFaceMatch, popupText)
                        presentFaceMatch = names_score_positionGetter(presentFaceMatch, row.text)
                        if (faceMatchPopupCheck(popupText, presentFaceMatch)):
                            if (not connectionCheck):
                                print("connected >>> OK")
                            try:
                                pregameForm = driver.find_element(By.XPATH, xPregameForm)
                            except:
                                pregameForm = None
                            presentFaceMatch = pregameScoreGetter(presentFaceMatch, pregameForm)
                            teamsLink = driver.find_elements(By.XPATH, xTeamsLink)
                            presentFaceMatch = teamHrefGetter(presentFaceMatch, teamsLink)
                            print("presentFaceMatch === %s" %presentFaceMatch)
                            driver,mainList=HAteamCollect(driver,presentFaceMatch)
                            mainDataBaseConstructor(presentFaceMatch,mainList)
                            bool = False
                            connectionCheck = True
                        else:
                            print("connecting . . . . . . . . . . ")
                            connectionCheck = False
                            presentFaceMatch = ""
                            time.sleep(3)
            if (not p):
                counter -= 1
    return driver


def oneDayPinAndCollect(driver, url, todayLeaguesPath):
    while (True):
        try:
            driver.refresh()
            time.sleep(2)
            driver.get(url)
            time.sleep(2)
            break
        except:
            print("refreshing ....")
    print("\nPinnig >>> %s\n" % url)
    #driver = pagePin(driver, todayLeaguesPath)
    #driver.refresh()
    #time.sleep(6)
    driver.find_element(By.XPATH, '/html/body').send_keys(Keys.HOME)
    screen = int(driver.execute_script("return document.documentElement.scrollHeight"))
    time.sleep(2)
    print("\ncollecting >>> %s\n" % url)
    driver = collect(driver)
    return driver


#sql /tools /. . .  . . .. .  .


def threeTab(driver):
    m = driver.find_element(By.XPATH, '/html/body')
    m.click()
    parent = driver.window_handles[0]
    driver.execute_script("window.open('about:blank', 'tab2');")
    time.sleep(3)
    homeTeamTab = driver.window_handles[1]
    driver.execute_script("window.open('about:blank', 'tab3');")
    time.sleep(3)
    awayTeamTab = driver.window_handles[2]
    driver.switch_to.window(parent)
    return driver


def start(url):
    option = webdriver.ChromeOptions()
    s=Service(executablePath)
    driver = webdriver.Chrome(service=s,options=workOption(option))
    driver.maximize_window()
    driver.get(url)
    for handle in driver.window_handles:
        driver.switch_to.window(handle)
    for i in range(4): # accept all cookies  button
        try:
            accept = driver.find_element(By.XPATH, '//*[@id="onetrust-accept-btn-handler"]')
            if (accept.is_displayed):
                accept.click()
        except:
            pass
    return driver


def timer(n):
    for x in reversed(range(0, n)):
        print(x + 1)
        time.sleep(1)


def scrollDown(driver, length):
    driver.execute_script("window.scrollTo(0,window.scrollY + %s );" % length)
    time.sleep(2)
    return driver


def openAllMatch(driver):
    for l in range(1, 3):
        try:
            xAllMatchButton = '/html/body/div[1]/main/div/div[2]/div[1]/div[@style="max-width:100%"]/div[2]/div[1]/button'
            allMatchButton = driver.find_element(By.XPATH, xAllMatchButton)
            loc=allMatchButton.location
            allMatchButton.send_keys(Keys.ENTER)
            time.sleep(6)
            driver.find_element(By.XPATH, '/html/body').send_keys(Keys.PAGE_UP)
            time.sleep(1)
            driver.find_element(By.XPATH, '/html/body').send_keys(Keys.PAGE_DOWN)
            time.sleep(1)
            driver.find_element(By.XPATH, '/html/body').send_keys(Keys.PAGE_DOWN)
            time.sleep(1)
            driver.find_element(By.XPATH, '/html/body').send_keys(Keys.PAGE_DOWN)
            time.sleep(1)
            driver.find_element(By.XPATH, '/html/body').send_keys(Keys.PAGE_DOWN)
            time.sleep(1)
            driver.find_element(By.XPATH, '/html/body').send_keys(Keys.PAGE_DOWN)
            time.sleep(1)
            driver.find_element(By.XPATH, '/html/body').send_keys(Keys.END)
            time.sleep(2)
            screenY = int(driver.execute_script("return window.scrollY"))
            driver.find_element(By.XPATH, '/html/body').send_keys(Keys.HOME)
            time.sleep(1)
            driver.find_element(By.XPATH, '/html/body').send_keys(Keys.HOME)
            time.sleep(1)
            return driver, screenY,loc['y']
        except:
            pass
    driver.find_element(By.XPATH, '/html/body').send_keys(Keys.END)
    time.sleep(2)
    screenY = int(driver.execute_script("return window.scrollY"))
    return driver, screenY,loc['y']


#previous games
#teamsData


def isValidSituation(teamGame):
    teamGame=teamGame.split("<=>")
    if ("Walkover" in teamGame[7] or "Postponed" in teamGame[7] or "Canceled" in teamGame[7] or "Awarded" in teamGame[7] or "FRO" in teamGame[7] or "Retired" in teamGame[7] or "Friendly" in teamGame[1]):
        return 0
    return 1


def dateIsBefore(currentDate,date):
    format = '%Y-%m-%d'
    current = datetime.strptime(currentDate, format).date()
    temp = datetime.strptime(date, format).date()
    if current>temp:
        return 1
    else:
        return 0


def previous(driver):
    xPreviousButton='/html/body/div[1]/main/div/div[2]/div[1]/div[1]/div[3]/div/div/div[1]/div/div[1]/div[1]/div'
    previousButton=driver.find_element(By.XPATH,xPreviousButton)
    time.sleep(1)
    previousButton.click()
    time.sleep(1)
    return driver


def previousPopupCheck(popupText, rowText):
    homeTeamName, awayTeamName = "zzzz", "zzzz"
    if (popupText != None) and (len(popupText.splitlines()) != 0):
        lines = popupText.splitlines()
        if 'Created' not in lines[0]:
            b = lines[2].split(" - ")
            homeTeamName, awayTeamName = b[0], b[1]
        elif 'Created' in lines[0]:
            b = lines[3].split(" - ")
            homeTeamName, awayTeamName = b[0], b[1]
    else:
        return 0
    t = rowText.splitlines()
    if (t[2] in homeTeamName )and (t[3] in awayTeamName):
        return 1
    elif (homeTeamName in t[2])and( awayTeamName in t[3]):
        return 1
    return 0


def primaryScore(driver):
    tempList,homeTeamScore,awayTeamScore,c=[''],'','',0
    xWidgetBody='/html/body/div[1]/main/div/div[2]/div[1]/div[1]/div[3]/div/div/div[@class="sc-e5255230-0 hLaBJx widget-wrapper"]/div/div[3]/div'
    xFT='/html/body/div[1]/main/div/div[2]/div[1]/div[1]/div[3]/div/div/div[2]//ol/li[div[1]/div[1]]'
    for a in range(4):
        try:
            tempList.extend([q.text for q in driver.find_elements(By.XPATH,xFT)])
            tempList = list(dict.fromkeys(tempList))
            for l in tempList:
                if "FT " in l:
                    l=l[3:]
                    homeTeamScore,awayTeamScore=l.split(" - ")[0],l.split(" - ")[1]
                    driver.find_element(By.XPATH,xWidgetBody).send_keys(Keys.HOME)
                    time.sleep(1)
                    print("primary detected")
                    return driver,homeTeamScore,awayTeamScore
            driver.find_element(By.XPATH,xWidgetBody).send_keys(Keys.PAGE_DOWN)
            time.sleep(1)
        except:
            if c==1:
                break
            c+=1
    try:
        time.sleep(1)
        driver.find_element(By.XPATH,xWidgetBody).send_keys(Keys.HOME)
    except:pass
    return driver,"null","null"


def insertToMainDataBase(user,password,host,mainDataBase,faceMatchText):
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=mainDataBase)
    con = cnx.cursor(buffered=True)
    fileds=faceMatchText.split("***")
    if(len(fileds)==28):
        print("ok\n\n")
        if fileds[8] == 'null' or fileds[9] == 'null':
            sql = "INSERT INTO main_database.database (Date, Position, Country, League, HomeTeam, AwayTeam, HomeResult, AwayResult, HomePreGameScore, AwayPreGameScore, XH1, XH2, XH3, YH1, YH2, YH3, YH4, ZH1, ZH2, XA1, XA2, XA3, YA1, YA2, YA3, YA4, ZA1, ZA2) VALUES (%s, %s, %s,%s, %s, %s, %s,%s,NULL,NULL, %s, %s,%s, %s, %s, %s,%s,%s,%s,%s, %s, %s,%s, %s, %s, %s,%s,%s)"
            val = (fileds[6],fileds[7],fileds[0],fileds[1],fileds[2],fileds[3],int(fileds[4]),int(fileds[5]),fileds[10],fileds[11],fileds[12],fileds[13],fileds[14],fileds[15],fileds[16],fileds[17],fileds[18],fileds[19],fileds[20],fileds[21],fileds[22],fileds[23],fileds[24],fileds[25],fileds[26],fileds[27])
        else:
            sql = "INSERT INTO main_database.database (Date, Position, Country, League, HomeTeam, AwayTeam, HomeResult, AwayResult, HomePreGameScore, AwayPreGameScore, XH1, XH2, XH3, YH1, YH2, YH3, YH4, ZH1, ZH2, XA1, XA2, XA3, YA1, YA2, YA3, YA4, ZA1, ZA2) VALUES (%s, %s, %s,%s, %s, %s, %s,%s,%s,%s, %s, %s,%s, %s, %s, %s,%s,%s,%s,%s, %s, %s,%s, %s, %s, %s,%s,%s)"
            val = (fileds[6],fileds[7],fileds[0],fileds[1],fileds[2],fileds[3],int(fileds[4]),int(fileds[5]),int(fileds[8]),int(fileds[9]),fileds[10],fileds[11],fileds[12],fileds[13],fileds[14],fileds[15],fileds[16],fileds[17],fileds[18],fileds[19],fileds[20],fileds[21],fileds[22],fileds[23],fileds[24],fileds[25],fileds[26],fileds[27])
        con.execute(sql, val)
        cnx.commit()


#mainDataBaseGetter


def presentHGetter(list,faceMatchDate, faceMatchHomeTeam):
    Hlist=[]
    for i in list:
        if len(Hlist) !=3:
            if dateIsBefore(faceMatchDate,i.split("<=>")[6]):
                if isValidSituation(i):
                    Hlist.append(i)
    for i in list:
        if faceMatchHomeTeam==i.split("<=>")[2]:
            if len(Hlist) !=7:
                if dateIsBefore(faceMatchDate,i.split("<=>")[6]):
                    if isValidSituation(i):
                        Hlist.append(i)
    for i in list:
        if faceMatchHomeTeam==i.split("<=>")[3]:
            if len(Hlist) !=9:
                if dateIsBefore(faceMatchDate,i.split("<=>")[6]):
                    if isValidSituation(i):
                        Hlist.append(i)
    return Hlist


def presentAGetter(list,faceMatchDate, faceMatchAwayTeam):
    Alist=[]
    for i in list:
        if len(Alist) !=3:
            if dateIsBefore(faceMatchDate,i.split("<=>")[6]):
                if isValidSituation(i):
                    Alist.append(i)
    for i in list:
        if faceMatchAwayTeam==i.split("<=>")[3]:
            if len(Alist) !=7:
                if dateIsBefore(faceMatchDate,i.split("<=>")[6]):
                    if isValidSituation(i):
                        Alist.append(i)
    for i in list:
        if faceMatchAwayTeam==i.split("<=>")[2]:
            if len(Alist) !=9:
                if dateIsBefore(faceMatchDate,i.split("<=>")[6]):
                    if isValidSituation(i):
                        Alist.append(i)
    return Alist


def presentMainGetter(faceMatch,GamesList,char):
    faceMatchDate, faceMatchHomeTeam, faceMatchAwayTeam= faceMatch[6],faceMatch[2],faceMatch[3]
    if char=='h':
        if len(presentHGetter(GamesList,faceMatchDate, faceMatchHomeTeam)) !=9:
            return GamesList
    if char=='a':
        if len(presentAGetter(GamesList,faceMatchDate, faceMatchAwayTeam)) !=9:
            return GamesList
    if char=='h':
        XH1, XH2, XH3, YH1, YH2, YH3, YH4, ZH1, ZH2=presentHGetter(GamesList,faceMatchDate, faceMatchHomeTeam)
        previousGames = [XH1, XH2, XH3, YH1, YH2, YH3, YH4, ZH1, ZH2]
    if char=='a':
        XA1, XA2, XA3, YA1, YA2, YA3, YA4, ZA1, ZA2=presentAGetter(GamesList,faceMatchDate, faceMatchAwayTeam)
        previousGames = [XA1, XA2, XA3, YA1, YA2, YA3, YA4, ZA1, ZA2]
    return previousGames


def hardCollector(output,homeTeamScore,awayTeamScore,rowText,league):
    t = rowText.splitlines()
    t1 = t[0].split('/')
    if "null" in homeTeamScore and len(t)>=5:
            homeTeamScore,awayTeamScore=t[4],t[5]
    output +="<=>"+league+ '<=>' + t[2] + '<=>' + t[3] +'<=>' + homeTeamScore + '<=>' + awayTeamScore + '<=>20' + t1[2] + '-' + t1[1] + '-' + t1[0] + '<=>' + t[1]
    return output


def easyCollector(output,rowText,league):
    t = rowText.splitlines()
    t1 = t[0].split('/')
    if len(t)>=5:homeTeamScore,awayTeamScore=t[4],t[5]
    else:homeTeamScore,awayTeamScore="null","null"
    output +="<=>"+league+  '<=>' + t[2] + '<=>' + t[3] +'<=>' + homeTeamScore + '<=>' + awayTeamScore + '<=>20' + t1[2] + '-' + t1[1] + '-' + t1[0] + '<=>' + t[1]
    return output


def sortTens(tens):
    temp=[]
    format = '%Y-%m-%d'
    for i in tens:
        temp.append(datetime.strptime(i[:10], format).date())
    temp.sort(datetime.strptime(i[:10], format).date())
    


def collect_ten(driver,faceMatchDate,gamesList):
    print("waiting for teams data . . . . . . . . . . .")
    format = '%Y-%m-%d'
    beginningDate=datetime.strptime('2018-07-01', format).date()
    faceMatchDate=datetime.strptime(faceMatchDate, format).date()
    xList_wrapper = '/html/body/div[1]/main/div/div[2]/div[1]/div[1]/div[3]/div/div/div[1]/div[@class="list-wrapper"]'
    xPopup = '/html/body/div[1]/main/div/div[2]/div[1]/div[1]/div[3]/div/div/div[2]'
    xCountry='/html/body/div[1]/main/div/div[1]/ul/li[2]/a'
    country = driver.find_element(By.XPATH, xCountry)
    TeamLink=driver.current_url
    list_wrapper = driver.find_element(By.XPATH, xList_wrapper)
    if list_wrapper != None:
        rows = list_wrapper.find_elements(By.XPATH, "./div[2]//a")
    if len(rows)==0:
        return driver,False
    currentLeague,tens='',[]
    for j in (range(len(rows))):
        list_wrapper = driver.find_element(By.XPATH, xList_wrapper)
        rows = list_wrapper.find_elements(By.XPATH, "./div[2]//a")
        row=rows[j]
        homeTeamScore,awayTeamScore,rowText,output='','',row.text,country.text
        if len(rowText.splitlines())<=3:
            currentLeague=rowText
        elif rowText.find(':') == -1:
            if (rowText.find('AP') > 0) or (rowText.find('AET') > 0):
                time.sleep(1)
                row.click()
                time.sleep(3)
                popup = driver.find_element(By.XPATH, xPopup)
                if previousPopupCheck(popup.text, rowText):
                    driver,homeTeamScore,awayTeamScore=primaryScore(driver)
                    output =hardCollector(output,homeTeamScore,awayTeamScore,rowText,currentLeague)
            else:
                output=easyCollector(output,rowText,currentLeague)
            output+="<=>"+TeamLink
            this=datetime.strptime(output.split("<=>")[6], format).date()
            if(this<beginningDate):
                return driver,False
            if(this<=faceMatchDate):
                tens.append(output)
                print("tens+=%s")
    for i in reversed(tens):
        print("i in reversed tens =%s"%i)
        gamesList.append(i)
    for j in gamesList:
        print("gamesList =%s"%j)
    return driver,True


def HAteamCollect(driver,faceMatch):
    faceMatch=faceMatch.split("<=>")
    faceMatchDate,homeTeamLink,awayTeamLink=faceMatch[6],faceMatch[10],faceMatch[11]
    bool=True
    homeTeamGamesList,awayTeamGamesList=[],[]
    parent= driver.window_handles[0]
    homeTeamTab = driver.window_handles[1]
    awayTeamTab = driver.window_handles[2]
    driver.switch_to.window(homeTeamTab)
    driver.get(homeTeamLink)
    while(bool):#homeTeamCollect
        driver,bool=collect_ten(driver,faceMatchDate,homeTeamGamesList)
        if(len(presentMainGetter(faceMatch,homeTeamGamesList,'h'))==9):
            homeTeamGamesList=presentMainGetter(faceMatch,homeTeamGamesList,'h')
            break
        driver=previous(driver)
    driver.switch_to.window(awayTeamTab)
    driver.get(awayTeamLink)
    bool=True
    while(bool):#awayTeamCollect
        driver,bool=collect_ten(driver,faceMatchDate,awayTeamGamesList)
        if(len(presentMainGetter(faceMatch,awayTeamGamesList,'a'))==9):
            awayTeamGamesList=presentMainGetter(faceMatch,awayTeamGamesList,'a')
            break
        driver=previous(driver)
    HAGamesList = homeTeamGamesList + awayTeamGamesList
    driver.switch_to.window(parent)
    return driver,HAGamesList


#mainDataBaseConstructor


def mainDataBaseConstructor(faceMatch,HAgamesList):
    previousGamesText,faceMatchText='',''
    # change faceMatch to formal (***)
    for ff in faceMatch.split("<=>")[:-2]:
        faceMatchText+=str(ff)+"***"
    # change list to formal (<=>)
    for i in range(18):
        temp=HAgamesList[i].split("<=>")[6]+"<=>"+HAgamesList[i].split("<=>")[7]
        for f in [0,1,2,3,4,5,8]:
            temp+="<=>"+HAgamesList[i].split("<=>")[f]
        HAgamesList[i]=temp
        previousGamesText+="'%s'***"%str(HAgamesList[i])#note
    faceMatchText+=previousGamesText[:-3]#last ***
    for l in range(len(faceMatchText.split("***"))):
        print("field%s == %s"%(l,faceMatchText.split("***")[l]))
    insertToMainDataBase(user,password,host,mainDataBase,faceMatchText)


def use():
    driver=start("https://www.sofascore.com")
    time.sleep(15)
    n=input("press any key .....\n")
    driver=threeTab(driver)
    oneDayPinAndCollect(driver,"https://www.sofascore.com",todayLeaguesPath)


use()
