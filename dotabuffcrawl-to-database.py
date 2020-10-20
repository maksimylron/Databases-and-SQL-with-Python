import urllib.request
import urllib.parse
import urllib.error
import re
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import ssl
import time
import pandas as pd
import os
import sys
import sqlite3
import dateutil.parser
from dateutil import tz

# Variable Initialization
match_result = ''
hero_played = ''
reset = 0
tagsLength = 0
dateIndex = 0
tagNum = 0
localTZ = tz.tzlocal()


# create Matches Database
conn = sqlite3.connect(os.path.join(sys.path[0], 'matchedDB.sqlite'))
cur = conn.cursor()

# Make some fresh tables using executescript()
cur.executescript('''
DROP TABLE IF EXISTS Matches;
DROP TABLE IF EXISTS Matches_Date;
DROP TABLE IF EXISTS Matches_length;

CREATE TABLE Matches (
    match_id TEXT PRIMARY KEY UNIQUE,
    hero_played TEXT,
    match_result TEXT
);

CREATE TABLE Matches_Date (
    matchDate_id TEXT PRIMARY KEY UNIQUE,
    match_date DATETIME
);

CREATE TABLE Matches_length (
    matchLength_id TEXT PRIMARY KEY UNIQUE,
    match_length TEXT
);

''')

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# input SteamID
steamID = input('Enter SteamID: ')

# parse steamID URL to get 64bit Steam bit equivalent
steamIDurl = 'https://steamcommunity.com/id/' + steamID + '/?xml=1'
reqID = urllib.request.urlopen(steamIDurl, context=ctx)
dataID = reqID.read().decode()
IDtree = ET.fromstring(dataID)
ID64 = IDtree.find('steamID64').text
ID32 = int(ID64) - 76561197960265728

# Access Dotabuff Match History
url = 'https://www.dotabuff.com/players/' + str(ID32) + '/matches'
print('Retrieving', url)
uh = urllib.request.Request(url, headers={'user-agent': 'Mozilla/5.0'})
uh2 = urllib.request.urlopen(uh, context=ctx)

data = uh2.read()
soup = BeautifulSoup(data, 'html.parser')

xLists = list()
gameDict = dict()
resultDict = dict()
matchdateList = list()
matchlengthList = list()

tags = soup('a')
matchDate = soup('time')
matchLengthtags = soup('td')

# get total number of match pages
totalPage = int(re.findall('([0-9]+)">Last', str(tags))[0])

# get save match dates in list
for tag in matchDate:
    x = re.findall('datetime="(.*\S)" title=?', str(tag))
    tagNum = tagNum + 1
    if tagNum > 3:
        timeUTC = dateutil.parser.parse(x[0])
        timeLocal = timeUTC.astimezone(localTZ)
        matchdateList.append(timeLocal)

# print(matchdateList)
# print(len(matchdateList))

# get save match length in list
for tag in matchLengthtags:
    if re.search('([0-9]+:[0-9]+)', tag.text):
        matchlengthList.append(tag.text)

# print(len(matchlengthList))

for tag in tags:
    x = re.findall('/matches/(.*\S?)<', str(tag))
    for i in x:
        if i != []:
            i = i.split('">')
            match_id = i[0]
            if re.search('Match', i[1]):
                resultDict[i[0]] = resultDict.get(i[0], i[1])
                match_result = i[1]
                reset = reset + 1
            else:
                gameDict[i[0]] = gameDict.get(i[0], i[1])
                hero_played = i[1]
                reset = reset + 1

            match_result = match_result
            hero_played = hero_played

            # populate database
            if reset == 2:
                # populated Matches Table
                cur.execute('''INSERT OR IGNORE INTO Matches (match_id, hero_played, match_result)
                    VALUES ( ?, ?, ?)''', (match_id, hero_played, match_result))
                reset = 0

                # populated Matches_Date Table
                cur.execute('''INSERT OR IGNORE INTO Matches_Date (matchDate_id, match_date)
                    VALUES ( ?, ? )''', (match_id, matchdateList[dateIndex]))

                # populated Matches_length Table
                cur.execute('''INSERT OR IGNORE INTO Matches_length (matchLength_id, match_length)
                    VALUES ( ?, ? )''', (match_id, matchlengthList[dateIndex]))

                dateIndex = dateIndex + 1
                # self.dateIndex += 1

                conn.commit()

# create pandas dataframe from dictionary
dfgameResults = pd.DataFrame(resultDict.items(), columns=['MatchID', 'Result'])
dfgameData = pd.DataFrame(gameDict.items(), columns=['MatchID', 'Hero Played'])
print('Dataframe saved')
# save dataframes for csv files
dfgameResults.to_csv(os.path.join(sys.path[0], 'gameResults_complete.csv'), index=False)
dfgameData.to_csv(os.path.join(sys.path[0], 'gameData_complete.csv'), index=False)
print('csv files saved')


# main loop after 1st page of matches
k = 2
for j in range(k, totalPage + 1):
    # for j in range(k, 4):
    try:

        tagNum = 0
        dateIndex = 0
        matchdateList = []
        matchlengthList = []

        url = 'https://www.dotabuff.com/players/' + str(ID32) + '/matches?enhance=overview&page=' + \
            str(j)
        print('Retrieving', url)
        time.sleep(5)
        uh = urllib.request.Request(url, headers={
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) \
                                Gecko/20100101 Firefox/81.0'})
        time.sleep(5)
        uh2 = urllib.request.urlopen(uh, context=ctx)

        data = uh2.read()
        soup = BeautifulSoup(data, 'html.parser')

        tags = soup('a')
        matchDate = soup('time')
        matchLengthtags = soup('td')

        match_result = ''
        hero_played = ''
        reset = 0

        # get save match dates in list
        for tag in matchDate:
            x = re.findall('datetime="(.*\S)" title=?', str(tag))
            tagNum = tagNum + 1
            if tagNum > 3:
                timeUTC = dateutil.parser.parse(x[0])
                timeLocal = timeUTC.astimezone(localTZ)
                matchdateList.append(timeLocal)

        # get save match length in list
        for tag in matchLengthtags:
            if re.search('([0-9]+:[0-9]+)', tag.text):
                matchlengthList.append(tag.text)

        for tag in tags:
            x = re.findall('/matches/(.*\S?)<', str(tag))
            for i in x:
                if i != []:
                    i = i.split('">')
                    match_id = i[0]
                    if re.search('Match', i[1]):
                        resultDict[i[0]] = resultDict.get(i[0], i[1])
                        match_result = i[1]
                        reset = reset + 1
                    else:
                        gameDict[i[0]] = gameDict.get(i[0], i[1])
                        hero_played = i[1]
                        reset = reset + 1

                    match_result = match_result
                    hero_played = hero_played

                    # populate database
                    if reset == 2:
                        cur.execute('''INSERT OR IGNORE INTO Matches (match_id, hero_played, match_result)
                            VALUES ( ?, ?, ?)''', (match_id, hero_played, match_result))
                        reset = 0

                        # populated Matches_Date Table
                        cur.execute('''INSERT OR IGNORE INTO Matches_Date (matchDate_id, match_date)
                            VALUES ( ?, ? )''', (match_id, matchdateList[dateIndex]))

                        # populated Matches_length Table
                        cur.execute('''INSERT OR IGNORE INTO Matches_length (matchLength_id, match_length)
                            VALUES ( ?, ? )''', (match_id, matchlengthList[dateIndex]))

                        dateIndex = dateIndex + 1

                        conn.commit()

        # print(matchdateList)

        # create pandas dataframe from dictionary
        dfgameResults = pd.DataFrame(resultDict.items(), columns=['MatchID', 'Result'])
        dfgameData = pd.DataFrame(gameDict.items(), columns=['MatchID', 'Hero Played'])
        print('Dataframe saved')
        # save dataframes for csv files
        dfgameResults.to_csv(os.path.join(sys.path[0], 'gameResults_complete.csv'), index=False)
        dfgameData.to_csv(os.path.join(sys.path[0], 'gameData_complete.csv'), index=False)
        print('csv files saved')

    except urllib.error.HTTPError as e:
        k = j
        print(e)
        print('Wait 600 seconds')
        time.sleep(600)
