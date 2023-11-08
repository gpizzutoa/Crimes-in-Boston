import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd
import random

# MySQL DB credentials
user = "Insert here your username"
password = "Insert here your password"

def createdb(user:str, passw:str):
    db = mysql.connect(host="localhost", user=user, passwd=passw)
    curs = db.cursor()

    databasecreation = "CREATE DATABASE Boston Crimes"

    curs.execute(databasecreation)

def creattables(user:str,passw:str):
    db = mysql.connect(host="localhost", user=user, passwd=passw, database="chess")
    curs = db.cursor()

    name_table_1 = "CREATE TABLE Players(" \
                   "Player_ID INT PRIMARY KEY," \
                   "Player_Name VARCHAR(255) NOT NULL," \
                   "Best_Rating SMALLINT NOT NULL," \
                   "Worst_Rating SMALLINT NOT NULL," \
                   "Year_of_Birth INT NOT NULL" \
                   ");"

    name_table_2 = "CREATE TABLE Victory_Types(" \
                       "Vt_ID SMALLINT PRIMARY KEY," \
                       "Vt VARCHAR(255) NOT NULL," \
                       "Winner VARCHAR(255) NOT NULL" \
                       ");"

    name_table_n = "CREATE TABLE Openings(" \
                   "Op_ID INT PRIMARY KEY," \
                   "Op_name VARCHAR(255) NOT NULL," \
                   "Op_eco VARCHAR(10) NOT NULL" \
                   ");"

    curs.execute(name_table_1)
    curs.execute(name_table_2)
    curs.execute(name_table_n)

def dataload(user: str,passw: str):
    datafile = "DATA/games.csv"
    db = mysql.connect(host="localhost", user=user, passwd=passw, database="chess")
    data = pd.read_csv(datafile, index_col=False, delimiter=",", encoding="UTF-8")
    players = {}
    random.seed(283010)
    for pl in data["black_id"]:
        if players.get(pl) is None:
            players[pl] = [3000, 0]
    for pl in data["white_id"]:
        if players.get(pl) is None:
            players[pl] = [3000, 0]
    playerslist = list(players.keys())
    playerslist.sort()
    gametype = {}
    for gt in data["increment_code"]:
        if gametype.get(gt) is None:
            gametype[gt] = 1
    gametypelist = list(gametype.keys())
    for pl, rt in zip(data["white_id"], data["white_rating"]):
        if players[pl][0] > rt:
            players[pl][0] = rt
        if players[pl][1] < rt:
            players[pl][1] = rt
    for pl, rt in zip(data["black_id"], data["black_rating"]):
        if players[pl][0] > rt:
            players[pl][0] = rt
        if players[pl][1] < rt:
            players[pl][1] = rt
    curs = db.cursor()

    # players
    for pl in playerslist:
        wr = players[pl][0]
        br = players[pl][1]
        year = random.randint(1936, 2014)
        ID = playerslist.index(pl) + 1
        T = (ID, pl, br, wr, year)
        query = "INSERT INTO Players (Player_ID,Player_Name,Best_Rating,Worst_Rating,Year_of_Birth) VALUES (%s,%s,%s,%s,%s)"
        curs.execute(query, T)

    # gametype
    for el in gametypelist:
        ID = gametypelist.index(el) + 1
        query = "INSERT INTO Gametypes (GT_ID,GT_Name) VALUES (%s,%s)"
        T = (ID, el)
        curs.execute(query, T)

    # openings
    openings = {}
    for name, eco in zip(data["opening_name"], data["opening_eco"]):
        code = f"{name},{eco}"
        if openings.get(code) is None:
            openings[code] = 1
    openingslist = []
    for k in openings.keys():
        L = k.split(",")
        openingslist.append(tuple(L))
    for el in openingslist:
        ID = openingslist.index(el) + 1
        T = (ID, el[0], el[1])
        query = "INSERT INTO Openings (Op_ID, Op_name, Op_eco) VALUES (%s,%s,%s)"
        curs.execute(query, T)

    # games
    IDLIST = []
    for n, row in data.iterrows():
        ID = row[0]
        if ID not in IDLIST:
            rated = row[1]
            nmoves = row[4]
            vict = (row[5], row[6])
            gametype = row[7]
            whitepl = row[8]
            whitert = row[9]
            blackpl = row[10]
            blackrt = row[11]
            moves = row[12]
            opening = (row[14], row[13])
            opnum = row[15]
            victID = Victorylist.index(vict) + 1
            whiteID = playerslist.index(whitepl) + 1
            blackID = playerslist.index(blackpl) + 1
            gametypeID = gametypelist.index(gametype) + 1
            openingID = openingslist.index(opening) + 1
            T = (ID, rated, nmoves, victID, gametypeID, whiteID, whitert, blackID, blackrt, moves, openingID, opnum)
            query = "INSERT INTO Games (Game_id,Rated,Turns,Victory_ID,GameType_ID,White_ID,White_Rating,Black_ID,Black_Rating,Moves,Opening_ID,Opening_ply) " \
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            curs.execute(query, T)
            IDLIST.append(ID)
    db.commit()
#creating database
try:
    print("creating the database...")
    createdb(user=user, passw=password)
    print("defining tables...")
    creattables(user=user, passw=password)
    print("loading the dataset into the DB...")
    dataload(user=user, passw=password)
except Error as e:
    print(f"the database already exists, running queries only...")

"""Database creation:"""

