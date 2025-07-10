import sqlite3
import urllib.request
import json
import re
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

conn = sqlite3.connect('dsc450.db')
c = conn.cursor()

createtbl_user = """
CREATE TABLE User
(
    id NUMBER(20) PRIMARY KEY,
    name VARCHAR(30),
    screen_name VARCHAR(15),
    description VARCHAR(10),
    friends_count NUMBER(9)
);
"""
c.execute('DROP TABLE User')
c.execute(createtbl_user)

createtbl_geo = """
CREATE TABLE Geo
(
    id NUMBER(10) PRIMARY KEY,
    type VARCHAR(8),
    latitude VARCHAR(13),
    longitude VARCHAR(13)
);
"""
c.execute('DROP TABLE Geo')
c.execute(createtbl_geo)

createtbl_tweet = """
CREATE TABLE Tweet
(
    created_at TIMESTAMP,
    id_str VARCHAR(18) PRIMARY KEY,
    text VARCHAR(240),
    source VARCHAR(100),
    in_reply_to_user_id NUMBER(10),
    in_reply_to_screen_name VARCHAR(15),
    in_reply_to_status_id NUMBER(18),
    retweet_count NUMBER(6),
    contributors NUMBER(4),
    user_id NUMBER(20),
    geo_id NUMBER(10),

    FOREIGN KEY(user_id) REFERENCES User(id),
    FOREIGN KEY(geo_id) REFERENCES Geo(id)
);
"""
c.execute('DROP TABLE Tweet')
c.execute(createtbl_tweet)
conn.commit()
conn.close()

#1-a
#----------------------------------------------------------------------------------------------------
web250 = urllib.request.urlopen("https://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt")
tweets250 = open('tweets250.txt','wb')

start_time = time.time()
for i in range(250000):
    tweets250.write(web250.readline())
timewrite250 = time.time() - start_time

web250.close()
tweets250.close()

web1500 = urllib.request.urlopen("https://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt")
tweets1500 = open('tweets1500.txt','wb')

start_time = time.time()
for i in range(1500000):
    tweets1500.write(web1500.readline())
timewrite1500 = time.time() - start_time

web1500.close()
tweets1500.close()

print("1-a")
print("Time Write 250000 =", timewrite250)
print("Time Write 1500000 =", timewrite1500)
print()

#1-b
#----------------------------------------------------------------------------------------------------
conn = sqlite3.connect('dsc450.db')
c = conn.cursor()
web250 = urllib.request.urlopen("https://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt")

start_time = time.time()
for i in range(250000):
    try:
      tdict = json.loads(web250.readline().decode('utf8'))
      
      if tdict['geo'] is None:
        geo_id = None
         
      else:
        lat = tdict['geo']['coordinates'][0]
        lon = tdict['geo']['coordinates'][1]
        strlat = ''.join(re.findall(r'[0-9]+',str(lat)))[:5]
        strlon = ''.join(re.findall(r'[0-9]+',str(lon)))[:5]
        geo_id = ''.join([strlat,strlon])
        geo_data = [geo_id,tdict['geo']['type'],lat,lon]
        c.execute("INSERT OR IGNORE INTO Geo VALUES (?,?,?,?)", geo_data)

      user_data = [tdict['user']['id'],tdict['user']['name'],tdict['user']['screen_name'],
                          tdict['user']['description'],tdict['user']['friends_count']]
      tweet_data = [tdict['created_at'],tdict['id_str'],tdict['text'],tdict['source'],tdict['in_reply_to_user_id'],
                    tdict['in_reply_to_screen_name'],tdict['in_reply_to_status_id'],tdict['retweet_count'],
                    tdict['contributors'],tdict['user']['id'],geo_id]
      
      c.execute("INSERT OR IGNORE INTO User VALUES (?,?,?,?,?)", user_data)
      c.execute("INSERT OR IGNORE INTO Tweet VALUES (?,?,?,?,?,?,?,?,?,?,?)", tweet_data)
    except ValueError:
      pass

web250.close()

timeinsert250 = time.time() - start_time
print("1-b")
print("Time Insert 250000 =", timeinsert250)

q = c.execute("SELECT COUNT(*) FROM User;")
print(q.fetchall())

q = c.execute("SELECT COUNT(*) FROM Tweet;")
print(q.fetchall())

q = c.execute("SELECT COUNT(*) FROM Geo;")
print(q.fetchall())

c.execute('DROP TABLE User')
c.execute(createtbl_user)
c.execute('DROP TABLE Geo')
c.execute(createtbl_geo)
c.execute('DROP TABLE Tweet')
c.execute(createtbl_tweet)

conn.commit()
conn.close()


conn = sqlite3.connect('dsc450.db')
c = conn.cursor()
web1500 = urllib.request.urlopen("https://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt")

start_time = time.time()
for i in range(1500000):
    try:
      tdict = json.loads(web1500.readline().decode('utf8'))
      
      if tdict['geo'] is None:
        geo_id = None
         
      else:
        lat = tdict['geo']['coordinates'][0]
        lon = tdict['geo']['coordinates'][1]
        strlat = ''.join(re.findall(r'[0-9]+',str(lat)))[:5]
        strlon = ''.join(re.findall(r'[0-9]+',str(lon)))[:5]
        geo_id = ''.join([strlat,strlon])
        geo_data = [geo_id,tdict['geo']['type'],lat,lon]
        c.execute("INSERT OR IGNORE INTO Geo VALUES (?,?,?,?)", geo_data)

      user_data = [tdict['user']['id'],tdict['user']['name'],tdict['user']['screen_name'],
                          tdict['user']['description'],tdict['user']['friends_count']]
      tweet_data = [tdict['created_at'],tdict['id_str'],tdict['text'],tdict['source'],tdict['in_reply_to_user_id'],
                    tdict['in_reply_to_screen_name'],tdict['in_reply_to_status_id'],tdict['retweet_count'],
                    tdict['contributors'],tdict['user']['id'],geo_id]
      
      c.execute("INSERT OR IGNORE INTO User VALUES (?,?,?,?,?)", user_data)
      c.execute("INSERT OR IGNORE INTO Tweet VALUES (?,?,?,?,?,?,?,?,?,?,?)", tweet_data)
    except ValueError:
      pass

web1500.close()

timeinsert1500 = time.time() - start_time
print("Time Insert 1500000 =", timeinsert1500)

q = c.execute("SELECT COUNT(*) FROM User;")
print(q.fetchall())

q = c.execute("SELECT COUNT(*) FROM Tweet;")
print(q.fetchall())

q = c.execute("SELECT COUNT(*) FROM Geo;")
print(q.fetchall())

print()

c.execute('DROP TABLE User')
c.execute(createtbl_user)
c.execute('DROP TABLE Geo')
c.execute(createtbl_geo)
c.execute('DROP TABLE Tweet')
c.execute(createtbl_tweet)

conn.commit()
conn.close()



#1-c
#----------------------------------------------------------------------------------------------------
conn = sqlite3.connect('dsc450.db')
c = conn.cursor()
tweets250 = open('tweets250.txt','r')

start_time = time.time()
for i in range(250000):
    try:
      tdict = json.loads(tweets250.readline())
      
      if tdict['geo'] is None:
        geo_id = None
         
      else:
        lat = tdict['geo']['coordinates'][0]
        lon = tdict['geo']['coordinates'][1]
        strlat = ''.join(re.findall(r'[0-9]+',str(lat)))[:5]
        strlon = ''.join(re.findall(r'[0-9]+',str(lon)))[:5]
        geo_id = ''.join([strlat,strlon])
        geo_data = [geo_id,tdict['geo']['type'],lat,lon]
        c.execute("INSERT OR IGNORE INTO Geo VALUES (?,?,?,?)", geo_data)

      user_data = [tdict['user']['id'],tdict['user']['name'],tdict['user']['screen_name'],
                          tdict['user']['description'],tdict['user']['friends_count']]
      tweet_data = [tdict['created_at'],tdict['id_str'],tdict['text'],tdict['source'],tdict['in_reply_to_user_id'],
                    tdict['in_reply_to_screen_name'],tdict['in_reply_to_status_id'],tdict['retweet_count'],
                    tdict['contributors'],tdict['user']['id'],geo_id]
      
      c.execute("INSERT OR IGNORE INTO User VALUES (?,?,?,?,?)", user_data)
      c.execute("INSERT OR IGNORE INTO Tweet VALUES (?,?,?,?,?,?,?,?,?,?,?)", tweet_data)
    except ValueError:
      pass

tweets250.close()
timeread250 = time.time() - start_time
print("1-c")
print("Time Read 250000 =", timeread250)

q = c.execute("SELECT COUNT(*) FROM User;")
print(q.fetchall())

q = c.execute("SELECT COUNT(*) FROM Tweet;")
print(q.fetchall())

q = c.execute("SELECT COUNT(*) FROM Geo;")
print(q.fetchall())

c.execute('DROP TABLE User')
c.execute(createtbl_user)
c.execute('DROP TABLE Geo')
c.execute(createtbl_geo)
c.execute('DROP TABLE Tweet')
c.execute(createtbl_tweet)
conn.commit()
conn.close()

conn = sqlite3.connect('dsc450.db')
c = conn.cursor()
tweets1500 = open('tweets1500.txt','r')

start_time = time.time()
for i in range(1500000):
    try:
      tdict = json.loads(tweets1500.readline())
      
      if tdict['geo'] is None:
        geo_id = None
         
      else:
        lat = tdict['geo']['coordinates'][0]
        lon = tdict['geo']['coordinates'][1]
        strlat = ''.join(re.findall(r'[0-9]+',str(lat)))[:5]
        strlon = ''.join(re.findall(r'[0-9]+',str(lon)))[:5]
        geo_id = ''.join([strlat,strlon])
        geo_data = [geo_id,tdict['geo']['type'],lat,lon]
        c.execute("INSERT OR IGNORE INTO Geo VALUES (?,?,?,?)", geo_data)

      user_data = [tdict['user']['id'],tdict['user']['name'],tdict['user']['screen_name'],
                          tdict['user']['description'],tdict['user']['friends_count']]
      tweet_data = [tdict['created_at'],tdict['id_str'],tdict['text'],tdict['source'],tdict['in_reply_to_user_id'],
                    tdict['in_reply_to_screen_name'],tdict['in_reply_to_status_id'],tdict['retweet_count'],
                    tdict['contributors'],tdict['user']['id'],geo_id]
      
      c.execute("INSERT OR IGNORE INTO User VALUES (?,?,?,?,?)", user_data)
      c.execute("INSERT OR IGNORE INTO Tweet VALUES (?,?,?,?,?,?,?,?,?,?,?)", tweet_data)
    except ValueError:
      pass

tweets1500.close()
timeread1500 = time.time() - start_time
print("Time Read 1500000 =", timeread1500)

q = c.execute("SELECT COUNT(*) FROM User;")
print(q.fetchall())

q = c.execute("SELECT COUNT(*) FROM Tweet;")
print(q.fetchall())

q = c.execute("SELECT COUNT(*) FROM Geo;")
print(q.fetchall())

print()

conn.commit()
conn.close()


#1-d
#----------------------------------------------------------------------------------------------------
conn = sqlite3.connect('dsc450.db')
c = conn.cursor()
tweets250 = open('tweets250.txt','r')
tweets1500 = open('tweets1500.txt','r')

geo_data = []
user_data = []
tweet_data = []

start_time = time.time()
for i in range(250000):
    try:
      tdict = json.loads(tweets250.readline())
      
      if tdict['geo'] is None:
        geo_id = None
         
      else:
        lat = tdict['geo']['coordinates'][0]
        lon = tdict['geo']['coordinates'][1]
        strlat = ''.join(re.findall(r'[0-9]+',str(lat)))[:5]
        strlon = ''.join(re.findall(r'[0-9]+',str(lon)))[:5]
        geo_id = ''.join([strlat,strlon])
        geo_data.append([geo_id,tdict['geo']['type'],lat,lon])
        if len(geo_data) == 200 or i == 249999:
            c.executemany("INSERT OR IGNORE INTO Geo VALUES (?,?,?,?)", geo_data)
            geo_data = []
    
      user_data.append([tdict['user']['id'],tdict['user']['name'],tdict['user']['screen_name'],
                          tdict['user']['description'],tdict['user']['friends_count']])
      if len(user_data) == 7500 or i == 249999:
         c.executemany("INSERT OR IGNORE INTO User VALUES (?,?,?,?,?)", user_data)
         user_data = []
    
      tweet_data.append([tdict['created_at'],tdict['id_str'],tdict['text'],tdict['source'],tdict['in_reply_to_user_id'],
                    tdict['in_reply_to_screen_name'],tdict['in_reply_to_status_id'],tdict['retweet_count'],
                    tdict['contributors'],tdict['user']['id'],geo_id])
      if len(tweet_data) == 7500 or i == 249999:
         c.executemany("INSERT OR IGNORE INTO Tweet VALUES (?,?,?,?,?,?,?,?,?,?,?)", tweet_data)
         tweet_data = []
    
    except ValueError:
      pass

timebatch250 = time.time() - start_time
print("1-d")
print("Time Batch 250000 =", timebatch250)

q = c.execute("SELECT COUNT(*) FROM User;")
print(q.fetchall())

q = c.execute("SELECT COUNT(*) FROM Tweet;")
print(q.fetchall())

q = c.execute("SELECT COUNT(*) FROM Geo;")
print(q.fetchall())

c.execute('DROP TABLE User')
c.execute(createtbl_user)
c.execute('DROP TABLE Geo')
c.execute(createtbl_geo)
c.execute('DROP TABLE Tweet')
c.execute(createtbl_tweet)
conn.commit()
conn.close()

conn = sqlite3.connect('dsc450.db')
c = conn.cursor()
geo_data = []
user_data = []
tweet_data = []

start_time = time.time()
for i in range(1500000):
    try:
      tdict = json.loads(web1500.readline())
      
      if tdict['geo'] is None:
        geo_id = None
         
      else:
        lat = tdict['geo']['coordinates'][0]
        lon = tdict['geo']['coordinates'][1]
        strlat = ''.join(re.findall(r'[0-9]+',str(lat)))[:5]
        strlon = ''.join(re.findall(r'[0-9]+',str(lon)))[:5]
        geo_id = ''.join([strlat,strlon])
        geo_data = [geo_id,tdict['geo']['type'],lat,lon]
        if len(geo_data) == 200 or i == 1499999:
            c.executemany("INSERT OR IGNORE INTO Geo VALUES (?,?,?,?)", geo_data)
            geo_data = []
    
      user_data.append([tdict['user']['id'],tdict['user']['name'],tdict['user']['screen_name'],
                          tdict['user']['description'],tdict['user']['friends_count']])
      if len(user_data) == 7500 or i == 1499999:
         c.executemany("INSERT OR IGNORE INTO User VALUES (?,?,?,?,?)", user_data)
         user_data = []
    
      tweet_data.append([tdict['created_at'],tdict['id_str'],tdict['text'],tdict['source'],tdict['in_reply_to_user_id'],
                    tdict['in_reply_to_screen_name'],tdict['in_reply_to_status_id'],tdict['retweet_count'],
                    tdict['contributors'],tdict['user']['id'],geo_id])
      if len(tweet_data) == 7500 or i == 1499999:
         c.executemany("INSERT OR IGNORE INTO Tweet VALUES (?,?,?,?,?,?,?,?,?,?,?)", tweet_data)
         tweet_data = []
    
    except ValueError:
      pass

timebatch1500 = time.time() - start_time
print("Time Batch 1500000 =", timebatch1500)

q = c.execute("SELECT COUNT(*) FROM User;")
print(q.fetchall())

q = c.execute("SELECT COUNT(*) FROM Tweet;")
print(q.fetchall())

q = c.execute("SELECT COUNT(*) FROM Geo;")
print(q.fetchall())

print()

tweets250.close()
tweets1500.close()
conn.commit()
conn.close()
