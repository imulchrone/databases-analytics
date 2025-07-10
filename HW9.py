import sqlite3
import urllib.request
import json
import re
import time
import pandas as pd
import matplotlib.pyplot as plt

#Part 1
conn = sqlite3.connect('dsc450.db')
c = conn.cursor()

createtbl = """
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
c.execute(createtbl)

createtbl3 = """
CREATE TABLE Geo
(
    id NUMBER(10) PRIMARY KEY,
    type VARCHAR(8),
    latitude VARCHAR(13),
    longitude VARCHAR(13)
);
"""
c.execute('DROP TABLE Geo')
c.execute(createtbl3)

createtbl2 = """
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
c.execute(createtbl2)


webFD = urllib.request.urlopen("https://dbgroup.cdm.depaul.edu/DSC450/Module7.txt")
allTweets = webFD.readlines()

errors = open('errors.txt','wb')

for tweet in allTweets:
   try:
      tdict = json.loads(tweet.decode('utf8'))
      
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


#1-a
start_time = time.time()
q = c.execute("""SELECT * FROM Tweet WHERE
                 id_str LIKE '%78%'
                 OR id_str LIKE '%8%'
                 OR id_str LIKE '%8791%';""")
query_time = time.time() - start_time
# print(q.fetchall())
print("Query Time:", query_time, "seconds")

q = c.execute("SELECT * FROM Tweet;")
tweets = q.fetchall()

df_tweets = pd.DataFrame(data = tweets, columns = ['created_at','id_str','text','source',
    'in_reply_to_user_id','in_reply_to_screen_name','in_reply_to_status_id','retweet_count',
    'contributors','user_id','geo_id'])

print()

#1-b
start_time = time.time()
result = df_tweets[df_tweets['id_str'].str.contains("78|8|8791")==True]
query_time = time.time() - start_time
# print(result)
print("Query Time:", query_time, "seconds")

print()

#1-c
start_time = time.time()
q = c.execute("""SELECT Count(*) FROM
                  (SELECT DISTINCT friends_count FROM User);""")
query_time = time.time() - start_time
# print(q.fetchall())
print("Query Time:", query_time, "seconds")

print()

q = c.execute("SELECT * FROM User;")
users = q.fetchall()

df_user = pd.DataFrame(data=users, columns = ['id','name','screen_name','decription','friends_count'])

#1-d
start_time = time.time()
distinct_friends_count = df_user['friends_count'].nunique()
query_time = time.time() - start_time
# print(distinct_friends_count)
print("Query Time:", query_time, "seconds")

#1-e
tweet_lens = list(map(lambda x : len(x), df_tweets['text'][:90].tolist()))
user_lens = list(map(lambda x : len(x), df_user['name'][:90].tolist()))

plt.scatter(user_lens, tweet_lens)
plt.xlabel('User Name Length')
plt.ylabel('Tweet Length')
plt.title('Twitter Name Length v Tweet Length')
plt.show()


#Part 2
#2-a
c.execute("CREATE INDEX UserIndex ON Tweet(user_id);")

#2-b
c.execute("CREATE INDEX FriendsName ON User(friends_count, screen_name);")

#2-c

createtblmv = """
CREATE TABLE TweetMV AS
SELECT * FROM Tweet WHERE
id_str LIKE '%78%'
OR id_str LIKE '%8%'
OR id_str LIKE '%8791%';
"""
c.execute('DROP TABLE TweetMV')
c.execute(createtblmv)

# q = c.execute("SELECT * FROM TweetMV LIMIT 5;")
# print(q.fetchall())


conn.commit()
conn.close()
errors.close()