import numpy as np
import pandas as pd

#Part 1
#1-a
def randomlist(x):
    nums = []
    for i in range(x):
        nums.append(np.random.randint(39,101))
    return nums

# print(randomlist(10))

#1-b
numbers = pd.Series(randomlist(90))
filter = numbers < 55
print(len(numbers[filter]))

#1-c
num_array = np.array(numbers)
num_array = np.reshape(num_array,(9,10))
new_array = np.where(num_array >= 56, 100, num_array)
print(new_array)


#Part 2
import sqlite3
import urllib.request
import json

#2-a
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

createtbl2 = """
CREATE TABLE Tweets
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

    FOREIGN KEY(user_id) REFERENCES User(id)
);
"""
c.execute(createtbl2)

#2-b
webFD = urllib.request.urlopen("https://dbgroup.cdm.depaul.edu/DSC450/Module7.txt")
allTweets = webFD.readline()

errors = open('errors.txt','wb')

for tweet in allTweets:
   try:
      tdict = json.loads(tweet.decode('utf8'))
      user_data = [tdict['user']['id'],tdict['user']['name'],tdict['user']['screen_name'],
                          tdict['user']['description'],tdict['user']['friends_count']]
      c.execute("INSERT OR IGNORE INTO User VALUES (?,?,?,?,?)", user_data)
   except ValueError:
      # Handle the problematic tweet, which in your case would require writing it to another file
      errors.write(tweet)
    #   print (tweet)

q = c.execute("SELECT * FROM User LIMIT 5;")
print(q.fetchall())

conn.commit()
conn.close()
errors.close()

#3-b
import re

cc = '1234-5678-9012-3456'
cc2 = '1234567890123456'
cc3 = '9234-5678-9012-3456'

result1 = ''.join(re.findall(r'[0-9]+',cc))
result2 = ''.join(re.findall(r'[0-9]+',cc2))
result3 = ''.join(re.findall(r'[0-9]+',cc3))

if result1 == result2:
    print('Match')
else:
    print('No match')

if result1 == result3:
    print('Match')
else:
    print('No match')
