import csv
import sqlite3

fd = open('Public_Chauffeurs_Short_hw3.csv', 'r')
reader = csv.reader(fd)

chauffers = []

for row in reader:
    row = list(map(lambda x: None if x == 'NULL' or x == '' else x, row))
    if ',' in row[7]:
        namesplit = row[7].split(',')
        if 'JR' in namesplit[1]:
            fullname = ''.join(namesplit)
        else:
            cleanname = list(filter(lambda x : x != '', namesplit[1].split(' ')))
            fullname = ' '.join(cleanname) + ' ' + namesplit[0]
        
    else:
        cleanname = list(filter(lambda x : x != '', row[7].split(' ')))
        fullname = ' '.join(cleanname)
    
    newrow = [row[x] if x != 7 else fullname for x in range(len(row))]
    chauffers.append(newrow)

fd.close()
chauffers.pop()


conn = sqlite3.connect('dsc450.db')
c = conn.cursor()

c.execute('DROP TABLE Chauffers')

createtbl = """
CREATE TABLE Chauffers
(
    LicenseNumber VARCHAR(14),
    Renewed DATE,
    Status VARCHAR(15),
    StatusDate DATE,
    DriverType VARCHAR(16),
    LicenseType VARCHAR(9),
    OriginalIssueDate DATE,
    Name VARCHAR(32),
    Sex VARCHAR(6),
    ChaufferCity VARCHAR(18),
    ChaufferState VARCHAR(2),
    RecordNumber VARCHAR(11) PRIMARY KEY
);
"""
c.execute(createtbl)

c.executemany("INSERT INTO Chauffers VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", chauffers)

fd.close()
q = c.execute("SELECT COUNT(*) FROM Chauffers")
print(q.fetchall())

issuedatenull = c.execute("""
                          SELECT COUNT(*) FROM Chauffers
                          WHERE OriginalIssueDate IS NULL
                          """)
print(issuedatenull.fetchall())


fd = open('Module5.txt', encoding='utf8')
data = fd.readline()

data = data.split('         EndOfTweet          ')

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
    contributors NUMBER(4)
);
"""
c.execute('DROP TABLE Tweets')
c.execute(createtbl2)

import json

for d in data:
    tweet = json.loads(d)
    tweet_data = [tweet['created_at'], tweet['id_str'], tweet['text'], tweet['source'],
                  tweet['in_reply_to_user_id'], tweet['in_reply_to_screen_name'],
                  tweet['in_reply_to_status_id'], tweet['retweet_count'], tweet['contributors']]

    c.execute("INSERT INTO Tweets VALUES (?,?,?,?,?,?,?,?,?)", tweet_data)

q = c.execute("SELECT * FROM Tweets")
print(q.fetchall())

conn.commit()
conn.close()