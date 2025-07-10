import sqlite3

fd = open('animal.txt', 'r')
data = fd.readlines()
newData = []
for d in data:
    d = d.strip()
    line = d.split(', ')
    newData.append(line)

conn = sqlite3.connect('dsc450.db')

c = conn.cursor()
c.execute('DROP TABLE Animal')

createtbl = """
CREATE TABLE Animal
(
  AID       NUMBER(3, 0),
  AName      VARCHAR2(30) NOT NULL,
  ACategory VARCHAR2(18),

  TimeToFeed NUMBER(4,2),

  CONSTRAINT Animal_PK
    PRIMARY KEY(AID)
);
"""
c.execute(createtbl)

c.executemany("INSERT INTO Animal VALUES (?, ?, ?, ?)", newData)
q = c.execute("SELECT COUNT(*) FROM Animal")
print(q.fetchone())
conn.commit()
conn.close()