#Ian Mulchrone
#DSC450 Homework 4

#Read in text file
fd = open('animal.txt', 'r')
data = fd.readlines()

#Clean data
animals = []
for d in data:
    d = d.strip()
    line = d.split(', ')
    animals.append(line)

print(animals)

# Find animal names that are not bears
for animal in animals:
    if "bear" not in animal[1]:
        print(animal[1])

# Find animals that are tigers and not common
for animal in animals:
    if "tiger" in animal[1] and animal[2] != 'common':
        print(animal[1])


# Read in text file
fd = open('data_module4_part2.txt', 'r')
data = fd.readlines()

#Clean data and replace 'NULL' with None
employee = []
position = []
for d in data:
    d = d.strip()
    row = d.split(', ')
    row = list(map(lambda x: None if x == 'NULL' else x, row))
    position.append(row[3:6])
    employee.append(row[0:4])

# print(position)
# print(employee)

import sqlite3

conn = sqlite3.connect('dsc450.db')
c = conn.cursor()

c.execute('DROP TABLE Position')
c.execute('DROP TABLE Employee')

createtbl1 = """
CREATE TABLE Position
(
    Name VARCHAR(15) PRIMARY KEY,
    Salary NUMBER(6),
    Assistant VARCHAR(6)
);
"""

createtbl2 = """
CREATE TABLE Employee
(
    First VARCHAR(5),
    Last VARCHAR(10),
    Address VARCHAR(20),
    Job VARCHAR(15),
    
    CONSTRAINT Employee_PK
        PRIMARY KEY(First, Last, Job),
    
    CONSTRAINT Employee_FK
        FOREIGN KEY(Job)
        REFERENCES Position(Name)
);
"""

c.execute(createtbl1)
c.execute(createtbl2)

#Populate tables accounting for null values
c.executemany("INSERT OR IGNORE INTO Position VALUES (?, ?, ?)", position)
c.executemany("INSERT OR IGNORE INTO Employee VALUES (?, ?, ?, ?)", employee)
q = c.execute("SELECT * FROM Position")
print(q.fetchall())
q = c.execute("SELECT * FROM Employee")
print(q.fetchall())

isnull = c.execute("SELECT * FROM Position WHERE Salary IS NULL")
print(isnull.fetchall())
conn.commit()
conn.close()
