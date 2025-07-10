import sqlite3

conn = sqlite3.connect('dsc450.db')
c = conn.cursor()

c.execute('DROP TABLE Student')
c.execute('DROP TABLE Course')
c.execute('DROP TABLE Grade')

studenttable = """
CREATE TABLE Student
(
    StudentID NUMBER PRIMARY KEY,
    Name VARCHAR(50),
    Address VARCHAR(25),
    GradYear NUMBER(4)
);"""

coursetable = """
CREATE TABLE Course
(
    CName VARCHAR(20) PRIMARY KEY,
    Department VARCHAR(20),
    Credits NUMBER(1)
);"""

gradetable = """
CREATE TABLE Grade
(
    CName VARCHAR(20),
    StudentID NUMBER,
    CGrade NUMBER(2,1),
    
    FOREIGN KEY (CName)
        REFERENCES Course(CName),
        
    FOREIGN KEY (StudentID)
        REFERENCES Student(StudentID)
);
"""

c.execute(studenttable)
c.execute(coursetable)
c.execute(gradetable)

inserts = '''
INSERT INTO Student VALUES(1, 'Smith, John S', 'Chicago IL', 2019);
INSERT INTO Student VALUES(2, 'Smith, Muriel K', 'Chicago IL', 2015);
INSERT INTO Student VALUES(3, 'Jones, Muriel L', 'Detroit MI', 2010);
INSERT INTO Student VALUES(4, 'Jacobs, Mark M', 'Milwaukee WI', 2004);
INSERT INTO Student VALUES(5, 'Jordan, Michael B', 'Chicago IL', 2009);

INSERT INTO Course VALUES('Calculus', 'Mathematics', 4);
INSERT INTO Course VALUES('US History', 'History', 4);
INSERT INTO Course VALUES('Painting', 'Art', 3);
INSERT INTO Course VALUES('Chemistry', 'Science', 4);

INSERT INTO Grade VALUES('Calculus', 1, 3.3);
INSERT INTO Grade VALUES('Calculus', 4, 3.0);
INSERT INTO Grade VALUES('US History', 2, 4.0);
INSERT INTO Grade VALUES('Painting', 1, 2.5);
INSERT INTO Grade VALUES('US History', 3, 3.0);
INSERT INTO Grade VALUES('Painting', 2, 3.6);'''

c.executescript(inserts)

# q = c.execute("SELECT * FROM Student")
# print(q.fetchall())

c.execute('DROP VIEW Records')

view = '''
CREATE VIEW Records AS
SELECT Student.StudentID, Name, Address, GradYear, Course.CName, CGrade, Department, Credits FROM Course
LEFT JOIN Grade ON Course.CName = Grade.CName
LEFT JOIN Student ON Student.StudentID = Grade.StudentID
UNION
SELECT Student.StudentID, Name, Address, GradYear, Course.CName, CGrade, Department, Credits FROM Student
LEFT JOIN Grade ON Student.StudentID = Grade.StudentID
LEFT JOIN Course ON Course.CName = Grade.CName;
'''

c.execute(view)

viewcount = c.execute('SELECT COUNT(*) FROM Records;')
print('Original view:', viewcount.fetchall())

c.execute("INSERT INTO Grade VALUES('US History',1,3.0);")

viewcount = c.execute('SELECT COUNT(*) FROM Records;')
print('After insert:', viewcount.fetchall())

records = c.execute("SELECT * FROM Records")

columns = list(map(lambda x: x[0], c.description))
columns = [description[0] for description in c.description]

file = open('records.txt','w')
file.write(', '.join(columns))
file.write("\n")

for record in records:
    record = list(map(lambda x: 'NULL' if x is None else str(x), record))
    file.write(', '.join(record))
    file.write("\n")

file.close()

import time

departments1 = '''
SELECT Department, MIN(GradYear), MAX(GradYear), AVG(Credits) FROM Course
JOIN Grade ON Course.CName = Grade.CName
JOIN Student ON Grade.StudentID = Student.StudentID
GROUP BY Department;'''

start_time = time.time()
group1 = c.execute(departments1)
print(group1.fetchall())
print("Query Normal %s seconds" % (time.time() - start_time))

departments2 = '''
SELECT Department, MIN(GradYear), MAX(GradYear), AVG(Credits) FROM Records
WHERE Department IS NOT NULL
GROUP BY Department
HAVING MIN(GradYear) IS NOT NULL;
'''

start_time = time.time()
group2 = c.execute(departments2)
print(group2.fetchall())
print("Query View %s seconds" % (time.time() - start_time))

conn.commit()
conn.close()
