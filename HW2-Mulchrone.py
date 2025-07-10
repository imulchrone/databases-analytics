#Ian Mulchrone
#DSC 450
#Homework 2

#validateInsert : string -> string
#Check if the given string is a valid SQL insert statement

def validateInsert(s):
    #check for 'INSERT INTO' and ';'
    if s[:11] == 'INSERT INTO' and s[-1] == ';':
        slist = s.split()
        table = slist[2] #table name
        values = ' '.join((slist[-3],slist[-2],slist[-1])) #combine values
        values = values.strip(';') #strip ;
        print('Inserting ' + values + " into " + table + " table")
    else:
        print('Invalid insert')

validateInsert("INSERT INTO Students VALUES (1, 'Jane', 'B+');")
validateInsert("INSERT INTO Students VALUES (1, 'Jane', 'B+')")
validateInsert("INSERT Students VALUES (1, 'Jane', B+);")
validateInsert("INSERT INTO Phones VALUES (42, '312-676-1213');")