import pandas as pd

file = open('records.txt','r')
lines = [line.rstrip() for line in file]

colnames = lines[0].split(', ')

#clean rows
records = []
lines[1] = lines[1].split(', ')
records.append(list(map(lambda x: None if x == 'NULL' else x, lines[1])))
for i in range(2,len(lines)):
    lines[i] = lines[i].split(', ')
    name = ', '.join(lines[i][1:3])
    del lines[i][2]
    lines[i][1] = name
    row = list(map(lambda x: None if x == 'NULL' else x, lines[i]))
    records.append(row)


course = {}

df = pd.DataFrame(records, columns = colnames)
df['GradYear'] = pd.to_numeric(df['GradYear'], errors='coerce').astype('Int64')
df['Credits'] = pd.to_numeric(df['Credits'], errors='coerce').astype('Int64')

for i in range(len(df)):
    if df['CName'][i] is not None and df['CName'][i] not in course:
        course[df['CName'][i]] = (df['Department'][i], df['Credits'][i])


for i in range(len(df)):
    if df['CName'][i] is not None:
        cvalues = course[df['CName'][i]]
        if df['Department'][i] != cvalues[0]:
            print('Row', i, df['CName'][i],'->', df['Department'][i], 'CName -> Department Violated')
        elif df['Credits'][i] != cvalues[1]:
            print('Row', i, df['CName'][i],'->', df['Credits'][i], 'CName -> Credits Violated')

# print(df)
import time

start_time = time.time()
grouped = df.groupby('Department').agg({
                                        'GradYear': ['min','max'],
                                        'Credits': 'mean'
})

result = grouped.dropna()
print(result)
print("Query Python %s seconds" % (time.time() - start_time))
