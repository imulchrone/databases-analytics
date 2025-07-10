import numpy as np
import pandas as pd

#Load data into dataframe
employee = pd.read_csv('Employee.txt', names = ['First','Middle','Last','ID','DOB','Address','City','State','Sex','Salary','SupervisorID','Group'])
print(employee)

print('\n')

#1-a
#Find all male employees
print('Male Employees:')
print(employee.loc[employee['Sex'] == 'M'])

print('\n')

#1-b
#Find highest salary for female employees
print('Highest Salary (Female):')
employeeF = employee.loc[employee['Sex'] == 'F']
print(employeeF['Salary'].max())

print('\n')

#1-c
#Select all salaries grouped by middle initial
print('Salaries by Middle Initial:')
print(employee.groupby('Middle')['Salary'].apply(list).reset_index(name='Salary'))