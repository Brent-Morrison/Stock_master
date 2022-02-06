# https://stackoverflow.com/questions/10823033/sending-arguments-from-batch-file-to-python-script

import sys
import datetime as dt
import csv

print()

# Arg 1
print('Argument 1 (integer string)')
print('Raw: ',sys.argv[1], ', Type: ',type(sys.argv[1]))

print()

print('Argument 1 (integer string converted)')
print('Raw: ',int(sys.argv[1]), ', Type: ',type(int(sys.argv[1])))

print()

# Arg 2
print('Argument 2 (unquoted date string)')
print('Raw: ',sys.argv[2], ', Type: ',type(sys.argv[2]))

print()

arg2dt = dt.datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
print('Argument 2 (unquoted date string converted to date)')
print('Raw: ',arg2dt, ', Type: ',type(arg2dt))

print()

# Arg3
print('Argument 3 (character string representing boolean)')
print('Raw: ',sys.argv[3], ', Type: ',type(sys.argv[3]))

print()

# Convert to boolean
# Initialise
st2bool = True
if sys.argv[3].lower() == 't':
    st2bool = True
elif sys.argv[3].lower() == 'f':
    st2bool = False

print('Argument 3 (character string converted to boolean)')
print('Raw: ',st2bool, ', Type: ',type(st2bool))

print()

# Write all to csv
lst = [sys.argv[1], sys.argv[2], sys.argv[3]]

print(lst)

with open('C:/Users/brent/Documents/VS_Code/postgres/postgres/test/sys.argv_py_test.csv', 'w') as f:
    write = csv.writer(f) 
    write.writerow(lst)
