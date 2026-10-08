# Q6 — Large-file processing

# You have a 20 GB log file.

# Write Python code that counts the number of lines containing:

# ERROR

# Constraint: You cannot load the entire file into memory.
file=open("python/large_file.txt",'r')

def process(file):
    for line in file:
        yield line
        

res=0

for line in process(file):
    if "ERROR" in line:
        res=res+1
        
print(res)