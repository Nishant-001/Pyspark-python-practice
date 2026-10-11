# Q7 — Generator

# Write a generator:

# read_large_file(path)

# that yields one line at a time.

# Then explain why this is better than:

# lines = open(path).readlines()

# for a huge file.
file=open("python/large_file.txt",'r')

def process(file):
    for line in file:
        yield line
        

res=0

for line in process(file):
    if "ERROR" in line:
        res=res+1
        
print(res)


file=open("path?to_")
