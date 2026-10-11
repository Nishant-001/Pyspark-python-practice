# Q3 — Second highest

# Given:

# nums = [10, 5, 20, 20, 8, 15, 10]

# Find the second-highest distinct number without using sort() or sorted().
import copy

nums = [10, 5, 20, 20, 8, 15, 10]
first,second=float("-inf"),float("-inf")
for i in nums:
    if first>i>second:
        second=i
    elif i>first:
        second=first
        first=i
        
    
print(first,second)

a = [[1, 2], [3, 4]]

b = copy.deepcopy(a)

b[0].append(99)

print(a)
print(b)

first=float("-inf")
second=float("-inf")
