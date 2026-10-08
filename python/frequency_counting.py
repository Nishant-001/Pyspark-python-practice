# Q2 — Frequency counting

# Given:

# nums = [1, 2, 3, 2, 4, 1, 2, 5, 3, 1]

from collections import Counter

nums = [1, 2, 3, 2, 4, 1, 2, 5, 3, 1]

print (Counter(nums))
di={}
for num in nums:
    di[num]=di.get(num,0)+1
    
print(di)
