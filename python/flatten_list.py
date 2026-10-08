# Q4 — Flatten nested list

# Given:

# data = [1, [2, 3], [4, [5, 6]], 7, [[8, 9]]]

# Flatten it completely:

# [1,2,3,4,5,6,7,8,9]

list1 = [1, [2, 3], [4, [5, 6]], 7, [[8, 9]]]
def rec(data):
    li=[]
    
    for i in data:
        if type(i) is not list:
            li.append(i)
        else:
            ret=rec(i)
            li=li+ret
            
    return li


ans=rec(list1)
print(ans)

        
            

    
    