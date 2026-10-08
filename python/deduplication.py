# Q1 — Deduplication

# Given:

# records = [
#     {"id": 1, "name": "A"},
#     {"id": 2, "name": "B"},
#     {"id": 1, "name": "A"},
#     {"id": 3, "name": "C"},
#     {"id": 2, "name": "B"}
# ]

# Remove duplicates while preserving the original order.

records = [
    {"id": 1, "name": "A"},
    {"id": 2, "name": "B"},
    {"id": 1, "name": "A"},
    {"id": 3, "name": "C"},
    {"id": 2, "name": "B"}
]

di={}
for record in records:
    key=record["id"]
    value=record["name"]
    di[key]=value
    
result=[{"id":key, "name":value} for key,value in di.items()]
print(result)

seen=set()
result1=[]

for record in records:
    if record["id"] not in seen:
        seen.add(record["id"])
        result1.append(record)
        
print(result1)