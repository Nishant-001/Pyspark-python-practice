# Q5 — Group records

# Given:

# employees = [
#     {"name": "A", "dept": "IT", "salary": 50000},
#     {"name": "B", "dept": "HR", "salary": 40000},
#     {"name": "C", "dept": "IT", "salary": 70000},
#     {"name": "D", "dept": "HR", "salary": 60000}
# ]

# Group employees by department.

# Expected structure:

# {
#     "IT": [...],
#     "HR": [...]
# }

employees = [
    {"name": "A", "dept": "IT", "salary": 50000},
    {"name": "B", "dept": "HR", "salary": 40000},
    {"name": "C", "dept": "IT", "salary": 70000},
    {"name": "D", "dept": "HR", "salary": 60000}
]
res={}
for item in employees:
    res[item["dept"]]=res.get(item["dept"],[])+[(item)]
    
print(res)