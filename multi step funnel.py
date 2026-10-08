# Driver code — solve the logic inside count_funnel()

import pandas as pd

# Sample data from the question
data = [
    (1, "page_view",   "2023-01-01 10:00:00"),
    (2, "page_view",   "2023-01-01 10:05:00"),
    (3, "page_view",   "2023-01-01 10:10:00"),
    (4, "page_view",   "2023-01-01 10:15:00"),
    (5, "page_view",   "2023-01-01 10:20:00"),

    (1, "add_to_cart", "2023-01-01 10:30:00"),
    (2, "add_to_cart", "2023-01-01 10:35:00"),
    (3, "add_to_cart", "2023-01-01 10:40:00"),
    (4, "add_to_cart", "2023-01-01 10:45:00"),

    (1, "checkout",    "2023-01-01 11:00:00"),
    
    (1, "purchase",    "2023-01-01 11:30:00"),
    (2, "purchase",    "2023-01-01 11:35:00"),
]

user_events = pd.DataFrame(
    data,
    columns=["user_id", "event_name", "event_time"]
)

user_events["event_time"] = pd.to_datetime(user_events["event_time"])

# ============================================================
# YOUR SOLUTION
# ============================================================

def count_funnel(user_events):

    steps = ["page_view", "add_to_cart", "checkout", "purchase"]

    # Write your logic here
    #
    # Requirements:
    # 1. A user must complete every previous step.
    # 2. Steps must occur in chronological order.
    # 3. Count distinct users reaching each step.
    # 4. Calculate dropoff_rate from previous step.
    # 5. Return:
    #
    #    step | users_reached | dropoff_rate
    #
    # sorted in funnel order.
    df1=user_events.groupby("event_name")["user_id"].count()
    print(df1)
    pass


# ============================================================
# TEST
# ============================================================

result = count_funnel(user_events)

print(result)