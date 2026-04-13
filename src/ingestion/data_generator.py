import random
import pandas as pd
from datetime import datetime, timedelta


def generate_transactions(n=1000):
    data = []

    for i in range(n):
        data.append({
            "txn_id": i,
            "account_id": random.randint(1000, 1100),
            "amount": round(random.uniform(100, 200000), 2),
            "transaction_type": random.choice(["DEBIT", "CREDIT"]),
            "location": random.choice(["India", "US", "UK"]),
            "timestamp": datetime.now() - timedelta(minutes=random.randint(0, 10000)),
            "is_fraud": 1 if random.random() < 0.05 else 0
        })

    return pd.DataFrame(data)