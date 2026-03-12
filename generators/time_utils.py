import random
from datetime import timedelta
from config import START_DATE, END_DATE

def random_date():
    delta = END_DATE - START_DATE
    return START_DATE + timedelta(days=random.randint(0, delta.days))
