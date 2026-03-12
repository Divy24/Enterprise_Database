import pandas as pd
import random
from datetime import timedelta
from generators.time_utils import random_date

def generate_orders(subs_df):
    orders = []
    order_id_counter = 1
    for _, sub in subs_df.iterrows():
        # Generate ~4 orders per subscription to hit 120k target from 30k subs
        order_count = random.randint(1, 4)
        if sub["status"] == "cancelled":
             order_count = random.randint(1, 2)

        for _ in range(order_count):
            order_time = random_date()
            orders.append({
                "order_id": order_id_counter,
                "user_id": sub["user_id"],
                "order_status": "paid",
                "order_time": order_time
            })
            order_id_counter += 1
    return pd.DataFrame(orders)

def generate_order_items(orders_df, products_df):
    rows = []
    item_id_counter = 1
    prod_ids = products_df["product_id"].tolist()
    
    for _, order in orders_df.iterrows():
        # 1-3 items per order
        count = random.randint(1, 3)
        for _ in range(count):
            rows.append({
                "order_item_id": item_id_counter,
                "order_id": order["order_id"],
                "product_id": random.choice(prod_ids),
                "quantity": random.randint(1, 5),
                "unit_price": random.randint(100, 5000)
            })
            item_id_counter += 1
    return pd.DataFrame(rows)

def generate_invoices(orders_df):
    invoices = []
    for idx, order in orders_df.iterrows():
        # In real world, sum of items. Here random for speed.
        amount = random.randint(500, 15000)
        invoices.append({
            "invoice_id": idx + 1,
            "order_id": order["order_id"],
            "total_amount": amount,
            "issued_at": order["order_time"]
        })
    return pd.DataFrame(invoices)

def generate_payments(invoices_df):
    payments = []
    for idx, inv in invoices_df.iterrows():
        payments.append({
            "payment_id": idx + 1,
            "invoice_id": inv["invoice_id"],
            "amount": inv["total_amount"],
            "method": random.choice(["card", "upi", "netbanking"]),
            "paid_at": inv["issued_at"] + timedelta(hours=random.randint(1, 48))
        })
    return pd.DataFrame(payments)

def generate_refunds(payments_df):
    rows = []
    refund_id = 1
    for _, pay in payments_df.iterrows():
        if random.random() < 0.05: # 5% refund rate
            rows.append({
                "refund_id": refund_id,
                "payment_id": pay["payment_id"],
                "amount": pay["amount"], # Full refund
                "refunded_at": pay["paid_at"] + timedelta(days=random.randint(1, 7))
            })
            refund_id += 1
    return pd.DataFrame(rows)
