"""Create deterministic CSV/JSON fixtures, including deliberate rejected rows."""
import csv, json, random
from datetime import datetime, timedelta
from pathlib import Path

def generate(root: str = "data/raw") -> None:
    out = Path(root); out.mkdir(parents=True, exist_ok=True)
    customers = []
    cities = [("Austin", "TX"), ("Seattle", "WA"), ("Boston", "MA"), ("Chicago", "IL")]
    for i in range(1, 31):
        city, state = cities[(i-1) % len(cities)]
        customers.append(dict(customer_id=f"C{i:03}", first_name=f"First{i}", last_name=f"Customer{i}", email=f"c{i}@example.test", signup_date=f"2024-{(i%12)+1:02}-01", city=city, state=state, country="US"))
    products = [dict(product_id=f"P{i:03}", product_name=n, category=c, unit_price=p, cost=round(p*.55,2)) for i,(n,c,p) in enumerate([("Trail Bottle","Outdoor",24.5),("Desk Lamp","Home",39.0),("Wireless Mouse","Electronics",29.99),("Yoga Mat","Fitness",34.0),("Coffee Grinder","Home",59.0),("USB Hub","Electronics",22.0)],1)]
    rng = random.Random(17); orders=[]; payments=[]
    start=datetime(2025,1,1,9)
    for i in range(1,121):
        customer=customers[(i*7)%len(customers)]; product=products[(i*5)%len(products)]
        qty=1+(i%4); discount=0.1 if i%9==0 else 0.0
        city,state=cities[(i*3)%len(cities)]
        row=dict(order_id=f"O{i:04}",customer_id=customer["customer_id"],product_id=product["product_id"],order_ts=(start+timedelta(days=i%300,hours=i%12)).strftime("%Y-%m-%d %H:%M:%S"),quantity=qty,unit_price=product["unit_price"],discount=discount,city=city,state=state,country="US")
        orders.append(row)
        if i <= 118:
            payments.append(dict(payment_id=f"PAY{i:04}",order_id=row["order_id"],payment_ts=row["order_ts"],amount=round(qty*product["unit_price"]*(1-discount),2),method=["card","paypal","gift_card"][i%3],status="paid"))
    # Invalid price, missing key, orphan customer/product, and duplicate order exercise DQ rules.
    orders += [dict(orders[0], order_id="O9001", unit_price=-5.0), dict(orders[1], order_id="O9002", customer_id="C999"), dict(orders[2], order_id="O9003", product_id="P999"), dict(orders[3], order_id="O9004", quantity=None), dict(orders[4])]
    customers.append(dict(customer_id="C9999", first_name="", last_name="", email="", signup_date="2025-01-01", city="", state="", country="US"))
    with (out/"customers.csv").open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=customers[0].keys()); w.writeheader(); w.writerows(customers)
    with (out/"products.csv").open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=products[0].keys()); w.writeheader(); w.writerows(products)
    (out/"orders.json").write_text("\n".join(json.dumps(x) for x in orders)+"\n",encoding="utf-8")
    (out/"payments.json").write_text("\n".join(json.dumps(x) for x in payments)+"\n",encoding="utf-8")
    print(f"Wrote {len(customers)} customers, {len(products)} products, {len(orders)} orders, {len(payments)} payments to {out}")

if __name__ == "__main__": generate()

