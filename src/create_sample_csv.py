from __future__ import annotations

import csv
from pathlib import Path

cols = [
    "order_id", "order_date", "status", "customer_id", "customer_name",
    "customer_phone", "customer_email", "city", "district", "delivery_type",
    "delivery_cost", "payment_method", "payment_status", "payment_amount",
    "currency", "total_amount", "items_json"
]

rows = []

# 700 Clean Rows
for i in range(1, 701):
    rows.append({
        "order_id": f"ORD-CLN-{i:05d}",
        "order_date": "2026-08-20T10:00:00",
        "status": "مؤكد",
        "customer_id": f"CUST-{i:05d}",
        "customer_name": f"العميل رقم {i}",
        "customer_phone": f"+967771{i:06d}"[:13],
        "customer_email": f"client_{i}@example.com",
        "city": "صنعاء",
        "district": "السبعين",
        "delivery_type": "سريع",
        "delivery_cost": "2000.0",
        "payment_method": "كاش",
        "payment_status": "تم الدفع",
        "payment_amount": "8000.0",
        "currency": "YER",
        "total_amount": "10000.0",
        "items_json": '[{"sku":"SKU-1","name":"Product A","qty":2,"unit_price":4000.0,"total":8000.0}]'
    })

# 200 Correctable Dirty Rows (Demonstrating all 9 Quality Rules)
for i in range(1, 201):
    rows.append({
        "order_id": f"ORD-DIRTY-{i:04d}",
        "order_date": "25/08/2026",
        "status": "مدفوع",
        "customer_id": f"CUST-D{i:04d}",
        "customer_name": f"  محمد  علي {i} ",
        "customer_phone": f"٠٠٩٦٧٧٧٢{i:06d}"[:15],
        "customer_email": f"user.{i}@@gmail...com",
        "city": " عدن ",
        "district": " المنصورة ",
        "delivery_type": " عادي ",
        "delivery_cost": "2,000.00 ريال يمني",
        "payment_method": " بطاقة ",
        "payment_status": "دفع",
        "payment_amount": "خمسة آلاف",
        "currency": "ر.ي",
        "total_amount": "7000",
        "items_json": '[{"sku":"ITEM-A","name":"Item A","qty":1,"unit_price":5000.0,"total":5000.0}]'
    })

# 100 Quarantine Rows (Severe Logical, Temporal & Structural Defects)
for i in range(1, 26):
    rows.append({
        "order_id": f"ORD-QUAR-DATE-{i:03d}",
        "order_date": "2025-02-31",
        "status": "مؤكد", "customer_id": f"CUST-Q{i}", "customer_name": "سامي", "customer_phone": "+967771122334",
        "customer_email": "test@test.com", "city": "صنعاء", "district": "الوحدة", "delivery_type": "سريع",
        "delivery_cost": "1000", "payment_method": "كاش", "payment_status": "تم الدفع", "payment_amount": "5000",
        "currency": "YER", "total_amount": "6000", "items_json": '[{"sku":"X","name":"X","qty":1,"unit_price":5000.0,"total":5000.0}]'
    })
    rows.append({
        "order_id": f"ORD-QUAR-JSON-{i:03d}",
        "order_date": "2026-08-20", "status": "مؤكد", "customer_id": f"CUST-Q{i}", "customer_name": "أحمد",
        "customer_phone": "+967771122335", "customer_email": "test@test.com", "city": "تعز", "district": "صالة",
        "delivery_type": "عادي", "delivery_cost": "1000", "payment_method": "كاش", "payment_status": "تم الدفع",
        "payment_amount": "5000", "currency": "YER", "total_amount": "6000",
        "items_json": "BROKEN_MALFORMED_JSON_TEXT"
    })
    rows.append({
        "order_id": "",
        "order_date": "2026-08-20", "status": "مؤكد", "customer_id": f"CUST-Q{i}", "customer_name": "خالد",
        "customer_phone": "+967771122336", "customer_email": "test@test.com", "city": "إب", "district": "المشنة",
        "delivery_type": "عادي", "delivery_cost": "1000", "payment_method": "كاش", "payment_status": "تم الدفع",
        "payment_amount": "5000", "currency": "YER", "total_amount": "6000",
        "items_json": '[{"sku":"Y","name":"Y","qty":1,"unit_price":5000.0,"total":5000.0}]'
    })
    rows.append({
        "order_id": f"ORD-QUAR-PRICE-{i:03d}",
        "order_date": "2026-08-20", "status": "مؤكد", "customer_id": f"CUST-Q{i}", "customer_name": "عمر",
        "customer_phone": "+967771122337", "customer_email": "test@test.com", "city": "الحديدة", "district": "الحوك",
        "delivery_type": "عادي", "delivery_cost": "1000", "payment_method": "كاش", "payment_status": "تم الدفع",
        "payment_amount": "5000", "currency": "YER", "total_amount": "6000",
        "items_json": '[{"sku":"Z","name":"Z","qty":1,"unit_price":null,"total":null}]'
    })

p = Path("data/orders_sample.csv")
p.parent.mkdir(parents=True, exist_ok=True)
with open(p, "w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    w.writerows(rows)

print(f"Generated {p} with {len(rows)} records ({p.stat().st_size / 1024:.1f} KB)")
