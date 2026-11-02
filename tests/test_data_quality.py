"""Small pure-Python checks for generated fixture contracts and bad-row examples."""
import json
from pathlib import Path
from src.generate_data import generate

def test_generator_writes_all_input_formats(tmp_path):
    generate(str(tmp_path))
    assert (tmp_path / "customers.csv").exists()
    assert (tmp_path / "products.csv").exists()
    orders=[json.loads(x) for x in (tmp_path / "orders.json").read_text().splitlines()]
    payments=[json.loads(x) for x in (tmp_path / "payments.json").read_text().splitlines()]
    assert len(orders) > 100 and len(payments) > 100

def test_fixture_contains_expected_quality_failures(tmp_path):
    generate(str(tmp_path))
    orders=[json.loads(x) for x in (tmp_path / "orders.json").read_text().splitlines()]
    assert any(row["unit_price"] < 0 for row in orders)
    assert any(row["customer_id"] == "C999" for row in orders)
    assert any(row["product_id"] == "P999" for row in orders)
    ids=[row["order_id"] for row in orders]
    assert len(ids) > len(set(ids))

