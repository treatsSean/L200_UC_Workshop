"""
generate_csv_data.py

Generates synthetic CSV data for the Lumina Technologies UC workshop.
Produces three files in workshop/data/output/:
  - customers.csv      (~5,000 rows)
  - transactions.csv   (~25,000 rows)
  - interactions.csv   (~15,000 rows)

All output is deterministic: re-running with the same seed always produces
identical files.

Dependencies: Python 3.8+ standard library only.
Optional: If the `faker` package is installed it will be used for richer
          name/address data. Otherwise built-in word lists are used.

Usage:
    python workshop/data/generate_csv_data.py
"""

import csv
import os
import random
import uuid
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Deterministic seed
# ---------------------------------------------------------------------------

RANDOM_SEED = 42
rng = random.Random(RANDOM_SEED)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NUM_CUSTOMERS = 5_000
NUM_TRANSACTIONS = 25_000
NUM_INTERACTIONS = 15_000

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

# Fixed reference date — keeps dates reproducible regardless of when script runs
REFERENCE_DATE = datetime(2022, 1, 1)
DATE_RANGE_DAYS = 730  # 2 years → up to 2023-12-31

# ---------------------------------------------------------------------------
# Static word lists (no external dependency required)
# ---------------------------------------------------------------------------

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
    "Linda", "William", "Barbara", "David", "Elizabeth", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Christopher",
    "Lisa", "Daniel", "Nancy", "Matthew", "Betty", "Anthony", "Margaret",
    "Mark", "Sandra", "Donald", "Ashley", "Steven", "Dorothy", "Paul",
    "Kimberly", "Andrew", "Emily", "Kenneth", "Donna", "Joshua", "Michelle",
    "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa", "Timothy",
    "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary",
    "Amy", "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna",
    "Stephen", "Brenda", "Larry", "Pamela", "Justin", "Emma", "Scott",
    "Nicole", "Brandon", "Helen", "Benjamin", "Samantha", "Samuel", "Katherine",
    "Raymond", "Christine", "Gregory", "Debra", "Frank", "Rachel", "Alexander",
    "Carolyn", "Patrick", "Janet", "Jack", "Catherine", "Dennis", "Maria",
    "Jerry", "Heather", "Tyler", "Diane", "Aaron", "Julie",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Phillips", "Evans", "Turner", "Torres", "Parker",
    "Collins", "Edwards", "Stewart", "Flores", "Morris", "Nguyen", "Murphy",
    "Rivera", "Cook", "Rogers", "Morgan", "Peterson", "Cooper", "Reed",
    "Bailey", "Bell", "Gomez", "Kelly", "Howard", "Ward", "Cox", "Diaz",
    "Richardson", "Wood", "Watson", "Brooks", "Bennett", "Gray", "James",
    "Reyes", "Cruz", "Hughes", "Price", "Myers", "Long", "Foster", "Sanders",
    "Ross", "Morales", "Powell", "Sullivan", "Russell", "Ortiz", "Jenkins",
    "Gutierrez", "Perry", "Butler", "Barnes", "Fisher",
]

EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "protonmail.com", "aol.com", "live.com", "msn.com", "me.com",
    "company.com", "work.org", "business.net", "mail.com", "inbox.com",
]

STREET_TYPES = ["St", "Ave", "Blvd", "Dr", "Ln", "Rd", "Way", "Ct", "Pl", "Ter"]

STREET_NAMES = [
    "Main", "Oak", "Maple", "Cedar", "Pine", "Elm", "Washington", "Park",
    "Lake", "Hill", "Valley", "River", "Sunset", "Forest", "Meadow",
    "Ridge", "Spring", "Willow", "Highland", "Lincoln", "Madison", "Jefferson",
    "Adams", "Monroe", "Franklin", "Liberty", "Union", "Central", "Church",
    "School", "Mill", "Market", "Water", "Bridge", "Garden", "Harbor",
]

US_CITIES_STATES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),
    ("Houston", "TX"), ("Phoenix", "AZ"), ("Philadelphia", "PA"),
    ("San Antonio", "TX"), ("San Diego", "CA"), ("Dallas", "TX"),
    ("San Jose", "CA"), ("Austin", "TX"), ("Jacksonville", "FL"),
    ("Fort Worth", "TX"), ("Columbus", "OH"), ("Charlotte", "NC"),
    ("Indianapolis", "IN"), ("San Francisco", "CA"), ("Seattle", "WA"),
    ("Denver", "CO"), ("Nashville", "TN"), ("Oklahoma City", "OK"),
    ("El Paso", "TX"), ("Washington", "DC"), ("Las Vegas", "NV"),
    ("Louisville", "KY"), ("Baltimore", "MD"), ("Milwaukee", "WI"),
    ("Albuquerque", "NM"), ("Tucson", "AZ"), ("Fresno", "CA"),
    ("Sacramento", "CA"), ("Mesa", "AZ"), ("Kansas City", "MO"),
    ("Atlanta", "GA"), ("Omaha", "NE"), ("Colorado Springs", "CO"),
    ("Raleigh", "NC"), ("Long Beach", "CA"), ("Virginia Beach", "VA"),
    ("Minneapolis", "MN"), ("Tampa", "FL"), ("New Orleans", "LA"),
    ("Arlington", "TX"), ("Wichita", "KS"), ("Bakersfield", "CA"),
    ("Aurora", "CO"), ("Anaheim", "CA"), ("Santa Ana", "CA"),
    ("Corpus Christi", "TX"), ("Riverside", "CA"),
]

COUNTRIES = [
    "United States", "Canada", "United Kingdom", "Australia", "Germany",
    "France", "Mexico", "Brazil", "Japan", "South Korea", "India",
    "Netherlands", "Sweden", "Norway", "Denmark", "Spain", "Italy",
    "New Zealand", "Singapore", "Ireland",
]

REGIONS = ["NORTH", "SOUTH", "EAST", "WEST"]

CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD"]
TRANSACTION_TYPES = ["purchase", "refund", "subscription"]
PRODUCT_CATEGORIES = [
    "Cloud Platform",
    "Data Analytics",
    "Security",
    "API Services",
    "Support",
    "Storage",
    "Compute",
    "ML Platform",
]

CHANNELS = ["email", "phone", "chat", "web"]
INTERACTION_TYPES = ["support", "marketing", "sales"]

# ---------------------------------------------------------------------------
# Faker-compatible shims (used when faker package is absent)
# ---------------------------------------------------------------------------

def _fake_first_name() -> str:
    return rng.choice(FIRST_NAMES)


def _fake_last_name() -> str:
    return rng.choice(LAST_NAMES)


def _fake_email(first: str, last: str) -> str:
    domain = rng.choice(EMAIL_DOMAINS)
    sep = rng.choice([".", "_", ""])
    tag = str(rng.randint(1, 999)) if rng.random() < 0.4 else ""
    return f"{first.lower()}{sep}{last.lower()}{tag}@{domain}"


def _fake_phone() -> str:
    area = rng.randint(200, 999)
    mid = rng.randint(200, 999)
    end = rng.randint(1000, 9999)
    return f"({area}) {mid}-{end}"


def _fake_street_address() -> str:
    number = rng.randint(1, 9999)
    name = rng.choice(STREET_NAMES)
    stype = rng.choice(STREET_TYPES)
    return f"{number} {name} {stype}"


def _fake_city_state():
    return rng.choice(US_CITIES_STATES)


def _fake_country() -> str:
    return rng.choice(COUNTRIES)


# ---------------------------------------------------------------------------
# Optional: use faker if available for richer data
# ---------------------------------------------------------------------------

try:
    from faker import Faker as _Faker
    _Faker.seed(RANDOM_SEED)
    _fk = _Faker()

    def fake_first_name() -> str:
        return _fk.first_name()

    def fake_last_name() -> str:
        return _fk.last_name()

    def fake_email(first: str, last: str) -> str:
        return _fk.email()

    def fake_phone() -> str:
        return _fk.phone_number()

    def fake_street_address() -> str:
        return _fk.street_address()

    def fake_city_state():
        return (_fk.city(), _fk.state_abbr())

    def fake_country() -> str:
        return _fk.country()

    _FAKER_AVAILABLE = True

except ImportError:
    fake_first_name = _fake_first_name
    fake_last_name = _fake_last_name
    fake_email = _fake_email
    fake_phone = _fake_phone
    fake_street_address = _fake_street_address
    fake_city_state = _fake_city_state
    fake_country = _fake_country
    _FAKER_AVAILABLE = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def random_date() -> str:
    """Return a random date string (YYYY-MM-DD) within the 2-year range."""
    offset = rng.randint(0, DATE_RANGE_DAYS)
    return (REFERENCE_DATE + timedelta(days=offset)).strftime("%Y-%m-%d")


def seeded_uuid(n: int) -> str:
    """Return a deterministic UUID derived from integer n."""
    return str(uuid.UUID(int=n))


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def generate_customers(n: int) -> list:
    customers = []
    for i in range(n):
        first = fake_first_name()
        last = fake_last_name()
        city, state = fake_city_state()
        customers.append({
            "customer_id": seeded_uuid(i + 1),
            "first_name": first,
            "last_name": last,
            "email": fake_email(first, last),
            "phone": fake_phone(),
            "street_address": fake_street_address(),
            "city": city,
            "state": state,
            "country": fake_country(),
            "region": rng.choice(REGIONS),
            "created_date": random_date(),
        })
    return customers


def generate_transactions(n: int, customer_ids: list) -> list:
    transactions = []
    for i in range(n):
        transactions.append({
            "transaction_id": seeded_uuid(i + 1_000_000),
            "customer_id": rng.choice(customer_ids),
            "amount": round(rng.uniform(1.0, 5000.0), 2),
            "currency": rng.choice(CURRENCIES),
            "transaction_type": rng.choice(TRANSACTION_TYPES),
            "product_category": rng.choice(PRODUCT_CATEGORIES),
            "transaction_date": random_date(),
        })
    return transactions


def generate_interactions(n: int, customer_ids: list) -> list:
    interactions = []
    for i in range(n):
        interactions.append({
            "interaction_id": seeded_uuid(i + 10_000_000),
            "customer_id": rng.choice(customer_ids),
            "channel": rng.choice(CHANNELS),
            "interaction_type": rng.choice(INTERACTION_TYPES),
            "sentiment_score": round(rng.uniform(0.0, 1.0), 4),
            "interaction_date": random_date(),
        })
    return interactions


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_csv(rows: list, filepath: str) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Verify output
# ---------------------------------------------------------------------------

def verify_csv(filepath: str, expected_rows: int, expected_cols: list) -> None:
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    actual_cols = list(rows[0].keys()) if rows else []
    assert len(rows) == expected_rows, (
        f"{filepath}: expected {expected_rows} rows, got {len(rows)}"
    )
    assert actual_cols == expected_cols, (
        f"{filepath}: column mismatch\n  expected: {expected_cols}\n  got:      {actual_cols}"
    )
    # Spot-check: no empty customer_id / primary key
    pk = expected_cols[0]
    empty_pk = sum(1 for r in rows if not r[pk].strip())
    assert empty_pk == 0, f"{filepath}: {empty_pk} rows with empty {pk}"
    print(f"  [OK] {os.path.basename(filepath)}: {len(rows):,} rows, {len(actual_cols)} columns")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    using = "faker" if _FAKER_AVAILABLE else "built-in word lists"
    print(f"Generating data for Lumina Technologies UC workshop (using {using})...")
    print(f"Output directory: {OUTPUT_DIR}\n")

    # Customers
    print(f"  Generating {NUM_CUSTOMERS:,} customers...", end=" ", flush=True)
    customers = generate_customers(NUM_CUSTOMERS)
    customer_ids = [c["customer_id"] for c in customers]
    customers_path = os.path.join(OUTPUT_DIR, "customers.csv")
    write_csv(customers, customers_path)
    print("done")

    # Transactions
    print(f"  Generating {NUM_TRANSACTIONS:,} transactions...", end=" ", flush=True)
    transactions = generate_transactions(NUM_TRANSACTIONS, customer_ids)
    transactions_path = os.path.join(OUTPUT_DIR, "transactions.csv")
    write_csv(transactions, transactions_path)
    print("done")

    # Interactions
    print(f"  Generating {NUM_INTERACTIONS:,} interactions...", end=" ", flush=True)
    interactions = generate_interactions(NUM_INTERACTIONS, customer_ids)
    interactions_path = os.path.join(OUTPUT_DIR, "interactions.csv")
    write_csv(interactions, interactions_path)
    print("done")

    # Summary
    print("\nRow counts:")
    print(f"  customers.csv:     {len(customers):>6,}")
    print(f"  transactions.csv:  {len(transactions):>6,}")
    print(f"  interactions.csv:  {len(interactions):>6,}")

    print("\nColumn schemas:")
    print(f"  customers:    {list(customers[0].keys())}")
    print(f"  transactions: {list(transactions[0].keys())}")
    print(f"  interactions: {list(interactions[0].keys())}")

    # Verification
    print("\nRunning verification checks...")
    verify_csv(customers_path, NUM_CUSTOMERS,
               ["customer_id", "first_name", "last_name", "email", "phone",
                "street_address", "city", "state", "country", "region", "created_date"])
    verify_csv(transactions_path, NUM_TRANSACTIONS,
               ["transaction_id", "customer_id", "amount", "currency",
                "transaction_type", "product_category", "transaction_date"])
    verify_csv(interactions_path, NUM_INTERACTIONS,
               ["interaction_id", "customer_id", "channel",
                "interaction_type", "sentiment_score", "interaction_date"])

    # Data quality spot-checks
    print("\nData quality checks...")

    # Region distribution
    region_counts = {}
    for c in customers:
        region_counts[c["region"]] = region_counts.get(c["region"], 0) + 1
    print(f"  Region distribution: {region_counts}")

    # Amount range
    amounts = [float(t["amount"]) for t in transactions]
    print(f"  Transaction amount range: ${min(amounts):.2f} – ${max(amounts):.2f}")

    # Sentiment range
    scores = [float(i["sentiment_score"]) for i in interactions]
    print(f"  Sentiment score range:    {min(scores):.4f} – {max(scores):.4f}")

    # FK integrity: all transaction/interaction customer_ids must be in customers
    cid_set = set(customer_ids)
    bad_tx = sum(1 for t in transactions if t["customer_id"] not in cid_set)
    bad_ix = sum(1 for i in interactions if i["customer_id"] not in cid_set)
    print(f"  FK violations (transactions): {bad_tx}")
    print(f"  FK violations (interactions): {bad_ix}")
    assert bad_tx == 0, "FK violation in transactions"
    assert bad_ix == 0, "FK violation in interactions"

    print("\nAll checks passed. Done.")


if __name__ == "__main__":
    main()
