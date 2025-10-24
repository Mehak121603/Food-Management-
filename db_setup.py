import os
import sqlite3
import pandas as pd
from holoviews.ipython import display
from sqlalchemy import create_engine, text
from datetime import datetime

DATA_DIR = r"D:\Project Mini"   # change if your CSVs are elsewhere
PROVIDERS_CSV = os.path.join(DATA_DIR, "providers_data.csv")
RECEIVERS_CSV = os.path.join(DATA_DIR, "receivers_data.csv")
FOOD_LISTINGS_CSV = os.path.join(DATA_DIR, "food_listings_data.csv")
CLAIMS_CSV = os.path.join(DATA_DIR, "claims_data.csv")

def safe_read_csv(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}. Please upload it to {DATA_DIR}")
    return pd.read_csv(path)

providers = safe_read_csv(PROVIDERS_CSV)
receivers = safe_read_csv(RECEIVERS_CSV)
food_listings = safe_read_csv(FOOD_LISTINGS_CSV)
claims = safe_read_csv(CLAIMS_CSV)

print("Providers:", providers.shape)
print("Receivers:", receivers.shape)
print("Food listings:", food_listings.shape)
print("Claims:", claims.shape)

providers.head()

def clean_providers(df):
    df = df.copy()
    df.columns = df.columns.str.strip()
    # ensure Provider_ID is int
    if 'Provider_ID' in df.columns:
        df['Provider_ID'] = pd.to_numeric(df['Provider_ID'], errors='coerce').fillna(0).astype(int)
    df['City'] = df['City'].fillna('Unknown').astype(str).str.strip()
    return df

def clean_receivers(df):
    df = df.copy()
    df.columns = df.columns.str.strip()
    if 'Receiver_ID' in df.columns:
        df['Receiver_ID'] = pd.to_numeric(df['Receiver_ID'], errors='coerce').fillna(0).astype(int)
    df['City'] = df['City'].fillna('Unknown').astype(str).str.strip()
    return df

def clean_food(df):
    df = df.copy()
    df.columns = df.columns.str.strip()
    # parse dates
    if 'Expiry_Date' in df.columns:
        df['Expiry_Date'] = pd.to_datetime(df['Expiry_Date'], errors='coerce')
    if 'Quantity' in df.columns:
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)
    df['Location'] = df['Location'].fillna('Unknown').astype(str).str.strip()
    return df

def clean_claims(df):
    df = df.copy()
    df.columns = df.columns.str.strip()
    if 'Timestamp' in df.columns:
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    if 'Claim_ID' in df.columns:
        df['Claim_ID'] = pd.to_numeric(df['Claim_ID'], errors='coerce').fillna(0).astype(int)
    return df

providers = clean_providers(providers)
receivers = clean_receivers(receivers)
food_listings = clean_food(food_listings)
claims = clean_claims(claims)

DB_PATH = os.path.join(DATA_DIR, "food_waste.db")
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)

# write to SQL (replace if exists)
providers.to_sql("providers", engine, if_exists="replace", index=False)
receivers.to_sql("receivers", engine, if_exists="replace", index=False)
food_listings.to_sql("food_listings", engine, if_exists="replace", index=False)
claims.to_sql("claims", engine, if_exists="replace", index=False)

print("Database and tables created at:", DB_PATH)

def run_sql(query):
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


print(run_sql("SELECT count(*) AS n_providers FROM providers"))

queries = {}

# 1: How many food providers and receivers are there in each city?
queries['providers_receivers_by_city'] = """
SELECT city,
  (SELECT COUNT(*) FROM providers p WHERE p.city = t.city) AS providers_count,
  (SELECT COUNT(*) FROM receivers r WHERE r.city = t.city) AS receivers_count
FROM (
  SELECT city FROM providers
  UNION
  SELECT city FROM receivers
) t
ORDER BY city;
"""

# 2: Which provider type contributes the most food? (by total quantity)
queries['provider_type_most_food'] = """
SELECT Provider_Type, SUM(Quantity) AS total_quantity
FROM food_listings
GROUP BY Provider_Type
ORDER BY total_quantity DESC;
"""

# 3: Contact info of providers in a specific city (example 'New Delhi' — change as needed)
queries['providers_contact_city'] = """
SELECT Name, Address, City, Contact
FROM providers
WHERE City = 'New Delhi';
"""

# 4: Which receivers have claimed the most food?
queries['receivers_most_claims'] = """
SELECT r.Name, r.Contact, COUNT(c.Claim_ID) AS num_claims, COALESCE(SUM(f.Quantity),0) AS total_quantity_claimed
FROM claims c
LEFT JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
LEFT JOIN food_listings f ON c.Food_ID = f.Food_ID
GROUP BY r.Receiver_ID
ORDER BY num_claims DESC, total_quantity_claimed DESC;
"""

# 5: Total quantity of food available
queries['total_quantity_available'] = "SELECT SUM(Quantity) AS total_available FROM food_listings;"

# 6: Which city has highest number of food listings
queries['city_highest_listings'] = """
SELECT Location AS city, COUNT(*) AS listings_count
FROM food_listings
GROUP BY Location
ORDER BY listings_count DESC;
"""

# 7: Most commonly available food types
queries['common_food_types'] = """
SELECT Food_Type, COUNT(*) AS num_listings
FROM food_listings
GROUP BY Food_Type
ORDER BY num_listings DESC;
"""

# 8: How many food claims have been made for each food item
queries['claims_per_food'] = """
SELECT f.Food_ID, f.Food_Name, COUNT(c.Claim_ID) AS num_claims
FROM food_listings f
LEFT JOIN claims c ON f.Food_ID = c.Food_ID
GROUP BY f.Food_ID, f.Food_Name
ORDER BY num_claims DESC;
"""

# 9: Which provider has highest number of successful claims
queries['provider_most_successful_claims'] = """
SELECT p.Provider_ID, p.Name, COUNT(c.Claim_ID) AS successful_claims
FROM claims c
JOIN food_listings f ON c.Food_ID = f.Food_ID
JOIN providers p ON f.Provider_ID = p.Provider_ID
WHERE c.Status = 'Completed'
GROUP BY p.Provider_ID, p.Name
ORDER BY successful_claims DESC;
"""

# 10: Percentage of claim statuses
queries['claim_status_percentage'] = """
SELECT Status, COUNT(*) AS cnt,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM claims), 2) AS pct
FROM claims
GROUP BY Status;
"""

# 11: Average quantity of food claimed per receiver
queries['avg_qty_per_receiver'] = """
SELECT r.Receiver_ID, r.Name, COALESCE(AVG(f.Quantity),0) AS avg_quantity
FROM receivers r
LEFT JOIN claims c ON r.Receiver_ID = c.Receiver_ID
LEFT JOIN food_listings f ON c.Food_ID = f.Food_ID
GROUP BY r.Receiver_ID, r.Name
ORDER BY avg_quantity DESC;
"""

# 12: Which meal type is claimed the most
queries['meal_type_most_claimed'] = """
SELECT f.Meal_Type, COUNT(c.Claim_ID) AS num_claims
FROM claims c
JOIN food_listings f ON c.Food_ID = f.Food_ID
GROUP BY f.Meal_Type
ORDER BY num_claims DESC;
"""

# 13: Total quantity donated by each provider
queries['total_qty_by_provider'] = """
SELECT p.Provider_ID, p.Name, COALESCE(SUM(f.Quantity),0) AS total_donated
FROM providers p
LEFT JOIN food_listings f ON p.Provider_ID = f.Provider_ID
GROUP BY p.Provider_ID, p.Name
ORDER BY total_donated DESC;
"""

# 14: Food listings near expiry (next 3 days)
queries['near_expiry'] = """
SELECT Food_ID, Food_Name, Quantity, Expiry_Date, Location
FROM food_listings
WHERE date(Expiry_Date) <= date('now', '+3 days')
ORDER BY Expiry_Date ASC;
"""

# 15: Top providers by listings count
queries['top_providers_by_listings'] = """
SELECT p.Provider_ID, p.Name, COUNT(f.Food_ID) AS listings_count
FROM providers p
LEFT JOIN food_listings f ON p.Provider_ID = f.Provider_ID
GROUP BY p.Provider_ID, p.Name
ORDER BY listings_count DESC;
"""

# Run and display queries
for key, q in queries.items():
    print("\n---", key, "---")
    try:
        display(run_sql(q).head(10))
    except Exception as e:
        print("Error running query", key, ":", e)

# Python helper functions to add / update / delete food listings and claims

def add_food_listing(row_dict):
    df = pd.DataFrame([row_dict])
    df.to_sql("food_listings", engine, if_exists="append", index=False)
    print("Inserted food listing.")

def update_food_listing(food_id, updates: dict):
    set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys()])
    params = dict(updates)
    params['food_id'] = food_id
    q = f"UPDATE food_listings SET {set_clause} WHERE Food_ID = :food_id"
    with engine.begin() as conn:
        conn.execute(text(q), params)
    print("Updated food listing", food_id)

def delete_food_listing(food_id):
    q = "DELETE FROM food_listings WHERE Food_ID = :fid"
    with engine.begin() as conn:
        conn.execute(text(q), {"fid": food_id})
    print("Deleted food listing", food_id)

def add_claim(claim_dict):
    df = pd.DataFrame([claim_dict])
    df.to_sql("claims", engine, if_exists="append", index=False)
    print("Inserted claim.")

def update_claim_status(claim_id, new_status):
    q = "UPDATE claims SET Status = :status WHERE Claim_ID = :cid"
    with engine.begin() as conn:
        conn.execute(text(q), {"status": new_status, "cid": claim_id})
    print("Updated claim", claim_id, "to", new_status)


new_food = {
    "Food_ID": 9999,
    "Food_Name": "Extra Sandwich",
    "Quantity": 10,
    "Expiry_Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "Provider_ID": 1,
    "Provider_Type": "Restaurant",
    "Location": "New Delhi",
    "Food_Type": "Vegetarian",
    "Meal_Type": "Snacks"
}



new_claim = {
    "Claim_ID": 9999,
    "Food_ID": 9999,
    "Receiver_ID": 1,
    "Status": "Pending",
    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

print("Uncomment the add/claims lines to execute them.")

providers.to_csv(os.path.join(DATA_DIR, "providers_clean.csv"), index=False)
receivers.to_csv(os.path.join(DATA_DIR, "receivers_clean.csv"), index=False)
food_listings.to_csv(os.path.join(DATA_DIR, "food_listings_clean.csv"), index=False)
claims.to_csv(os.path.join(DATA_DIR, "claims_clean.csv"), index=False)
print("Cleaned CSVs saved to", DATA_DIR)


