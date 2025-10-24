import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os

from db_setup import run_sql

st.set_page_config(page_title="Food Waste Management System", layout="wide")

st.title("🍽️ Local Food Waste Management System")
st.subheader("Welcome, Mehak!")
st.write("If you can see this, your Streamlit setup is working perfectly ✅")

st.markdown("---")
st.write("Next, we’ll connect your database and display some data.")

DB_PATH = r"D:\Project Mini\food_waste.db"
engine = create_engine(f"sqlite:///{DB_PATH}")

st.header("📊 Food Listings Overview")
df = pd.read_sql("SELECT * FROM food_listings", engine)
st.dataframe(df)



# Sidebar filters
st.sidebar.header("Filters")
city = st.sidebar.text_input("City (leave blank = all)")
provider = st.sidebar.text_input("Provider name (substring)")

# Data selection
q = "SELECT * FROM food_listings"
conds = []
params = {}
if city:
    conds.append("Location = :city")
    params['city'] = city
if provider:
    conds.append("Provider_ID IN (SELECT Provider_ID FROM providers WHERE Name LIKE :pname)")
    params['pname'] = f"%{provider}%"
if conds:
    q = q + " WHERE " + " AND ".join(conds)

df = run_sql(q, params or None)
st.subheader("Food listings")
st.dataframe(df)

# Simple CRUD UI
st.sidebar.header("Add Food Listing")
with st.sidebar.form("add_food"):
    fid = st.number_input("Food_ID", value=0)
    fname = st.text_input("Food_Name")
    qty = st.number_input("Quantity", value=1)
    expiry = st.date_input("Expiry Date")
    pid = st.number_input("Provider_ID", value=1)
    ptype = st.text_input("Provider_Type", value="Restaurant")
    loc = st.text_input("Location", value=city or "")
    ftype = st.selectbox("Food_Type", ["Vegetarian", "Non-Vegetarian", "Vegan"])
    mtype = st.selectbox("Meal_Type", ["Breakfast", "Lunch", "Dinner", "Snacks"])
    submitted = st.form_submit_button("Add")
    if submitted:
        insert_q = """
        INSERT INTO food_listings (Food_ID, Food_Name, Quantity, Expiry_Date, Provider_ID, Provider_Type, Location, Food_Type, Meal_Type)
        VALUES (:fid, :fname, :qty, :expiry, :pid, :ptype, :loc, :ftype, :mtype)
        """
        try:
            with engine.begin() as conn:
                conn.execute(text(insert_q), {"fid":int(fid),"fname":fname,"qty":int(qty),"expiry":expiry.strftime("%Y-%m-%d"),"pid":int(pid),"ptype":ptype,"loc":loc,"ftype":ftype,"mtype":mtype})
            st.success("Inserted.")
        except Exception as e:
            st.error(f"Failed to insert: {e}")

# Claims view & action
st.subheader("Claims")
claims_df = run_sql("SELECT c.*, r.Name AS Receiver_Name, f.Food_Name FROM claims c LEFT JOIN receivers r ON c.Receiver_ID=r.Receiver_ID LEFT JOIN food_listings f ON c.Food_ID=f.Food_ID")
st.dataframe(claims_df)

st.sidebar.header("Update Claim Status")
cid = st.sidebar.number_input("Claim_ID", value=0)
new_status = st.sidebar.selectbox("New status", ["Pending","Completed","Cancelled"])
if st.sidebar.button("Update Claim"):
    try:
        with engine.begin() as conn:
            conn.execute(text("UPDATE claims SET Status = :s WHERE Claim_ID = :cid"), {"s":new_status,"cid":int(cid)})
        st.sidebar.success("Updated claim")
    except Exception as e:
        st.sidebar.error(str(e))

st.sidebar.markdown("---")
st.sidebar.write("DB path: " + DB_PATH)
