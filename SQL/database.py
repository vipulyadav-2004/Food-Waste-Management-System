import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / 'data' / 'cleaned_data'
DB_NAME = str(PROJECT_ROOT / 'food_waste.db')


def _build_food_filter_clause(filters=None, alias=''):
    """Builds a safe SQL WHERE fragment for common food listing filters."""
    filters = filters or {}
    prefix = f"{alias}." if alias else ""
    clauses = []
    params = []

    city = filters.get('city')
    provider = filters.get('provider')
    food_type = filters.get('food_type')
    meal_type = filters.get('meal_type')

    if city and city != 'All':
        clauses.append(f"{prefix}City = ?")
        params.append(city)
    if provider and provider != 'All':
        clauses.append(f"{prefix}Provider_Name = ?")
        params.append(provider)
    if food_type and food_type != 'All':
        clauses.append(f"{prefix}Food_Type = ?")
        params.append(food_type)
    if meal_type and meal_type != 'All':
        clauses.append(f"{prefix}Meal_Type = ?")
        params.append(meal_type)

    if clauses:
        return " AND " + " AND ".join(clauses), params
    return "", params

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 1. Create the two denormalized tables
    cursor.executescript('''
        CREATE TABLE IF NOT EXISTS food_listings (
            Food_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Food_Name TEXT, Quantity INTEGER, Expiry_Date TEXT,
            Provider_Name TEXT, Provider_Type TEXT, Address TEXT, City TEXT, Contact TEXT,
            Food_Type TEXT, Meal_Type TEXT
        );

        CREATE TABLE IF NOT EXISTS claims (
            Claim_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Food_ID INTEGER,
            Status TEXT,
            Timestamp TEXT,
            Receiver_Name TEXT,    -- Merged from receivers_data
            Receiver_Type TEXT,    -- Merged from receivers_data
            Receiver_Contact TEXT, -- Merged from receivers_data
            Receiver_City TEXT,    -- Merged from receivers_data
            FOREIGN KEY (Food_ID) REFERENCES food_listings (Food_ID)
        );
    ''')
    
    # 2. Check if data is already loaded
    cursor.execute("SELECT count(*) FROM food_listings")
    if cursor.fetchone()[0] == 0:
        try:
            # 1. Process Food + Providers
            food_df = pd.read_csv(DATA_DIR / 'food_listings_cleaned.csv')
            prov_df = pd.read_csv(DATA_DIR / 'providers_cleaned.csv')
            
            food_master = pd.merge(food_df, prov_df, on='Provider_ID', how='left')
            food_master = food_master.rename(columns={'Name': 'Provider_Name'})
            
            # FIX: Include 'Food_ID' to preserve links with existing claims
            cols_food = ['Food_ID', 'Food_Name', 'Quantity', 'Expiry_Date', 'Provider_Name', 
                        'Provider_Type', 'Address', 'City', 'Contact', 'Food_Type', 'Meal_Type']
            
            food_master[cols_food].to_sql('food_listings', conn, if_exists='append', index=False)

            # 2. Process Claims + Receivers
            claims_df = pd.read_csv(DATA_DIR / 'claims_cleaned.csv')
            recv_df = pd.read_csv(DATA_DIR / 'receivers_cleaned.csv')
            
            claims_master = pd.merge(claims_df, recv_df, on='Receiver_ID', how='left')
            claims_master = claims_master.rename(columns={'Name': 'Receiver_Name', 'Type': 'Receiver_Type', 
                                                        'Contact': 'Receiver_Contact', 'City': 'Receiver_City'})
            
            # FIX: Include 'Claim_ID' and 'Food_ID' to keep historical data accurate
            cols_claims = ['Claim_ID', 'Food_ID', 'Status', 'Timestamp', 'Receiver_Name', 
                        'Receiver_Type', 'Receiver_Contact', 'Receiver_City']
            
            claims_master[cols_claims].to_sql('claims', conn, if_exists='append', index=False)
            
            print("Successfully initialized two-table system with ID preservation.")
        except Exception as e:
            raise RuntimeError(f"Database seed failed from {DATA_DIR}: {e}") from e
        
    conn.commit()
    conn.close()

def add_food_listing(name, qty, expiry, p_name, p_type, addr, city, contact, f_type, m_type):
    """Allows a donor to list new surplus food."""
    conn = sqlite3.connect(DB_NAME)
    query = '''INSERT INTO food_listings (Food_Name, Quantity, Expiry_Date, Provider_Name, 
               Provider_Type, Address, City, Contact, Food_Type, Meal_Type) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    conn.execute(query, (name, qty, expiry, p_name, p_type, addr, city, contact, f_type, m_type))
    conn.commit()
    conn.close()

def get_available_food():
    """Returns all food items that have stock left."""
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM food_listings WHERE Quantity > 0", conn)
    conn.close()
    return df


def get_filter_options():
    """Returns distinct values for UI filters."""
    conn = sqlite3.connect(DB_NAME)
    options = {
        'cities': pd.read_sql_query(
            "SELECT DISTINCT City FROM food_listings WHERE City IS NOT NULL AND TRIM(City) <> '' ORDER BY City",
            conn,
        )['City'].tolist(),
        'providers': pd.read_sql_query(
            "SELECT DISTINCT Provider_Name FROM food_listings WHERE Provider_Name IS NOT NULL AND TRIM(Provider_Name) <> '' ORDER BY Provider_Name",
            conn,
        )['Provider_Name'].tolist(),
        'food_types': pd.read_sql_query(
            "SELECT DISTINCT Food_Type FROM food_listings WHERE Food_Type IS NOT NULL AND TRIM(Food_Type) <> '' ORDER BY Food_Type",
            conn,
        )['Food_Type'].tolist(),
        'meal_types': pd.read_sql_query(
            "SELECT DISTINCT Meal_Type FROM food_listings WHERE Meal_Type IS NOT NULL AND TRIM(Meal_Type) <> '' ORDER BY Meal_Type",
            conn,
        )['Meal_Type'].tolist(),
    }
    conn.close()
    return options


def get_filtered_available_food(filters=None):
    """Returns available food rows with optional filters."""
    conn = sqlite3.connect(DB_NAME)
    filter_clause, params = _build_food_filter_clause(filters)
    query = f"""
        SELECT *
        FROM food_listings
        WHERE Quantity > 0 {filter_clause}
        ORDER BY Expiry_Date ASC, Food_ID DESC
    """
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def get_provider_contacts(filters=None):
    """Returns provider contact details for direct coordination."""
    conn = sqlite3.connect(DB_NAME)
    filter_clause, params = _build_food_filter_clause(filters)
    query = f"""
        SELECT DISTINCT
            Provider_Name,
            Provider_Type,
            Contact,
            Address,
            City
        FROM food_listings
        WHERE Quantity > 0 {filter_clause}
        ORDER BY Provider_Name
    """
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def run_analytics_queries(filters=None):
    """Executes and returns outputs of 15 SQL analytics queries."""
    conn = sqlite3.connect(DB_NAME)
    food_filter_clause, food_filter_params = _build_food_filter_clause(filters, alias='f')
    plain_filter_clause, plain_filter_params = _build_food_filter_clause(filters)

    queries = [
        {
            'title': 'Query 1: Total Available Listings And Units',
            'sql': f"""
                SELECT
                    COUNT(*) AS total_available_listings,
                    COALESCE(SUM(f.Quantity), 0) AS total_available_units
                FROM food_listings f
                WHERE f.Quantity > 0 {food_filter_clause}
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 2: Availability By City',
            'sql': f"""
                SELECT
                    f.City,
                    COUNT(*) AS listing_count,
                    COALESCE(SUM(f.Quantity), 0) AS total_units
                FROM food_listings f
                WHERE f.Quantity > 0 {food_filter_clause}
                GROUP BY f.City
                ORDER BY total_units DESC
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 3: Availability By Provider',
            'sql': f"""
                SELECT
                    f.Provider_Name,
                    COUNT(*) AS listing_count,
                    COALESCE(SUM(f.Quantity), 0) AS total_units
                FROM food_listings f
                WHERE f.Quantity > 0 {food_filter_clause}
                GROUP BY f.Provider_Name
                ORDER BY total_units DESC
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 4: Availability By Food Type',
            'sql': f"""
                SELECT
                    f.Food_Type,
                    COUNT(*) AS listing_count,
                    COALESCE(SUM(f.Quantity), 0) AS total_units
                FROM food_listings f
                WHERE f.Quantity > 0 {food_filter_clause}
                GROUP BY f.Food_Type
                ORDER BY total_units DESC
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 5: Availability By Meal Type',
            'sql': f"""
                SELECT
                    f.Meal_Type,
                    COUNT(*) AS listing_count,
                    COALESCE(SUM(f.Quantity), 0) AS total_units
                FROM food_listings f
                WHERE f.Quantity > 0 {food_filter_clause}
                GROUP BY f.Meal_Type
                ORDER BY total_units DESC
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 6: Items Expiring In 3 Days',
            'sql': f"""
                SELECT
                    f.Food_ID,
                    f.Food_Name,
                    f.Provider_Name,
                    f.City,
                    f.Quantity,
                    f.Expiry_Date,
                    f.Contact
                FROM food_listings f
                WHERE f.Quantity > 0
                  AND DATE(f.Expiry_Date) BETWEEN DATE('now') AND DATE('now', '+3 day')
                  {food_filter_clause}
                ORDER BY DATE(f.Expiry_Date) ASC
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 7: Expired Listings',
            'sql': f"""
                SELECT
                    f.Food_ID,
                    f.Food_Name,
                    f.Provider_Name,
                    f.City,
                    f.Quantity,
                    f.Expiry_Date,
                    f.Contact
                FROM food_listings f
                WHERE DATE(f.Expiry_Date) < DATE('now') {food_filter_clause}
                ORDER BY DATE(f.Expiry_Date) DESC
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 8: Top Providers By Available Units',
            'sql': f"""
                SELECT
                    f.Provider_Name,
                    f.Provider_Type,
                    f.Contact,
                    f.City,
                    COALESCE(SUM(f.Quantity), 0) AS total_units
                FROM food_listings f
                WHERE f.Quantity > 0 {food_filter_clause}
                GROUP BY f.Provider_Name, f.Provider_Type, f.Contact, f.City
                ORDER BY total_units DESC
                LIMIT 10
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 9: Claim Count By Status',
            'sql': f"""
                SELECT
                    c.Status,
                    COUNT(*) AS total_claims
                FROM claims c
                JOIN food_listings f ON c.Food_ID = f.Food_ID
                WHERE 1 = 1 {food_filter_clause}
                GROUP BY c.Status
                ORDER BY total_claims DESC
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 10: Daily Claims Trend (Last 30 Days)',
            'sql': f"""
                SELECT
                    DATE(c.Timestamp) AS claim_date,
                    COUNT(*) AS total_claims
                FROM claims c
                JOIN food_listings f ON c.Food_ID = f.Food_ID
                WHERE DATE(c.Timestamp) >= DATE('now', '-30 day') {food_filter_clause}
                GROUP BY DATE(c.Timestamp)
                ORDER BY claim_date DESC
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 11: Claims By Receiver City',
            'sql': f"""
                SELECT
                    c.Receiver_City,
                    COUNT(*) AS total_claims
                FROM claims c
                JOIN food_listings f ON c.Food_ID = f.Food_ID
                WHERE 1 = 1 {food_filter_clause}
                GROUP BY c.Receiver_City
                ORDER BY total_claims DESC
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 12: Top Receivers By Number Of Claims',
            'sql': f"""
                SELECT
                    c.Receiver_Name,
                    c.Receiver_Type,
                    c.Receiver_Contact,
                    COUNT(*) AS total_claims
                FROM claims c
                JOIN food_listings f ON c.Food_ID = f.Food_ID
                WHERE 1 = 1 {food_filter_clause}
                GROUP BY c.Receiver_Name, c.Receiver_Type, c.Receiver_Contact
                ORDER BY total_claims DESC
                LIMIT 10
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 13: Pending Claims With Provider Contacts',
            'sql': f"""
                SELECT
                    c.Claim_ID,
                    c.Timestamp,
                    c.Receiver_Name,
                    c.Receiver_Contact,
                    f.Food_Name,
                    f.Provider_Name,
                    f.Contact AS Provider_Contact,
                    f.City
                FROM claims c
                JOIN food_listings f ON c.Food_ID = f.Food_ID
                WHERE c.Status = 'Pending' {food_filter_clause}
                ORDER BY c.Timestamp DESC
            """,
            'params': food_filter_params,
        },
        {
            'title': 'Query 14: City-Level Supply vs Claims',
            'sql': f"""
                SELECT
                    f.City,
                    COUNT(DISTINCT f.Food_ID) AS listing_count,
                    COALESCE(SUM(CASE WHEN f.Quantity > 0 THEN f.Quantity ELSE 0 END), 0) AS available_units,
                    COUNT(c.Claim_ID) AS total_claims
                FROM food_listings f
                LEFT JOIN claims c ON c.Food_ID = f.Food_ID
                WHERE 1 = 1 {plain_filter_clause}
                GROUP BY f.City
                ORDER BY available_units DESC
            """,
            'params': plain_filter_params,
        },
        {
            'title': 'Query 15: Filtered Detailed Inventory With Contacts',
            'sql': f"""
                SELECT
                    f.Food_ID,
                    f.Food_Name,
                    f.Quantity,
                    f.Expiry_Date,
                    f.Food_Type,
                    f.Meal_Type,
                    f.Provider_Name,
                    f.Provider_Type,
                    f.Contact,
                    f.Address,
                    f.City
                FROM food_listings f
                WHERE f.Quantity > 0 {food_filter_clause}
                ORDER BY DATE(f.Expiry_Date) ASC, f.Food_ID DESC
            """,
            'params': food_filter_params,
        },
    ]

    outputs = []
    for query in queries:
        df = pd.read_sql_query(query['sql'], conn, params=query['params'])
        outputs.append({'title': query['title'], 'data': df})

    conn.close()
    return outputs

def get_all_food_listings():
    """Returns all food listings."""
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM food_listings ORDER BY Food_ID DESC", conn)
    conn.close()
    return df

def update_food_listing(food_id, name, qty, expiry, p_name, p_type, addr, city, contact, f_type, m_type):
    """Updates an existing food listing by Food_ID."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    query = '''UPDATE food_listings
               SET Food_Name = ?, Quantity = ?, Expiry_Date = ?, Provider_Name = ?,
                   Provider_Type = ?, Address = ?, City = ?, Contact = ?, Food_Type = ?, Meal_Type = ?
               WHERE Food_ID = ?'''
    cursor.execute(query, (name, qty, expiry, p_name, p_type, addr, city, contact, f_type, m_type, food_id))
    conn.commit()
    updated = cursor.rowcount > 0
    conn.close()
    return updated

def get_all_claims():
    """Returns all claim history (who took what)."""
    conn = sqlite3.connect(DB_NAME)
    # Using a JOIN to show Food Name alongside the claim details
    query = '''
        SELECT c.*, f.Food_Name 
        FROM claims c 
        JOIN food_listings f ON c.Food_ID = f.Food_ID
        ORDER BY c.Timestamp DESC
    '''
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def claim_food(food_id, r_name, r_type, r_contact, r_city, claim_qty):
    """Reduces food stock and creates a claim entry (Transactional)."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Check stock
    cursor.execute("SELECT Quantity FROM food_listings WHERE Food_ID = ?", (food_id,))
    res = cursor.fetchone()
    
    if res and res[0] >= claim_qty:
        # Update Quantity in listings
        cursor.execute("UPDATE food_listings SET Quantity = Quantity - ? WHERE Food_ID = ?", (claim_qty, food_id))
        
        # Create a claim record with Receiver details
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''INSERT INTO claims (Food_ID, Status, Timestamp, Receiver_Name, 
                          Receiver_Type, Receiver_Contact, Receiver_City) 
                          VALUES (?, 'Pending', ?, ?, ?, ?, ?)''', 
                       (food_id, now, r_name, r_type, r_contact, r_city))
        conn.commit()
        conn.close()
        return True
    
    conn.close()
    return False

def update_claim_status(claim_id, new_status):
    """Updates the status of a claim (Pending, Completed, Rejected, Cancelled)."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("UPDATE claims SET Status = ? WHERE Claim_ID = ?", (new_status, claim_id))
    conn.commit()
    updated = cursor.rowcount > 0
    conn.close()
    return updated

def delete_listing(food_id):
    """Removes a food listing (e.g., if it expired or was entered in error)."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM food_listings WHERE Food_ID = ?", (food_id,))
    conn.commit()
    deleted = cursor.rowcount > 0
    conn.close()
    return deleted


if __name__ == "__main__":
    init_db()