"""
Insurance Data Warehouse ETL Pipeline
Author: Data Warehouse Project
Description: Extract, Transform, Load insurance claims data into dimensional model
"""

import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# CONFIGURATION
MYSQL_CONFIG = {
    'host': 'localhost',
    'database': 'InsuranceDW',
    'user': 'root',
    'password': ''
}

# File paths - Update these to your local paths
INSURANCE_DATA_PATH = 'insurance_data.csv'
EMPLOYEE_DATA_PATH = 'employee_data.csv'
VENDOR_DATA_PATH = 'vendor_data.csv'


# DATABASE CONNECTION
def create_connection():
    """Create database connection"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        if conn.is_connected():
            print("✅ Connected to MySQL database")
            return conn
    except Error as e:
        print(f"❌ Error: {e}")
        return None


# STEP 1: EXTRACT DATA
def extract_data():
    """Extract data from CSV files"""
    print("\n📤 EXTRACTING DATA...")

    try:
        insurance_df = pd.read_csv(INSURANCE_DATA_PATH)
        employee_df = pd.read_csv(EMPLOYEE_DATA_PATH)
        vendor_df = pd.read_csv(VENDOR_DATA_PATH)

        print(f"✅ Insurance Data: {len(insurance_df):,} rows")
        print(f"✅ Employee Data: {len(employee_df):,} rows")
        print(f"✅ Vendor Data: {len(vendor_df):,} rows")

        return insurance_df, employee_df, vendor_df
    except Exception as e:
        print(f"❌ Error reading files: {e}")
        return None, None, None


# STEP 2: TRANSFORM DATA
def transform_data(insurance_df, employee_df, vendor_df):
    """Clean and transform raw data"""
    print("\n🔄 TRANSFORMING DATA...")

    # ---- Clean Insurance Data ----
    print("  • Cleaning insurance data...")

    # Handle missing values
    insurance_df['VENDOR_ID'] = insurance_df['VENDOR_ID'].fillna('UNKNOWN')
    insurance_df['AUTHORITY_CONTACTED'] = insurance_df['AUTHORITY_CONTACTED'].fillna(
        'None')
    insurance_df['POLICE_REPORT_AVAILABLE'] = insurance_df['POLICE_REPORT_AVAILABLE'].fillna(
        0)
    insurance_df['ANY_INJURY'] = insurance_df['ANY_INJURY'].fillna(0)

    # Convert date columns
    date_columns = ['TXN_DATE_TIME', 'POLICY_EFF_DT', 'LOSS_DT', 'REPORT_DT']
    for col in date_columns:
        insurance_df[col] = pd.to_datetime(insurance_df[col], errors='coerce')

    # Calculate Settlement Days
    insurance_df['SETTLEMENT_DAYS'] = (
        insurance_df['REPORT_DT'] - insurance_df['LOSS_DT']).dt.days
    insurance_df['SETTLEMENT_DAYS'] = insurance_df['SETTLEMENT_DAYS'].clip(
        lower=0)  # No negative days

    # Create Age Groups
    insurance_df['AGE_GROUP'] = pd.cut(insurance_df['AGE'],
                                       bins=[0, 25, 35, 45, 55, 65, 100],
                                       labels=['18-25', '26-35', '36-45', '46-55', '56-65', '65+'])

    # Create Tenure Groups (in months)
    insurance_df['TENURE_GROUP'] = pd.cut(insurance_df['TENURE'],
                                          bins=[0, 12, 24, 36, 60, 120, 1000],
                                          labels=['<1 year', '1-2 years', '2-3 years', '3-5 years', '5-10 years', '10+ years'])

    # ---- Calculate Business KPIs ----
    print("  • Calculating derived metrics...")

    # Premium to Claim Ratio (will be calculated in SQL but we can flag outliers)
    insurance_df['PREMIUM_TO_CLAIM_RATIO'] = insurance_df['PREMIUM_AMOUNT'] / \
        insurance_df['CLAIM_AMOUNT'].replace(0, np.nan)

    # Flag unusually high claims (more than 100x premium)
    insurance_df['HIGH_CLAIM_FLAG'] = (
        insurance_df['CLAIM_AMOUNT'] > insurance_df['PREMIUM_AMOUNT'] * 100).astype(int)

    # ---- Clean Employee Data ----
    print("  • Cleaning employee data...")
    employee_df['DATE_OF_JOINING'] = pd.to_datetime(
        employee_df['DATE_OF_JOINING'], errors='coerce')
    employee_df['TENURE_YEARS'] = (
        datetime.now() - employee_df['DATE_OF_JOINING']).dt.days / 365
    employee_df['TENURE_YEARS'] = employee_df['TENURE_YEARS'].round(1)

    # Extract location from address (simplified)
    employee_df['CITY'] = employee_df['CITY'].fillna('Unknown')
    employee_df['STATE'] = employee_df['STATE'].fillna('Unknown')

    # ---- Clean Vendor Data ----
    print("  • Cleaning vendor data...")
    vendor_df['CITY'] = vendor_df['CITY'].fillna('Unknown')
    vendor_df['STATE'] = vendor_df['STATE'].fillna('Unknown')

    print("✅ Data transformation complete")
    return insurance_df, employee_df, vendor_df


# STEP 3: CREATE DIMENSION TABLES
def load_dim_customer(conn, insurance_df):
    """Load customer dimension (SCD Type 1 for simplicity)"""
    print("\n👤 Loading Dim_Customer...")

    cursor = conn.cursor()

    # Get unique customers
    customer_df = insurance_df[['CUSTOMER_ID', 'CUSTOMER_NAME', 'AGE', 'AGE_GROUP',
                                'MARITAL_STATUS', 'EMPLOYMENT_STATUS', 'CUSTOMER_EDUCATION_LEVEL',
                                'RISK_SEGMENTATION', 'HOUSE_TYPE', 'SOCIAL_CLASS',
                                'NO_OF_FAMILY_MEMBERS', 'TENURE', 'TENURE_GROUP',
                                'CITY', 'STATE', 'POSTAL_CODE']].drop_duplicates(subset=['CUSTOMER_ID'])

    # Insert customers
    insert_query = """
    INSERT INTO Dim_Customer (
        Customer_ID, Customer_Name, Age, Age_Group, Marital_Status,
        Employment_Status, Education_Level, Risk_Segmentation, House_Type,
        Social_Class, No_Of_Family_Members, Tenure_Months, City, State, 
        Postal_Code, Effective_Date, Is_Current
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    today = datetime.now().date()
    records = []

    for _, row in customer_df.iterrows():
        records.append((
            row['CUSTOMER_ID'],
            row['CUSTOMER_NAME'][:50] if pd.notna(
                row['CUSTOMER_NAME']) else 'Unknown',
            row['AGE'] if pd.notna(row['AGE']) else 0,
            row['AGE_GROUP'] if pd.notna(row['AGE_GROUP']) else 'Unknown',
            row['MARITAL_STATUS'] if pd.notna(row['MARITAL_STATUS']) else 'U',
            row['EMPLOYMENT_STATUS'] if pd.notna(
                row['EMPLOYMENT_STATUS']) else 'Unknown',
            row['CUSTOMER_EDUCATION_LEVEL'] if pd.notna(
                row['CUSTOMER_EDUCATION_LEVEL']) else 'Unknown',
            row['RISK_SEGMENTATION'] if pd.notna(
                row['RISK_SEGMENTATION']) else 'M',
            row['HOUSE_TYPE'] if pd.notna(row['HOUSE_TYPE']) else 'Unknown',
            row['SOCIAL_CLASS'] if pd.notna(row['SOCIAL_CLASS']) else 'MI',
            row['NO_OF_FAMILY_MEMBERS'] if pd.notna(
                row['NO_OF_FAMILY_MEMBERS']) else 0,
            row['TENURE'] if pd.notna(row['TENURE']) else 0,
            row['CITY'] if pd.notna(row['CITY']) else 'Unknown',
            row['STATE'] if pd.notna(row['STATE']) else 'Unknown',
            row['POSTAL_CODE'] if pd.notna(row['POSTAL_CODE']) else 'Unknown',
            today,
            True
        ))

    cursor.executemany(insert_query, records)
    conn.commit()
    print(f"✅ Loaded {len(records)} customers")


def load_dim_agent(conn, employee_df):
    """Load agent dimension"""
    print("\n👤 Loading Dim_Agent...")

    cursor = conn.cursor()

    insert_query = """
    INSERT INTO Dim_Agent (
        Agent_ID, Agent_Name, Date_Of_Joining, Tenure_Years,
        City, State, Postal_Code, Effective_Date, Is_Current
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    today = datetime.now().date()
    records = []

    for _, row in employee_df.iterrows():
        records.append((
            row['AGENT_ID'],
            row['AGENT_NAME'][:50] if pd.notna(
                row['AGENT_NAME']) else 'Unknown',
            row['DATE_OF_JOINING'].date() if pd.notna(
                row['DATE_OF_JOINING']) else today,
            row['TENURE_YEARS'] if pd.notna(row['TENURE_YEARS']) else 0,
            row['CITY'] if pd.notna(row['CITY']) else 'Unknown',
            row['STATE'] if pd.notna(row['STATE']) else 'Unknown',
            row['POSTAL_CODE'] if pd.notna(row['POSTAL_CODE']) else 'Unknown',
            today,
            True
        ))

    cursor.executemany(insert_query, records)
    conn.commit()
    print(f"✅ Loaded {len(records)} agents")


def load_dim_vendor(conn, vendor_df):
    """Load vendor dimension"""
    print("\n🏢 Loading Dim_Vendor...")

    cursor = conn.cursor()

    insert_query = """
    INSERT INTO Dim_Vendor (
        Vendor_ID, Vendor_Name, City, State, Postal_Code, 
        Effective_Date, Is_Current
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    today = datetime.now().date()
    records = []

    for _, row in vendor_df.iterrows():
        records.append((
            row['VENDOR_ID'],
            row['VENDOR_NAME'][:50] if pd.notna(
                row['VENDOR_NAME']) else 'Unknown',
            row['CITY'] if pd.notna(row['CITY']) else 'Unknown',
            row['STATE'] if pd.notna(row['STATE']) else 'Unknown',
            row['POSTAL_CODE'] if pd.notna(row['POSTAL_CODE']) else 'Unknown',
            today,
            True
        ))

    cursor.executemany(insert_query, records)
    conn.commit()
    print(f"✅ Loaded {len(records)} vendors")


def load_dim_policy(conn, insurance_df):
    """Load policy dimension"""
    print("\n📄 Loading Dim_Policy...")

    cursor = conn.cursor()

    # Get unique policies
    policy_df = insurance_df[['POLICY_NUMBER', 'INSURANCE_TYPE', 'PREMIUM_AMOUNT',
                              'POLICY_EFF_DT']].drop_duplicates(subset=['POLICY_NUMBER'])

    insert_query = """
    INSERT INTO Dim_Policy (
        Policy_Number, Insurance_Type, Premium_Amount, Policy_Effective_Date,
        Policy_Year, Effective_Date, Is_Current
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    today = datetime.now().date()
    records = []

    for _, row in policy_df.iterrows():
        eff_date = row['POLICY_EFF_DT'].date() if pd.notna(
            row['POLICY_EFF_DT']) else today

        records.append((
            row['POLICY_NUMBER'],
            row['INSURANCE_TYPE'] if pd.notna(
                row['INSURANCE_TYPE']) else 'Unknown',
            row['PREMIUM_AMOUNT'] if pd.notna(row['PREMIUM_AMOUNT']) else 0,
            eff_date,
            eff_date.year,
            today,
            True
        ))

    cursor.executemany(insert_query, records)
    conn.commit()
    print(f"✅ Loaded {len(records)} policies")


def load_dim_time(conn):
    """Load time dimension - 5 years of data"""
    print("\n📅 Loading Dim_Time...")

    cursor = conn.cursor()

    # Generate dates for 5 years (2020-2024)
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)

    insert_query = """
    INSERT INTO Dim_Time (
        Full_Date, Year, Quarter, Month, Month_Name,
        Week, Day_of_Week, Day_Name, Is_Weekend
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = []
    current_date = start_date

    while current_date <= end_date:
        records.append((
            current_date.date(),
            current_date.year,
            (current_date.month - 1) // 3 + 1,
            current_date.month,
            current_date.strftime('%B'),
            current_date.isocalendar()[1],
            current_date.weekday(),
            current_date.strftime('%A'),
            current_date.weekday() >= 5  # Saturday or Sunday
        ))
        current_date += timedelta(days=1)

    # Insert in batches
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        cursor.executemany(insert_query, batch)
        conn.commit()

    print(f"✅ Loaded {len(records)} days")


# STEP 4: CREATE FACT TABLE
def load_fact_claims(conn, insurance_df):
    """Load fact claims table with business KPIs"""
    print("\n📊 Loading Fact_Claims...")

    cursor = conn.cursor()

    # Get dimension keys
    cursor.execute("SELECT Customer_ID, Customer_Key FROM Dim_Customer")
    customer_map = dict(cursor.fetchall())

    cursor.execute("SELECT Agent_ID, Agent_Key FROM Dim_Agent")
    agent_map = dict(cursor.fetchall())

    cursor.execute("SELECT Vendor_ID, Vendor_Key FROM Dim_Vendor")
    vendor_map = dict(cursor.fetchall())
    vendor_map['UNKNOWN'] = None

    cursor.execute("SELECT Policy_Number, Policy_Key FROM Dim_Policy")
    policy_map = dict(cursor.fetchall())

    cursor.execute("SELECT Full_Date, Time_Key FROM Dim_Time")
    time_map = {str(d): k for d, k in cursor.fetchall()}

    # Business KPIs calculation
    print("  • Calculating business KPIs...")

    # Calculate claim statistics by insurance type (will be used in dashboard)
    insurance_stats = insurance_df.groupby('INSURANCE_TYPE').agg({
        'CLAIM_AMOUNT': ['mean', 'sum', 'count'],
        'PREMIUM_AMOUNT': 'sum'
    }).round(2)

    print("\n📈 Insurance Type Summary:")
    print(insurance_stats)

    # Prepare fact records
    insert_query = """
    INSERT INTO Fact_Claims (
        Transaction_ID, Customer_Key, Agent_Key, Vendor_Key, Policy_Key, Time_Key,
        Premium_Amount, Claim_Amount, Settlement_Days, Claim_Count,
        Insurance_Type, Claim_Status, Incident_Severity, Fraud_Flag,
        Authority_Contacted, Any_Injury, Police_Report_Available, Incident_Hour
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = []
    fraud_cases = 0

    for _, row in insurance_df.iterrows():
        # Get date key
        txn_date = row['TXN_DATE_TIME'].date() if pd.notna(
            row['TXN_DATE_TIME']) else None
        time_key = time_map.get(str(txn_date), None)

        if time_key is None:
            continue

        # Simple fraud flag logic (can be enhanced later)
        fraud_flag = 0
        # Flag 1: Claim amount > 50x premium
        if row['CLAIM_AMOUNT'] > row['PREMIUM_AMOUNT'] * 50:
            fraud_flag = 1
        # Flag 2: Incident late night (11 PM - 4 AM) with high claim
        elif row['INCIDENT_HOUR_OF_THE_DAY'] in [23, 0, 1, 2, 3, 4] and row['CLAIM_AMOUNT'] > 5000:
            fraud_flag = 1
        # Flag 3: No police report for major/total loss
        elif row['INCIDENT_SEVERITY'] in ['Major Loss', 'Total Loss'] and row['POLICE_REPORT_AVAILABLE'] == 0:
            fraud_flag = 1

        if fraud_flag == 1:
            fraud_cases += 1

        records.append((
            row['TRANSACTION_ID'],
            customer_map.get(row['CUSTOMER_ID'], None),
            agent_map.get(row['AGENT_ID'], None),
            vendor_map.get(row['VENDOR_ID'], None),
            policy_map.get(row['POLICY_NUMBER'], None),
            time_key,
            row['PREMIUM_AMOUNT'],
            row['CLAIM_AMOUNT'],
            row['SETTLEMENT_DAYS'],
            1,  # Claim count
            row['INSURANCE_TYPE'],
            row['CLAIM_STATUS'],
            row['INCIDENT_SEVERITY'],
            fraud_flag,
            row['AUTHORITY_CONTACTED'],
            row['ANY_INJURY'],
            row['POLICE_REPORT_AVAILABLE'],
            row['INCIDENT_HOUR_OF_THE_DAY']
        ))

    # Insert in batches
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        cursor.executemany(insert_query, batch)
        conn.commit()

    print(f"✅ Loaded {len(records)} claim facts")
    print(
        f"⚠️  Flagged {fraud_cases} potential fraud cases ({fraud_cases/len(records)*100:.1f}%)")


# STEP 5: CREATE BUSINESS KPI VIEWS
def create_kpi_views(conn):
    """Create views for easy KPI access in PowerBI"""
    print("\n👁️ Creating KPI views...")

    cursor = conn.cursor()

    # View 1: Monthly Insurance KPIs
    cursor.execute("""
    CREATE OR REPLACE VIEW vw_monthly_kpis AS
    SELECT 
        t.Year,
        t.Month,
        t.Month_Name,
        f.Insurance_Type,
        COUNT(*) as Total_Claims,
        SUM(f.Claim_Amount) as Total_Claim_Amount,
        SUM(f.Premium_Amount) as Total_Premium_Amount,
        AVG(f.Claim_Amount) as Avg_Claim_Amount,
        AVG(f.Settlement_Days) as Avg_Settlement_Days,
        SUM(f.Claim_Amount) / NULLIF(SUM(f.Premium_Amount), 0) * 100 as Loss_Ratio_Pct,
        SUM(CASE WHEN f.Fraud_Flag = 1 THEN 1 ELSE 0 END) as Fraud_Cases,
        SUM(CASE WHEN f.Fraud_Flag = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100 as Fraud_Rate_Pct
    FROM Fact_Claims f
    JOIN Dim_Time t ON f.Time_Key = t.Time_Key
    GROUP BY t.Year, t.Month, t.Month_Name, f.Insurance_Type
    """)

    # View 2: Agent Performance KPIs
    cursor.execute("""
    CREATE OR REPLACE VIEW vw_agent_performance AS
    SELECT 
        a.Agent_ID,
        a.Agent_Name,
        a.State as Agent_State,
        COUNT(*) as Total_Claims_Handled,
        SUM(f.Claim_Amount) as Total_Claim_Amount,
        AVG(f.Claim_Amount) as Avg_Claim_Amount,
        AVG(f.Settlement_Days) as Avg_Settlement_Days,
        SUM(CASE WHEN f.Fraud_Flag = 1 THEN 1 ELSE 0 END) as Fraud_Cases,
        SUM(CASE WHEN f.Fraud_Flag = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100 as Fraud_Rate
    FROM Fact_Claims f
    JOIN Dim_Agent a ON f.Agent_Key = a.Agent_Key
    GROUP BY a.Agent_ID, a.Agent_Name, a.State
    """)

    # View 3: Customer Segment Analysis
    cursor.execute("""
    CREATE OR REPLACE VIEW vw_customer_segments AS
    SELECT 
        c.Age_Group,
        c.Risk_Segmentation,
        c.House_Type,
        c.Social_Class,
        COUNT(DISTINCT c.Customer_Key) as Customer_Count,
        COUNT(*) as Total_Claims,
        AVG(f.Claim_Amount) as Avg_Claim_Per_Customer,
        SUM(f.Claim_Amount) as Total_Claim_Amount,
        SUM(f.Premium_Amount) as Total_Premium_Amount
    FROM Fact_Claims f
    JOIN Dim_Customer c ON f.Customer_Key = c.Customer_Key
    GROUP BY c.Age_Group, c.Risk_Segmentation, c.House_Type, c.Social_Class
    """)

    # View 4: Overall Business KPIs
    cursor.execute("""
    CREATE OR REPLACE VIEW vw_business_summary AS
    SELECT 
        COUNT(DISTINCT Transaction_ID) as Total_Claims,
        COUNT(DISTINCT Policy_Key) as Total_Policies,
        COUNT(DISTINCT Customer_Key) as Total_Customers,
        SUM(Claim_Amount) as Total_Claims_Paid,
        SUM(Premium_Amount) as Total_Premium_Earned,
        AVG(Claim_Amount) as Avg_Claim_Cost,
        AVG(Settlement_Days) as Avg_Days_To_Settle,
        SUM(Claim_Amount) / NULLIF(SUM(Premium_Amount), 0) * 100 as Overall_Loss_Ratio,
        SUM(CASE WHEN Fraud_Flag = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100 as Overall_Fraud_Rate
    FROM Fact_Claims
    """)

    conn.commit()
    print("✅ Created 4 KPI views")


# MAIN ETL PIPELINE
def run_etl():
    """Main ETL pipeline execution"""
    print("=" * 60)
    print("🏥 INSURANCE DATA WAREHOUSE ETL PIPELINE")
    print("=" * 60)

    start_time = datetime.now()

    # Step 1: Extract
    insurance_df, employee_df, vendor_df = extract_data()
    if insurance_df is None:
        return

    # Step 2: Transform
    insurance_df, employee_df, vendor_df = transform_data(
        insurance_df, employee_df, vendor_df)

    # Step 3: Connect to database
    conn = create_connection()
    if not conn:
        return

    try:
        # Clear existing data (optional - be careful!)
        cursor = conn.cursor()
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute("TRUNCATE TABLE Fact_Claims")
        cursor.execute("TRUNCATE TABLE Dim_Customer")
        cursor.execute("TRUNCATE TABLE Dim_Agent")
        cursor.execute("TRUNCATE TABLE Dim_Vendor")
        cursor.execute("TRUNCATE TABLE Dim_Policy")
        cursor.execute("TRUNCATE TABLE Dim_Time")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()
        print("\n🧹 Cleared existing data")

        # Step 4: Load dimensions
        load_dim_customer(conn, insurance_df)
        load_dim_agent(conn, employee_df)
        load_dim_vendor(conn, vendor_df)
        load_dim_policy(conn, insurance_df)
        load_dim_time(conn)

        # Step 5: Load fact table
        load_fact_claims(conn, insurance_df)

        # Step 6: Create views
        create_kpi_views(conn)

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print("\n" + "=" * 60)
        print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"⏱️  Duration: {duration:.2f} seconds")
        print(f"📊 Total Claims Processed: {len(insurance_df):,}")

        # Show sample KPIs
        cursor.execute("SELECT * FROM vw_business_summary")
        summary = cursor.fetchone()
        if summary:
            print("\n📈 BUSINESS SUMMARY:")
            print(f"   Total Claims: {summary[0]:,}")
            print(f"   Total Premium: ${summary[4]:,.2f}")
            print(f"   Total Claims Paid: ${summary[3]:,.2f}")
            print(f"   Loss Ratio: {summary[6]:.2f}%")
            print(f"   Fraud Rate: {summary[7]:.2f}%")

    except Error as e:
        print(f"❌ Database error: {e}")
        conn.rollback()
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("\n🔒 Database connection closed")


# RUN THE ETL
if __name__ == "__main__":
    run_etl()
