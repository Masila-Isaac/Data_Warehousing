"""
Insurance Data Warehouse ETL Pipeline - FINAL FIXED VERSION
Fixed Issues:
- Series truth value error in safe_int function
- Categorical data type issues with fillna
- Proper vectorized operations for pandas
- Improved data type handling
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
    'password': '',  # Add your password if needed
    'connection_timeout': 300,
    'autocommit': False,
    'buffered': True
}

# File paths
INSURANCE_DATA_PATH = 'insurance_data.csv'
EMPLOYEE_DATA_PATH = 'employee_data.csv'
VENDOR_DATA_PATH = 'vendor_data.csv'

# Batch sizes for database operations
BATCH_SIZE = 500
TIME_BATCH_SIZE = 1000

# DATABASE CONNECTION


def create_connection():
    """Create database connection with error handling"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        if conn.is_connected():
            # Set session variables
            cursor = conn.cursor()
            cursor.execute("SET SESSION net_write_timeout=600")
            cursor.execute("SET SESSION net_read_timeout=600")
            cursor.execute("SET SESSION wait_timeout=28800")
            cursor.execute("SET SESSION interactive_timeout=28800")
            cursor.close()
            print("✅ Connected to MySQL database")
            return conn
    except Error as e:
        print(f"❌ Connection Error: {e}")
        return None

# STEP 1: EXTRACT DATA


def extract_data():
    """Extract data from CSV files with encoding handling"""
    print("\n📤 EXTRACTING DATA...")

    try:
        insurance_df = pd.read_csv(INSURANCE_DATA_PATH, encoding='utf-8')
        employee_df = pd.read_csv(EMPLOYEE_DATA_PATH, encoding='utf-8')
        vendor_df = pd.read_csv(VENDOR_DATA_PATH, encoding='utf-8')

        print(
            f"✅ Insurance Data: {len(insurance_df):,} rows, {len(insurance_df.columns)} columns")
        print(
            f"✅ Employee Data: {len(employee_df):,} rows, {len(employee_df.columns)} columns")
        print(
            f"✅ Vendor Data: {len(vendor_df):,} rows, {len(vendor_df.columns)} columns")

        return insurance_df, employee_df, vendor_df
    except Exception as e:
        print(f"❌ Error reading files: {e}")
        return None, None, None

# HELPER FUNCTIONS FOR DATA CLEANING (VECTORIZED)


def clean_text_series(series, max_len=50, default='Unknown'):
    """Clean a text series - vectorized operation"""
    result = series.fillna(default).astype(str)
    result = result.str[:max_len]
    return result


def clean_int_series(series, default=0):
    """Clean an integer series - vectorized operation"""
    result = pd.to_numeric(series, errors='coerce').fillna(default).astype(int)
    return result


def clean_float_series(series, default=0.0):
    """Clean a float series - vectorized operation"""
    result = pd.to_numeric(series, errors='coerce').fillna(
        default).astype(float)
    return result


def clean_date_series(series):
    """Clean a date series - vectorized operation"""
    return pd.to_datetime(series, errors='coerce')

# STEP 2: TRANSFORM DATA


def transform_data(insurance_df, employee_df, vendor_df):
    """Clean and transform raw data with robust error handling"""
    print("\n🔄 TRANSFORMING DATA...")

    # ---- Clean Insurance Data ----
    print("  • Cleaning insurance data...")

    # Make a copy to avoid warnings
    insurance_df = insurance_df.copy()

    # Handle missing values using vectorized operations
    insurance_df['VENDOR_ID'] = insurance_df['VENDOR_ID'].fillna('UNKNOWN')
    insurance_df['AUTHORITY_CONTACTED'] = insurance_df['AUTHORITY_CONTACTED'].fillna(
        'None')
    insurance_df['POLICE_REPORT_AVAILABLE'] = insurance_df['POLICE_REPORT_AVAILABLE'].fillna(
        0)
    insurance_df['ANY_INJURY'] = insurance_df['ANY_INJURY'].fillna(0)
    insurance_df['CUSTOMER_NAME'] = insurance_df['CUSTOMER_NAME'].fillna(
        'Unknown')
    insurance_df['CITY'] = insurance_df['CITY'].fillna('Unknown')
    insurance_df['STATE'] = insurance_df['STATE'].fillna('Unknown')
    insurance_df['POSTAL_CODE'] = insurance_df['POSTAL_CODE'].fillna(
        'Unknown').astype(str).str[:10]

    # Convert date columns
    date_columns = ['TXN_DATE_TIME', 'POLICY_EFF_DT', 'LOSS_DT', 'REPORT_DT']
    for col in date_columns:
        insurance_df[col] = clean_date_series(insurance_df[col])

    # Calculate Settlement Days (vectorized)
    insurance_df['SETTLEMENT_DAYS'] = (
        insurance_df['REPORT_DT'] - insurance_df['LOSS_DT']).dt.days
    insurance_df['SETTLEMENT_DAYS'] = insurance_df['SETTLEMENT_DAYS'].clip(
        lower=0).fillna(0)
    insurance_df['SETTLEMENT_DAYS'] = clean_int_series(
        insurance_df['SETTLEMENT_DAYS'])

    # Clean numeric columns
    insurance_df['AGE'] = clean_int_series(insurance_df['AGE'], 30)
    insurance_df['TENURE'] = clean_int_series(insurance_df['TENURE'], 0)
    insurance_df['NO_OF_FAMILY_MEMBERS'] = clean_int_series(
        insurance_df['NO_OF_FAMILY_MEMBERS'], 1)
    insurance_df['INCIDENT_HOUR_OF_THE_DAY'] = clean_int_series(
        insurance_df['INCIDENT_HOUR_OF_THE_DAY'], 12)

    # Clean float columns
    insurance_df['PREMIUM_AMOUNT'] = clean_float_series(
        insurance_df['PREMIUM_AMOUNT'])
    insurance_df['CLAIM_AMOUNT'] = clean_float_series(
        insurance_df['CLAIM_AMOUNT'])

    # Create Age Groups - Convert to string first to avoid categorical issues
    bins = [0, 25, 35, 45, 55, 65, 200]
    labels = ['18-25', '26-35', '36-45', '46-55', '56-65', '65+']

    # Create as categorical then convert to string
    age_group_cat = pd.cut(
        insurance_df['AGE'], bins=bins, labels=labels, right=False)
    insurance_df['AGE_GROUP'] = age_group_cat.astype(str)
    insurance_df['AGE_GROUP'] = insurance_df['AGE_GROUP'].replace(
        'nan', 'Unknown')

    # Create Tenure Groups - Convert to string first
    tenure_bins = [0, 12, 24, 36, 60, 120, 10000]
    tenure_labels = ['<1 year', '1-2 years', '2-3 years',
                     '3-5 years', '5-10 years', '10+ years']

    tenure_group_cat = pd.cut(
        insurance_df['TENURE'], bins=tenure_bins, labels=tenure_labels, right=False)
    insurance_df['TENURE_GROUP'] = tenure_group_cat.astype(str)
    insurance_df['TENURE_GROUP'] = insurance_df['TENURE_GROUP'].replace(
        'nan', 'Unknown')

    # Clean text columns
    text_columns = {
        'CUSTOMER_NAME': 50,
        'CUSTOMER_EDUCATION_LEVEL': 50,
        'EMPLOYMENT_STATUS': 20,
        'HOUSE_TYPE': 20,
        'RISK_SEGMENTATION': 10,
        'SOCIAL_CLASS': 10,
        'MARITAL_STATUS': 1,
        'INSURANCE_TYPE': 20,
        'CLAIM_STATUS': 10,
        'INCIDENT_SEVERITY': 20
    }

    for col, max_len in text_columns.items():
        if col in insurance_df.columns:
            insurance_df[col] = clean_text_series(insurance_df[col], max_len)

    # ---- Calculate Business KPIs ----
    print("  • Calculating derived metrics...")

    # Flag unusually high claims (more than 50x premium)
    insurance_df['HIGH_CLAIM_FLAG'] = 0
    mask = (insurance_df['CLAIM_AMOUNT'] > insurance_df['PREMIUM_AMOUNT']
            * 50) & (insurance_df['PREMIUM_AMOUNT'] > 0)
    insurance_df.loc[mask, 'HIGH_CLAIM_FLAG'] = 1

    # ---- Clean Employee Data ----
    print("  • Cleaning employee data...")
    employee_df = employee_df.copy()

    employee_df['DATE_OF_JOINING'] = clean_date_series(
        employee_df['DATE_OF_JOINING'])
    employee_df['TENURE_YEARS'] = (
        (datetime.now() - employee_df['DATE_OF_JOINING']).dt.days / 365).round(1)
    employee_df['TENURE_YEARS'] = employee_df['TENURE_YEARS'].fillna(0)

    # Clean employee text columns
    employee_df['CITY'] = clean_text_series(employee_df['CITY'], 50)
    employee_df['STATE'] = clean_text_series(employee_df['STATE'], 20)
    employee_df['POSTAL_CODE'] = clean_text_series(
        employee_df['POSTAL_CODE'], 10)
    employee_df['AGENT_NAME'] = clean_text_series(
        employee_df['AGENT_NAME'], 50)
    employee_df['AGENT_ID'] = clean_text_series(employee_df['AGENT_ID'], 20)

    # ---- Clean Vendor Data ----
    print("  • Cleaning vendor data...")
    vendor_df = vendor_df.copy()

    vendor_df['CITY'] = clean_text_series(vendor_df['CITY'], 50)
    vendor_df['STATE'] = clean_text_series(vendor_df['STATE'], 20)
    vendor_df['POSTAL_CODE'] = clean_text_series(vendor_df['POSTAL_CODE'], 10)
    vendor_df['VENDOR_NAME'] = clean_text_series(vendor_df['VENDOR_NAME'], 50)
    vendor_df['VENDOR_ID'] = clean_text_series(vendor_df['VENDOR_ID'], 20)

    print("✅ Data transformation complete")
    return insurance_df, employee_df, vendor_df

# STEP 3: CREATE DIMENSION TABLES


def load_dim_customer(conn, insurance_df):
    """Load customer dimension with batch processing"""
    print("\n👤 Loading Dim_Customer...")

    cursor = conn.cursor()

    # Get unique customers
    customer_cols = ['CUSTOMER_ID', 'CUSTOMER_NAME', 'AGE', 'AGE_GROUP', 'MARITAL_STATUS',
                     'EMPLOYMENT_STATUS', 'CUSTOMER_EDUCATION_LEVEL', 'RISK_SEGMENTATION',
                     'HOUSE_TYPE', 'SOCIAL_CLASS', 'NO_OF_FAMILY_MEMBERS', 'TENURE',
                     'TENURE_GROUP', 'CITY', 'STATE', 'POSTAL_CODE']

    customer_df = insurance_df[customer_cols].drop_duplicates(subset=[
                                                              'CUSTOMER_ID'])

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
    total_inserted = 0

    for _, row in customer_df.iterrows():
        record = (
            str(row['CUSTOMER_ID'])[:20],
            str(row['CUSTOMER_NAME'])[:50],
            int(row['AGE']),
            str(row['AGE_GROUP']),
            str(row['MARITAL_STATUS'])[:1],
            str(row['EMPLOYMENT_STATUS'])[:20],
            str(row['CUSTOMER_EDUCATION_LEVEL'])[:50],
            str(row['RISK_SEGMENTATION'])[:10],
            str(row['HOUSE_TYPE'])[:20],
            str(row['SOCIAL_CLASS'])[:10],
            int(row['NO_OF_FAMILY_MEMBERS']),
            int(row['TENURE']),
            str(row['CITY'])[:50],
            str(row['STATE'])[:20],
            str(row['POSTAL_CODE'])[:10],
            today,
            1
        )
        records.append(record)

        if len(records) >= BATCH_SIZE:
            try:
                cursor.executemany(insert_query, records)
                conn.commit()
                total_inserted += len(records)
                print(f"  • Inserted {total_inserted} customers...")
                records = []
            except Error as e:
                print(f"  ⚠️ Batch error: {e}")
                conn.rollback()
                records = []

    # Insert remaining records
    if records:
        try:
            cursor.executemany(insert_query, records)
            conn.commit()
            total_inserted += len(records)
        except Error as e:
            print(f"  ⚠️ Final batch error: {e}")
            conn.rollback()

    print(f"✅ Loaded {total_inserted} customers")


def load_dim_agent(conn, employee_df):
    """Load agent dimension with batch processing"""
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
    total_inserted = 0

    for _, row in employee_df.iterrows():
        join_date = row['DATE_OF_JOINING'].date() if pd.notna(
            row['DATE_OF_JOINING']) else today

        record = (
            str(row['AGENT_ID'])[:20],
            str(row['AGENT_NAME'])[:50],
            join_date,
            float(row['TENURE_YEARS']),
            str(row['CITY'])[:50],
            str(row['STATE'])[:20],
            str(row['POSTAL_CODE'])[:10],
            today,
            1
        )
        records.append(record)

        if len(records) >= BATCH_SIZE:
            try:
                cursor.executemany(insert_query, records)
                conn.commit()
                total_inserted += len(records)
                print(f"  • Inserted {total_inserted} agents...")
                records = []
            except Error as e:
                print(f"  ⚠️ Batch error: {e}")
                conn.rollback()
                records = []

    if records:
        try:
            cursor.executemany(insert_query, records)
            conn.commit()
            total_inserted += len(records)
        except Error as e:
            print(f"  ⚠️ Final batch error: {e}")
            conn.rollback()

    print(f"✅ Loaded {total_inserted} agents")


def load_dim_vendor(conn, vendor_df):
    """Load vendor dimension with batch processing"""
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
    total_inserted = 0

    for _, row in vendor_df.iterrows():
        record = (
            str(row['VENDOR_ID'])[:20],
            str(row['VENDOR_NAME'])[:50],
            str(row['CITY'])[:50],
            str(row['STATE'])[:20],
            str(row['POSTAL_CODE'])[:10],
            today,
            1
        )
        records.append(record)

        if len(records) >= BATCH_SIZE:
            try:
                cursor.executemany(insert_query, records)
                conn.commit()
                total_inserted += len(records)
                print(f"  • Inserted {total_inserted} vendors...")
                records = []
            except Error as e:
                print(f"  ⚠️ Batch error: {e}")
                conn.rollback()
                records = []

    if records:
        try:
            cursor.executemany(insert_query, records)
            conn.commit()
            total_inserted += len(records)
        except Error as e:
            print(f"  ⚠️ Final batch error: {e}")
            conn.rollback()

    print(f"✅ Loaded {total_inserted} vendors")


def load_dim_policy(conn, insurance_df):
    """Load policy dimension with batch processing"""
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
    total_inserted = 0

    for _, row in policy_df.iterrows():
        eff_date = row['POLICY_EFF_DT'].date() if pd.notna(
            row['POLICY_EFF_DT']) else today

        record = (
            str(row['POLICY_NUMBER'])[:20],
            str(row['INSURANCE_TYPE'])[:20],
            float(row['PREMIUM_AMOUNT']),
            eff_date,
            eff_date.year,
            today,
            1
        )
        records.append(record)

        if len(records) >= BATCH_SIZE:
            try:
                cursor.executemany(insert_query, records)
                conn.commit()
                total_inserted += len(records)
                print(f"  • Inserted {total_inserted} policies...")
                records = []
            except Error as e:
                print(f"  ⚠️ Batch error: {e}")
                conn.rollback()
                records = []

    if records:
        try:
            cursor.executemany(insert_query, records)
            conn.commit()
            total_inserted += len(records)
        except Error as e:
            print(f"  ⚠️ Final batch error: {e}")
            conn.rollback()

    print(f"✅ Loaded {total_inserted} policies")


def load_dim_time(conn):
    """Load time dimension with batch processing"""
    print("\n📅 Loading Dim_Time...")

    cursor = conn.cursor()

    # Generate dates for 5 years
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
    total_inserted = 0

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
            1 if current_date.weekday() >= 5 else 0
        ))
        current_date += timedelta(days=1)

        if len(records) >= TIME_BATCH_SIZE:
            try:
                cursor.executemany(insert_query, records)
                conn.commit()
                total_inserted += len(records)
                print(f"  • Inserted {total_inserted} days...")
                records = []
            except Error as e:
                print(f"  ⚠️ Time batch error: {e}")
                conn.rollback()
                records = []

    if records:
        try:
            cursor.executemany(insert_query, records)
            conn.commit()
            total_inserted += len(records)
        except Error as e:
            print(f"  ⚠️ Final time batch error: {e}")
            conn.rollback()

    print(f"✅ Loaded {total_inserted} days")

# STEP 4: CREATE FACT TABLE


def load_fact_claims(conn, insurance_df):
    """Load fact claims table with business KPIs"""
    print("\n📊 Loading Fact_Claims...")

    cursor = conn.cursor()

    # Get dimension keys
    print("  • Loading dimension mappings...")

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
    time_map = {}
    for date, key in cursor.fetchall():
        time_map[str(date)] = key

    print("  • Calculating business KPIs...")

    # Calculate claim statistics by insurance type
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
    total_processed = 0
    fact_batch_size = 1000

    for _, row in insurance_df.iterrows():
        # Get date key
        txn_date = row['TXN_DATE_TIME'].date() if pd.notna(
            row['TXN_DATE_TIME']) else None
        time_key = time_map.get(str(txn_date), None)

        if time_key is None:
            continue

        # Simple fraud flag logic
        fraud_flag = 0
        premium = float(row['PREMIUM_AMOUNT'])
        claim = float(row['CLAIM_AMOUNT'])

        if premium > 0 and claim > premium * 50:
            fraud_flag = 1
        elif int(row['INCIDENT_HOUR_OF_THE_DAY']) in [23, 0, 1, 2, 3, 4] and claim > 5000:
            fraud_flag = 1
        elif str(row['INCIDENT_SEVERITY']) in ['Major Loss', 'Total Loss'] and int(row['POLICE_REPORT_AVAILABLE']) == 0:
            fraud_flag = 1

        if fraud_flag == 1:
            fraud_cases += 1

        record = (
            str(row['TRANSACTION_ID'])[:20],
            customer_map.get(row['CUSTOMER_ID']),
            agent_map.get(row['AGENT_ID']),
            vendor_map.get(row['VENDOR_ID']),
            policy_map.get(row['POLICY_NUMBER']),
            time_key,
            premium,
            claim,
            int(row['SETTLEMENT_DAYS']),
            1,
            str(row['INSURANCE_TYPE']),
            str(row['CLAIM_STATUS']),
            str(row['INCIDENT_SEVERITY']),
            fraud_flag,
            str(row['AUTHORITY_CONTACTED']),
            int(row['ANY_INJURY']),
            int(row['POLICE_REPORT_AVAILABLE']),
            int(row['INCIDENT_HOUR_OF_THE_DAY'])
        )
        records.append(record)

        if len(records) >= fact_batch_size:
            try:
                cursor.executemany(insert_query, records)
                conn.commit()
                total_processed += len(records)
                print(f"  • Inserted {total_processed} claim facts...")
                records = []
            except Error as e:
                print(f"  ⚠️ Fact batch error: {e}")
                conn.rollback()
                records = []

    if records:
        try:
            cursor.executemany(insert_query, records)
            conn.commit()
            total_processed += len(records)
        except Error as e:
            print(f"  ⚠️ Final fact batch error: {e}")
            conn.rollback()

    print(f"✅ Loaded {total_processed} claim facts")
    if total_processed > 0:
        print(
            f"⚠️  Flagged {fraud_cases} potential fraud cases ({fraud_cases/total_processed*100:.1f}%)")

# STEP 5: CREATE BUSINESS KPI VIEWS


def create_kpi_views(conn):
    """Create views for easy KPI access in PowerBI"""
    print("\n👁️ Creating KPI views...")

    cursor = conn.cursor()

    views = [
        """
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
            COALESCE(SUM(f.Claim_Amount) / NULLIF(SUM(f.Premium_Amount), 0) * 100, 0) as Loss_Ratio_Pct,
            SUM(CASE WHEN f.Fraud_Flag = 1 THEN 1 ELSE 0 END) as Fraud_Cases,
            COALESCE(SUM(CASE WHEN f.Fraud_Flag = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 0) as Fraud_Rate_Pct
        FROM Fact_Claims f
        JOIN Dim_Time t ON f.Time_Key = t.Time_Key
        GROUP BY t.Year, t.Month, t.Month_Name, f.Insurance_Type
        """,

        """
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
            COALESCE(SUM(CASE WHEN f.Fraud_Flag = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 0) as Fraud_Rate
        FROM Fact_Claims f
        JOIN Dim_Agent a ON f.Agent_Key = a.Agent_Key
        GROUP BY a.Agent_ID, a.Agent_Name, a.State
        """,

        """
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
        """,

        """
        CREATE OR REPLACE VIEW vw_business_summary AS
        SELECT 
            COUNT(DISTINCT Transaction_ID) as Total_Claims,
            COUNT(DISTINCT Policy_Key) as Total_Policies,
            COUNT(DISTINCT Customer_Key) as Total_Customers,
            COALESCE(SUM(Claim_Amount), 0) as Total_Claims_Paid,
            COALESCE(SUM(Premium_Amount), 0) as Total_Premium_Earned,
            COALESCE(AVG(Claim_Amount), 0) as Avg_Claim_Cost,
            COALESCE(AVG(Settlement_Days), 0) as Avg_Days_To_Settle,
            COALESCE(SUM(Claim_Amount) / NULLIF(SUM(Premium_Amount), 0) * 100, 0) as Overall_Loss_Ratio,
            COALESCE(SUM(CASE WHEN Fraud_Flag = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 0) as Overall_Fraud_Rate
        FROM Fact_Claims
        """
    ]

    for i, view_sql in enumerate(views, 1):
        try:
            cursor.execute(view_sql)
            conn.commit()
            print(f"  • Created view {i}")
        except Error as e:
            print(f"  ⚠️ Error creating view {i}: {e}")

    print("✅ KPI views created")

# MAIN ETL PIPELINE


def run_etl():
    """Main ETL pipeline execution"""
    print("=" * 60)
    print("🏥 INSURANCE DATA WAREHOUSE ETL PIPELINE (FINAL VERSION)")
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
        # Clear existing data
        print("\n🧹 Preparing database...")
        cursor = conn.cursor()
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        tables = ['Fact_Claims', 'Dim_Customer', 'Dim_Agent',
                  'Dim_Vendor', 'Dim_Policy', 'Dim_Time']
        for table in tables:
            try:
                cursor.execute(f"TRUNCATE TABLE {table}")
                print(f"  • Cleared {table}")
            except Error as e:
                print(f"  ⚠️ Could not clear {table}: {e}")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()

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
        try:
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
            print(f"⚠️ Could not fetch summary: {e}")

    except Error as e:
        print(f"❌ Database error: {e}")
        try:
            conn.rollback()
        except:
            pass
    finally:
        try:
            if conn.is_connected():
                cursor.close()
                conn.close()
                print("\n🔒 Database connection closed")
        except:
            pass


# RUN THE ETL
if __name__ == "__main__":
    run_etl()
