-- Create Database
CREATE DATABASE InsuranceDW;
USE InsuranceDW;

-- Create Dimension Tables
CREATE TABLE Dim_Customer (
    Customer_Key INT AUTO_INCREMENT PRIMARY KEY,
    Customer_ID VARCHAR(20),
    Customer_Name VARCHAR(100),
    Age INT,
    Age_Group VARCHAR(20),
    Gender CHAR(1),
    Marital_Status VARCHAR(10),
    Employment_Status VARCHAR(50),
    Education_Level VARCHAR(50),
    Risk_Segmentation VARCHAR(10),
    House_Type VARCHAR(20),
    Social_Class VARCHAR(10),
    No_Of_Family_Members INT,
    Tenure_Months INT,
    City VARCHAR(50),
    State VARCHAR(20),
    Postal_Code VARCHAR(10),
    Effective_Date DATE,
    Expiry_Date DATE,
    Is_Current BOOLEAN
);

CREATE TABLE Dim_Agent (
    Agent_Key INT AUTO_INCREMENT PRIMARY KEY,
    Agent_ID VARCHAR(20),
    Agent_Name VARCHAR(100),
    Date_Of_Joining DATE,
    Tenure_Years INT,
    City VARCHAR(50),
    State VARCHAR(20),
    Postal_Code VARCHAR(10),
    Effective_Date DATE,
    Expiry_Date DATE,
    Is_Current BOOLEAN
);

CREATE TABLE Dim_Vendor (
    Vendor_Key INT AUTO_INCREMENT PRIMARY KEY,
    Vendor_ID VARCHAR(20),
    Vendor_Name VARCHAR(100),
    City VARCHAR(50),
    State VARCHAR(20),
    Postal_Code VARCHAR(10),
    Effective_Date DATE,
    Expiry_Date DATE,
    Is_Current BOOLEAN
);

CREATE TABLE Dim_Policy (
    Policy_Key INT AUTO_INCREMENT PRIMARY KEY,
    Policy_Number VARCHAR(20),
    Insurance_Type VARCHAR(20),
    Premium_Amount DECIMAL(10,2),
    Policy_Effective_Date DATE,
    Policy_Year INT,
    Effective_Date DATE,
    Expiry_Date DATE,
    Is_Current BOOLEAN
);

CREATE TABLE Dim_Time (
    Time_Key INT AUTO_INCREMENT PRIMARY KEY,
    Full_Date DATE,
    Year INT,
    Quarter INT,
    Month INT,
    Month_Name VARCHAR(20),
    Week INT,
    Day_of_Week INT,
    Day_Name VARCHAR(20),
    Is_Weekend BOOLEAN
);

-- Create Fact Table
CREATE TABLE Fact_Claims (
    Claim_Key INT AUTO_INCREMENT PRIMARY KEY,
    Transaction_ID VARCHAR(20),
    Customer_Key INT,
    Agent_Key INT,
    Vendor_Key INT,
    Policy_Key INT,
    Time_Key INT,
    
    -- Measures
    Premium_Amount DECIMAL(10,2),
    Claim_Amount DECIMAL(10,2),
    Settlement_Days INT,
    Claim_Count INT DEFAULT 1,
    
    -- Attributes for analysis
    Insurance_Type VARCHAR(20),
    Claim_Status VARCHAR(10),
    Incident_Severity VARCHAR(20),
    Fraud_Flag BOOLEAN DEFAULT FALSE,
    Authority_Contacted VARCHAR(20),
    Any_Injury BOOLEAN,
    Police_Report_Available BOOLEAN,
    Incident_Hour INT,
    
    -- Foreign Keys
    FOREIGN KEY (Customer_Key) REFERENCES Dim_Customer(Customer_Key),
    FOREIGN KEY (Agent_Key) REFERENCES Dim_Agent(Agent_Key),
    FOREIGN KEY (Vendor_Key) REFERENCES Dim_Vendor(Vendor_Key),
    FOREIGN KEY (Policy_Key) REFERENCES Dim_Policy(Policy_Key),
    FOREIGN KEY (Time_Key) REFERENCES Dim_Time(Time_Key)
);

-- Create Indexes for Performance
CREATE INDEX idx_fact_customer ON Fact_Claims(Customer_Key);
CREATE INDEX idx_fact_agent ON Fact_Claims(Agent_Key);
CREATE INDEX idx_fact_time ON Fact_Claims(Time_Key);
CREATE INDEX idx_fact_policy ON Fact_Claims(Policy_Key);
CREATE INDEX idx_fact_vendor ON Fact_Claims(Vendor_Key);