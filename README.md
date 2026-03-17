# 🏥 Insurance Analytics Data Warehouse Project

## 📊 Project Overview

A complete enterprise data warehouse solution for insurance analytics, enabling real-time business intelligence, fraud detection, and profitability analysis.

---

## ✅ Phase 1: Data Warehouse Foundation

- [x] MySQL database design with star schema
- [x] 5 Dimension tables: Customer, Agent, Vendor, Policy, Time
- [x] 1 Fact table: Claims (10,000+ transactions)
- [x] Optimized indexes and relationships

## ✅ Phase 2: ETL Pipeline Development

- [x] Python ETL scripts with pandas
- [x] Data extraction from 3 CSV sources
- [x] Data cleaning and transformation
- [x] Batch processing (500 records/batch)
- [x] Fraud flag logic implementation
- [x] MySQL Connector/ODBC 8.0.28 configuration

## ✅ Phase 3: KPI View Creation

- [x] `vw_business_summary` - Company health metrics
- [x] `vw_monthly_kpis` - Monthly performance trends
- [x] `vw_agent_performance` - Agent-level analytics
- [x] `vw_customer_segments` - Customer demographics

## ✅ Phase 4: DAX Measures Development

- [x] Core Business KPIs (Premium, Claims, Counts)
- [x] Key Ratios (Loss Ratio, Claim Frequency)
- [x] Averages (Claim Cost, Settlement Days)
- [x] YoY Growth Calculations
- [x] Fraud Detection Metrics
- [x] High Risk Indicators

## ✅ Phase 5: Executive Dashboard (Page 1) - COMPLETE

- [x] **KPI Cards** (6 total):
  - Total Premium ($12.4M)
  - Total Claims Paid ($8.2M)
  - Loss Ratio (66%)
  - Fraud Rate (4.5%)
  - Total Claim Count (10,000)
  - Avg Settlement Days (12 days)
- [x] **Premium YoY Growth** indicator with ▲/▼
- [x] **Monthly Premium vs Claims** line chart
- [x] **Claims by Insurance Type** donut chart
- [x] **Top 5 States by Claims** bar chart
- [x] **Monthly Performance** table
- [x] Interactive Year & Insurance Type slicers

---

## add this to page one Assign measures:

    Card 5: Total Claim Count

    Card 6: Avg Settlement Days

    - Add Premium YoY Growth as a small text below Total Premium:

    Create a Card visual

    -Add measure: Premium YoY Growth

    - Format as percentage with ▲/▼ indicato

## 🚧 Phase 6: Profitability Analysis Dashboard (Page 2) - IN PROGRESS

- [ ] **KPI Cards**:
  - Avg Claim Cost
  - Premium per Policy
  - Claim Frequency
- [ ] **Loss Ratio by Customer Age Group** matrix
- [ ] **Premium vs Claims** scatter plot
- [ ] **Premium to Claim Ratio** trend line
- [ ] **Claims by Policy Type** stacked bar chart
- [ ] **Customer Segmentation** analysis

## ⏳ Phase 7: Fraud Detection Dashboard (Page 3) - PENDING

- [ ] **KPI Cards**:
  - Fraud Rate (with alert colors)
  - High Risk Claim Rate
  - Total Fraud Cases
- [ ] **Top 10 Agents by Fraud Rate** bar chart
- [ ] **Top 10 Vendors by Fraud Cases** bar chart
- [ ] **Time-of-Day Fraud** heat map
- [ ] **Suspicious Claims** table
- [ ] **Fraud Trends** line chart

## ⏳ Phase 8: Agent Performance Dashboard (Page 4) - PENDING

- [ ] **Agent Ranking** by claims handled
- [ ] **Settlement Days** analysis
- [ ] **Agent Territory** map
- [ ] **Performance vs Fraud** scatter plot

## ⏳ Phase 9: Advanced Analytics - PENDING

- [ ] Row-level security implementation
- [ ] Mobile-responsive layout
- [ ] Scheduled data refresh
- [ ] Power BI Service publishing

---

## 🛠️ Technology Stack

| Component           | Technology                  |
| ------------------- | --------------------------- |
| **Database**        | MySQL 8.0.28 (localhost)    |
| **ETL Pipeline**    | Python 3.10, pandas, numpy  |
| **Visualization**   | Power BI Desktop 64-bit     |
| **Connectivity**    | MySQL Connector/ODBC 8.0.28 |
| **Version Control** | Git/GitHub                  |

---

## 📈 Key Performance Indicators

### Business KPIs

| KPI                     | Formula                      | Current Value |
| ----------------------- | ---------------------------- | ------------- |
| **Loss Ratio**          | Claims Paid / Premium Earned | 66%           |
| **Claim Frequency**     | # Claims / # Customers       | 2.1           |
| **Avg Claim Cost**      | Total Claims / Claim Count   | $820          |
| **Avg Settlement Days** | Avg days from loss to report | 12 days       |
| **Premium per Policy**  | Avg premium per policy       | $1,240        |
| **Premium YoY Growth**  | Year-over-year growth        | +8.5%         |

### Fraud KPIs

| KPI                  | Formula                         | Current Value          |
| -------------------- | ------------------------------- | ---------------------- |
| **Fraud Rate**       | Fraud Claims / Total Claims     | 4.5%                   |
| **High Risk Rate**   | High Risk Claims / Total Claims | 3.2%                   |
| **Top Agent Fraud**  | Agent with highest fraud rate   | Agent #0042 (12%)      |
| **Top Vendor Fraud** | Vendor with most fraud cases    | Vendor #0017 (8 cases) |

---

## 📁 Repository Structure

---

## 🎯 Sample DAX Measures

```dax
-- Core Business KPIs
Total Premium = SUM(Fact_Claims[Premium_Amount])
Total Claims Paid = SUM(Fact_Claims[Claim_Amount])
Total Claim Count = COUNTROWS(Fact_Claims)
Total Customers = DISTINCTCOUNT(Fact_Claims[Customer_Key])

-- Key Ratios
Loss Ratio % = DIVIDE([Total Claims Paid], [Total Premium], 0) * 100
Avg Claim Cost = AVERAGE(Fact_Claims[Claim_Amount])
Avg Settlement Days = AVERAGE(Fact_Claims[Settlement_Days])
Claim Frequency = DIVIDE([Total Claim Count], [Total Customers], 0)

-- Fraud Metrics
Fraud Rate =
DIVIDE(
    CALCULATE(COUNTROWS(Fact_Claims), Fact_Claims[Fraud_Flag] = 1),
    COUNTROWS(Fact_Claims),
    0
) * 100

-- YoY Growth
Premium YoY Growth =
VAR CurrentYear = YEAR(MAX(Dim_Time[Full_Date]))
VAR PrevYear = CurrentYear - 1
VAR CurrentPremium = CALCULATE([Total Premium], Dim_Time[Year] = CurrentYear)
VAR PrevPremium = CALCULATE([Total Premium], Dim_Time[Year] = PrevYear)
RETURN DIVIDE(CurrentPremium - PrevPremium, PrevPremium, 0)
```

## 📊 Project Status

| Phase                      | Status         | Completion |
| -------------------------- | -------------- | ---------- |
| 1-3: Foundation & ETL      | ✅ Complete    | 100%       |
| 4: DAX Measures            | ✅ Complete    | 100%       |
| 5: Executive Dashboard     | ✅ Complete    | 100%       |
| 6: Profitability Dashboard | 🚧 In Progress | 25%        |
| 7: Fraud Detection         | ⏳ Pending     | 0%         |
| 8: Agent Performance       | ⏳ Pending     | 0%         |
| 9: Advanced Analytics      | ⏳ Pending     | 0%         |

**Overall Progress: 70% Complete**

---

## 👨‍💻 Author

**Your Name**

- GitHub: [@masila-isaac]
- LinkedIn: [https://www.linkedin.com/in/isaac-masila-6266752b8/]
- Email: masilaisaacmim@gmail.com

---

## 🙏 Acknowledgments

- Kaggle Insurance Claims Fraud Dataset
- MySQL Community Edition
- Power BI Community
- Open-source contributors

---

### Coming Soon:

- Profitability Analysis Dashboard
- Fraud Detection Dashboard
- Agent Performance Dashboard

---

## 📌 Next Immediate Steps

1. ✅ Complete Executive Dashboard (Page 1)
2. 🔄 Add missing KPI cards (Claim Count, Settlement Days)
3. 🔄 Create Profitability Dashboard (Page 2)
4. 🔄 Add Premium YoY Growth indicator
5. 📅 Build Fraud Detection Dashboard (Page 3)
6. 📅 Implement Agent Performance views (Page 4)

---

**Last Updated:** March 11, 2026
**Current Version:** v1.0.0

```

```

/// average settlements
Avg Settlement Days =
AVERAGE(Fact_Claims[Settlement_Days])
