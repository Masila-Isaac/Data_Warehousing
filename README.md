created the database running in a local host for now(MySQL to be specific )
working on creating the etl work flow.. teh current one has the lowerside implemented by AI

## Dashboard implementationa nd requirements for now...

loss ratio
claim frequencies
avarage claim cost
avarage settlement time
claim by insurance type
premium to claim ratio

**fraud detection kpis**
fraud rate
fraud agent
fraad vendor

high risk claim rates

### the meassures we will use in kpi

// Core Business KPIs
Total Premium = SUM(Fact_Claims[Premium_Amount])
Total Claims Paid = SUM(Fact_Claims[Claim_Amount])
Total Claim Count = COUNTROWS(Fact_Claims)
Total Customers = DISTINCTCOUNT(Fact_Claims[Customer_Key])

// Key Ratios
Loss Ratio % = DIVIDE([Total Claims Paid], [Total Premium], 0) \* 100
Avg Claim Cost = AVERAGE(Fact_Claims[Claim_Amount])
Avg Settlement Days = AVERAGE(Fact_Claims[Settlement_Days])

// Profitability
Premium per Policy = AVERAGE(Fact_Claims[Premium_Amount])
Claim Frequency = DIVIDE([Total Claim Count], [Total Customers], 0)

// Year-over-Year Growth
Premium YoY Growth =
VAR CurrentYear = YEAR(MAX(Dim_Time[Full_Date]))
VAR PrevYear = CurrentYear - 1
VAR CurrentPremium = CALCULATE([Total Premium], Dim_Time[Year] = CurrentYear)
VAR PrevPremium = CALCULATE([Total Premium], Dim_Time[Year] = PrevYear)
RETURN DIVIDE(CurrentPremium - PrevPremium, PrevPremium, 0)
