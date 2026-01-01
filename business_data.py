
# Importing the necessary files 

from sqlalchemy import create_engine, text
import pandas as pd
import plotly.express as px

# Setting up the MySQL environment
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]

connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string) 

# Query 1

query1 = text("""SELECT Year, SUM(Transaction_count) AS 'Total Number of Transactions', 
              SUM(Transaction_amount) AS 'Total Transaction Amount' 
              FROM agg_trans 
              GROUP BY Year 
              ORDER BY Year;""")

with engine.connect() as conn:
    df_trans_value_growth = pd.read_sql(query1, conn)

df_trans_value_growth["Transaction Number Growth (%)"] = df_trans_value_growth["Total Number of Transactions"].pct_change()*100
df_trans_value_growth["Transaction Amount Growth (%)"] = df_trans_value_growth["Total Transaction Amount"].pct_change()*100

import plotly.express as px 
df_trans_value_growth_fil = df_trans_value_growth[df_trans_value_growth["Year"]!=2018]
fig1 = px.bar(df_trans_value_growth_fil, x = "Year", y = ["Transaction Number Growth (%)","Transaction Amount Growth (%)"],
             barmode = "group", labels = {"value":"Values", "variable":"Metric", "Year":"Year"}, 
             title = "Growth in Transaction Volume Over Years",
             color_discrete_sequence = ["#636EFA","#EF553B"])

fig1.update_xaxes(tickmode = "array", tickvals = [2019, 2020, 2021, 2022, 2023, 2024], 
                  ticktext = ["2019","2020","2021","2022","2023","2024"])
fig1.update_yaxes(tickformat = ",")

# Query 2 

query2 = text("""WITH YoYGrowth AS (
                 SELECT State, Year, 
                 ROUND((SUM(Transaction_count) - LAG(SUM(Transaction_count)) OVER (PARTITION BY State ORDER BY Year)) * 100.0 /
                 NULLIF(LAG(SUM(Transaction_count)) OVER (PARTITION BY State ORDER BY Year), 0), 2) AS YoY_Transaction_Growth_Percent
                 FROM agg_trans
                 GROUP BY State, Year),
                 RankedGrowth AS (
                 SELECT State, Year, YoY_Transaction_Growth_Percent, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY YoY_Transaction_Growth_Percent DESC) AS rn_desc, 
                 ROW_NUMBER() OVER (PARTITION BY Year ORDER BY YoY_Transaction_Growth_Percent ASC) AS rn_asc
                 FROM YoYGrowth
                 WHERE YoY_Transaction_Growth_Percent IS NOT NULL),
                 TopBottomStates AS (
                 SELECT DISTINCT State FROM RankedGrowth WHERE rn_desc <= 3 OR rn_asc <= 3)
                 SELECT yg.State, AVG(yg.YoY_Transaction_Growth_Percent) as 'Average Transaction Growth Percent'
                 FROM YoYGrowth yg
                 JOIN TopBottomStates tbs ON yg.State = tbs.State
                 WHERE Year >= 2019
                 GROUP BY State
                 ORDER BY AVG(yg.YoY_Transaction_Growth_Percent) DESC
                 LIMIT 5;""")

with engine.connect() as conn:
    df_state_yoy_growth = pd.read_sql(query2, conn)

df_state_yoy_growth.fillna(0, inplace = True)

fig2 = px.bar(df_state_yoy_growth, x = "State", y = "Average Transaction Growth Percent",
              color = "State",
               color_discrete_sequence = px.colors.qualitative.Plotly,
               labels = {"Average Transaction Growth Percent": "Average Transaction Growth (%)",
                         "State":"State"}, title = "Top 5 States with Highest Average Year on Year Transaction Growth")
fig2.update_layout(height = 500)

# Query 3

query3 = text("""WITH state_year_summary AS (
                 SELECT State, Year, SUM(Transaction_count) AS TotalTransactions
                 FROM agg_trans WHERE Year BETWEEN 2020 AND 2024 
                 GROUP BY State, Year),
                 growth AS (
                 SELECT s.State, s.Year, s.TotalTransactions, 
                 LAG(s.TotalTransactions) OVER (PARTITION BY s.State ORDER BY s.Year) AS PreviousYearTransactions
                 FROM state_year_summary s),
                 declining_states AS (
                 SELECT State, ROUND((TotalTransactions - PreviousYearTransactions) * 100.0 / PreviousYearTransactions, 2)
                 AS YoY_Transaction_Growth_Percent
                 FROM growth
                 WHERE PreviousYearTransactions IS NOT NULL
                 AND Year = 2024
                 ORDER BY YoY_Transaction_Growth_Percent ASC
                 LIMIT 5)
                 SELECT g.State, g.Year, ROUND((g.TotalTransactions - g.PreviousYearTransactions) * 100.0 / g.PreviousYearTransactions,2)
                 AS "YoY Transaction Growth (%)"
                 FROM growth g
                 JOIN declining_states d ON g.State = d.State
                 WHERE g.PreviousYearTransactions IS NOT NULL
                 ORDER BY g.State, g.Year;""")

with engine.connect() as conn:
    df_trans_growth_decline = pd.read_sql(query3, conn)

fig3 = px.line(df_trans_growth_decline, x = "Year", y = "YoY Transaction Growth (%)", color = "State",
               markers = True,
               color_discrete_sequence = px.colors.qualitative.Plotly,
               labels = {"State":"Regions","YoY Transaction Growth (%)":"YoY Transaction Growth (%)"},
               title = "Top 5 Regions Showing Most Decline in Transaction Growth from 2021-2024")
fig3.update_xaxes(tickmode = "array", tickvals = [2019, 2020, 2021, 2022, 2023, 2024], 
                  ticktext = ["2019","2020","2021","2022","2023","2024"])
fig3.update_traces(mode = "lines + markers", marker = dict(size = 6))

# Query 4 

query4 = text("""WITH quarterly AS (
              SELECT Year, Quarter, SUM(Transaction_count) AS TotalTransactions
              FROM agg_trans
              WHERE Year BETWEEN 2018 AND 2024
              GROUP BY Year, Quarter),
              with_prev AS (
              SELECT Year, Quarter, TotalTransactions, LAG(TotalTransactions) OVER (ORDER BY Year, Quarter) AS PrevTotalTransactions,
              CASE
               WHEN LAG(TotalTransactions) OVER (ORDER BY Year, Quarter) IS NULL THEN NULL
               WHEN LAG(TotalTransactions) OVER (ORDER BY Year, Quarter) = 0 THEN NULL
               ELSE (TotalTransactions - LAG(TotalTransactions) OVER (ORDER BY Year, Quarter)) * 100.0 / LAG(TotalTransactions) OVER (ORDER BY Year, Quarter)
              END AS TransactionSpikePct
              FROM quarterly)
              SELECT Year, Quarter AS 'Quarter With Max Pct Spike', TotalTransactions as 'Total Transactions', 
              PrevTotalTransactions as 'Prev Total Transactions', ROUND(TransactionSpikePct,2) AS 'Spike Pct'
              FROM (SELECT *,
              ROW_NUMBER() OVER (PARTITION BY Year ORDER BY CASE WHEN TransactionSpikePct IS NULL THEN 1 ELSE 0 END, TransactionSpikePct DESC) 
              AS rn
              FROM with_prev) t
              WHERE rn = 1
              ORDER BY Year;""")

with engine.connect() as conn:
    df_quarter_spike = pd.read_sql(query4, conn)

df_quarter_spike["Quarter With Max Pct Spike"] = df_quarter_spike["Quarter With Max Pct Spike"].astype(str)
fig4 = px.bar(df_quarter_spike, x = "Year", y = "Spike Pct", color = "Quarter With Max Pct Spike",
              labels = {"Year":"Year","Spike Pct":"Transaction Spike (%)","Quarter With Max Pct Spike":"Quarter"},
              color_discrete_sequence = px.colors.qualitative.Plotly,
              title = "Quarters with Highest Transaction Spike in Each Year")

# Query 5

query5 = text("""WITH yearly_totals AS (
                 SELECT Year, SUM(Transaction_amount) AS TotalTransactionAmount
                 FROM agg_trans
                 GROUP BY Year),
                 type_share AS (
                 SELECT a.Year, a.Transaction_type AS TransactionType, SUM(a.Transaction_amount) AS TransactionAmount,
                 SUM(a.Transaction_amount) * 100.0 / y.TotalTransactionAmount AS SharePct
                 FROM agg_trans a
                 JOIN yearly_totals y ON a.Year = y.Year
                 GROUP BY a.Year, a.Transaction_type, y.TotalTransactionAmount)
                 SELECT TransactionType AS "Transaction Type", ROUND(AVG(SharePct), 2) AS "Average Share Pct"
                 FROM type_share
                 GROUP BY TransactionType;""")

with engine.connect() as conn:
    df_trans_type_high_share = pd.read_sql(query5, conn)

fig5 = px.pie(df_trans_type_high_share, names = "Transaction Type", values = "Average Share Pct", 
              color = "Transaction Type",
              color_discrete_sequence = px.colors.qualitative.Plotly,
              title = "Percentage Share of All Transactions By Each Payment Type")

fig5.update_traces(textposition = "outside", pull = 0.1)
fig5.update_layout(width = 600, height = 500, uniformtext_minsize = 14, uniformtext_mode = "show")

# Query 6

query6 = text("""WITH brand_users AS (
                 SELECT Brand_name AS Brandname, SUM(User_count) AS Totalusers 
                 FROM agg_user
                 GROUP BY Brand_name),
                 ranked AS (
                 SELECT Brandname, Totalusers,
                 RANK() OVER (ORDER BY Totalusers DESC) AS rank_highest,
                 RANK() OVER (ORDER BY Totalusers ASC) AS rank_lowest
                 FROM brand_users)
                 SELECT Brandname, Totalusers 
                 FROM ranked 
                 WHERE rank_highest <= 3 OR rank_lowest <= 3
                 ORDER BY Totalusers DESC;""")

with engine.connect() as conn:
    df_device_brand_users = pd.read_sql(query6, conn)

df_device_brand_users = df_device_brand_users.rename(columns = {"Brandname":"Brand Name", 
                                                                "Totalusers":"Total Users"})
import plotly.graph_objects as go
from plotly.subplots import make_subplots

top3_df = df_device_brand_users.iloc[0:3]
bottom3_df = df_device_brand_users.iloc[3:]

palette = px.colors.qualitative.Plotly

top3_colors = [palette[i % len(palette)] for i in range(len(top3_df))]
bottom3_colors = [palette[i % len(palette)] for i in range(len(bottom3_df))]

fig6 = make_subplots(rows = 1, cols = 2, subplot_titles = ("Top 3 Mobile Brands",
                                                           "Bottom 3 Mobile Brands"))
fig6.add_trace(go.Bar(x = top3_df["Brand Name"], y = top3_df["Total Users"],
                      marker_color = top3_colors), row = 1, col = 1)
fig6.add_trace(go.Bar(x = bottom3_df["Brand Name"], y = bottom3_df["Total Users"],
                     marker_color = bottom3_colors), row = 1, col = 2)

fig6.update_layout(title_text = "Total Number of PhonePe Users For Each Device Brand", showlegend = False)

# Query 7

query7 = text("""WITH app_engagement AS (
                SELECT State AS "State", ROUND(AVG(Registered_users), 2) AS AvgRegUsers, ROUND(AVG(Number_of_app_opens), 2) AS AvgAppOpens
                FROM map_user
                GROUP BY State),
                engagement_rate AS (
                SELECT State, ROUND(AvgAppOpens / AvgRegUsers, 2) AS EngagementRate 
                FROM app_engagement)
                (SELECT * FROM engagement_rate
                ORDER BY EngagementRate DESC
                LIMIT 3)
                UNION ALL
                (SELECT * FROM engagement_rate ORDER BY EngagementRate ASC LIMIT 3)
                ORDER BY EngagementRate DESC;""")

with engine.connect() as conn:
    df_state_user_eng_rate = pd.read_sql(query7, conn)

df_state_user_eng_rate = df_state_user_eng_rate.rename(columns = {"EngagementRate":"Engagement Rate"})

top3_eng = df_state_user_eng_rate.iloc[0:3]
bottom3_eng = df_state_user_eng_rate.iloc[3:]

palette = px.colors.qualitative.Plotly

top3_colors = [palette[i % len(palette)] for i in range(len(top3_eng))]
bottom3_colors = [palette[i % len(palette)] for i in range(len(bottom3_eng))]

fig7 = make_subplots(rows = 1, cols = 2, subplot_titles = ("Top 3 Regions",
                                                           "Bottom 3 Regions"))
fig7.add_trace(go.Bar(x = top3_eng["State"], y = top3_eng["Engagement Rate"],
                      marker_color = top3_colors), row = 1, col = 1)
fig7.add_trace(go.Bar(x = bottom3_eng["State"], y = bottom3_eng["Engagement Rate"],
                     marker_color = bottom3_colors), row = 1, col = 2)

fig7.update_layout(title_text = "PhonePe App Engagement Rates For Each Region", showlegend = False)

# Query 8

query8 = text("""WITH quarterly_engagement AS (
                 SELECT Year, Quarter, ROUND(SUM(Number_of_app_opens)/SUM(Registered_users), 4) AS EngagementRate
                 FROM map_user
                 GROUP BY Year, Quarter),
                 ranked AS (
                 SELECT Year, Quarter, EngagementRate,
                 RANK() OVER (PARTITION BY Year ORDER BY EngagementRate DESC) AS rank_highest,
                 RANK() OVER (PARTITION BY Year ORDER BY EngagementRate ASC) AS rank_lowest
                 FROM quarterly_engagement)
                 SELECT Year, Quarter, EngagementRate as 'Engagement Rate' FROM ranked
                 WHERE rank_highest = 1 OR rank_lowest = 1
                 ORDER BY Year, Quarter, EngagementRate ASC;""")

with engine.connect() as conn:
    df_quarter_user_eng_rate = pd.read_sql(query8, conn)

df_quarter_user_eng_rate = df_quarter_user_eng_rate[df_quarter_user_eng_rate["Year"]!=2018]
df_quarter_user_eng_rate["Quarter"] = df_quarter_user_eng_rate["Quarter"].astype(str)
df_quarter_user_eng_rate = df_quarter_user_eng_rate.sort_values(["Year","Engagement Rate"], ascending = [True, True])
fig8 = px.bar(df_quarter_user_eng_rate, x = "Year", y = "Engagement Rate",
             barmode = "group", labels = {"Year":"Year", "Quarter":"Quarter", "Engagement Rate":"Engagement Rate"}, 
             color = "Quarter",
             color_discrete_sequence = px.colors.qualitative.Plotly,
             title = "Highest and Lowest PhonePe App User Engagement Rate Per Year")

# Query 9

query9 = text("""WITH yearly_insurance AS (
                 SELECT Year, SUM(Insurance_count) AS TotalInsurance, SUM(Insurance_amount) AS TotalValue
                 FROM agg_ins
                 WHERE Year BETWEEN 2020 AND 2024
                 GROUP BY Year),
                 growth AS (
                 SELECT Year, TotalInsurance, TotalValue, LAG(TotalInsurance) OVER (ORDER BY Year) AS PrevTransactions,
                 LAG(TotalValue) OVER (ORDER BY Year) AS PrevValue
                 FROM yearly_insurance)
                 SELECT Year, TotalInsurance as "No of Insurance Transactions", TotalValue "Total Insurance Amount",
                 ROUND((TotalInsurance - PrevTransactions) * 100.0 / PrevTransactions, 2) AS "Insurance Transaction Growth (%)",
                 ROUND((TotalValue - PrevValue) * 100.0 / PrevValue, 2) AS "Insurance Amount Growth (%)"
                 FROM growth
                 WHERE PrevTransactions IS NOT NULL AND PrevValue IS NOT NULL
                 ORDER BY Year;""")

with engine.connect() as conn:
    df_ins_growth_each_year = pd.read_sql(query9, conn)

fig9 = px.bar(df_ins_growth_each_year, x = "Year", y = ["Insurance Transaction Growth (%)", "Insurance Amount Growth (%)"],
              barmode = "group",
              color_discrete_sequence = px.colors.qualitative.Plotly,
              labels = {"Year":"Year","Insurance Transaction Growth (%)":"Insurance Transaction Growth (%)",
                        "Insurance Amount Growth (%)":"Insurance Amount Growth (%)","variable":"Metric", 
                        "value":"Growth (%)"},
             title = "Growth in Number Of Transactions and Total Insurance Transactions Over The Years")

# Query 10

query10 = text("""WITH yearly_totals AS (
                  SELECT State, Year, SUM(Insurance_amount) AS total_value FROM agg_ins
                  GROUP BY state, year)
                  SELECT State, (MAX(total_value) - MIN(total_value)) AS InsuranceTransactionValue
                  FROM yearly_totals
                  GROUP BY state
                  ORDER BY InsuranceTransactionValue DESC
                  LIMIT 5;""")

with engine.connect() as conn:
    df_high_insurance_trans = pd.read_sql(query10, conn)

df_high_insurance_trans["InsuranceTransactionValue"] = df_high_insurance_trans["InsuranceTransactionValue"]/1e7

df_high_insurance_trans = df_high_insurance_trans.rename(columns = 
                                                         {"InsuranceTransactionValue":"Insurance Transaction Value (in Cr)"})

fig10 = px.bar(df_high_insurance_trans, x = "State", y = "Insurance Transaction Value (in Cr)",
               color = "State",
               color_discrete_sequence = px.colors.qualitative.Plotly,
               labels = {"State":"State","Insurance Transaction Value (in Cr)":"Insurance Transaction Value (in Cr)"},
               title = "Top 5 States with Highest Insurance Transaction Value Over The Years")

# Query 11

query11 = text("""WITH total_activity AS (
                  SELECT state, SUM(Transaction_count) AS total_txn_count, SUM(Transaction_amount) AS total_txn_value
                  FROM agg_trans
                  GROUP BY state),
                  insurance_activity AS (
                  SELECT State, SUM(Insurance_count) AS total_insurance_count, SUM(Insurance_amount) AS total_insurance_value
                  FROM agg_ins
                  GROUP BY State),
                  combined AS (
                  SELECT t.State, t.total_txn_count, t.total_txn_value, i.total_insurance_count, i.total_insurance_value,
                  ROUND((i.total_insurance_count * 100.0 / NULLIF(t.total_txn_count, 0)), 5) AS insurance_penetration_rate,
                  ROUND((i.total_insurance_value * 100.0 / NULLIF(t.total_txn_value, 0)), 5) AS insurance_value_share
                  FROM total_activity t
                  LEFT JOIN insurance_activity i ON t.state = i.state)
                  SELECT State, total_txn_count as 'Total Transactions', total_txn_value as 'Total Transaction Amount', 
                  total_insurance_count as 'Total Insurances', total_insurance_value as 'Total Insurance Amount', 
                  insurance_penetration_rate as "Insurance Penetration Rate", 
                  insurance_value_share as "Insurance Value Share"
                  FROM combined
                  WHERE insurance_penetration_rate IS NOT NULL
                  ORDER BY insurance_penetration_rate ASC
                  limit 5;""")

with engine.connect() as conn:
    df_untapped_region = pd.read_sql(query11, conn)

fig11 = px.bar(df_untapped_region, x = "State", y = ["Insurance Penetration Rate"], 
               color = 'State',
               color_discrete_sequence = px.colors.qualitative.Plotly, 
               labels = {"State": "State","variable": "Metric","value": "Insurance Penetration Rate"},
               title = "Untapped States - High Total Transaction Values But Relatively Low Insurance Penetration")

# Query 12

query12 = text("""WITH yearly_user_growth AS (
                  SELECT state, year, SUM(Registered_users) AS yearly_registered
                  FROM map_user
                  GROUP BY state, year),
                  user_growth_rate AS (
                  SELECT state, year, yearly_registered, LAG(yearly_registered) OVER (PARTITION BY state ORDER BY year) AS prev_registered,
                  ROUND((yearly_registered - LAG(yearly_registered) OVER (PARTITION BY state ORDER BY year)) * 100.0 / LAG(yearly_registered) OVER (PARTITION BY state ORDER BY year), 2) AS reg_growth_pct
                  FROM yearly_user_growth),
                  yearly_txn_growth AS (
                  SELECT state, year, SUM(transaction_count) AS yearly_txns
                  FROM agg_trans GROUP BY state, year),
                  txn_growth_rate AS (
                  SELECT state, year, yearly_txns, LAG(yearly_txns) OVER (PARTITION BY state ORDER BY year) AS prev_txns,
                  ROUND((yearly_txns - LAG(yearly_txns) OVER (PARTITION BY state ORDER BY year)) * 100.0 / LAG(yearly_txns) OVER (PARTITION BY state ORDER BY year), 2) AS txn_growth_pct
                  FROM yearly_txn_growth),
                  combined AS (
                  SELECT u.state, u.year, u.reg_growth_pct, t.txn_growth_pct FROM user_growth_rate u
                  JOIN txn_growth_rate t 
                  ON u.state = t.state AND u.year = t.year
                  WHERE u.prev_registered IS NOT NULL AND t.prev_txns IS NOT NULL)
                  SELECT state, ROUND(AVG(reg_growth_pct), 2) AS avg_user_growth_pct, 
                  ROUND(AVG(txn_growth_pct), 2) AS avg_txn_growth_pct
                  FROM combined GROUP BY state HAVING AVG(reg_growth_pct) > 0 AND AVG(txn_growth_pct) > 0
                  ORDER BY avg_txn_growth_pct DESC LIMIT 10;""")

with engine.connect() as conn:
    df_state_consistent_growth = pd.read_sql(query12, conn)

df_state_consistent_growth = df_state_consistent_growth.rename(columns = {"state":"State", 
                                                                          "avg_user_growth_pct":"Average User Growth (%)",
                                                                          "avg_txn_growth_pct":"Average Transaction Growth (%)"})

fig12 = px.bar(df_state_consistent_growth, x = "State", y = ["Average User Growth (%)", "Average Transaction Growth (%)"], 
               barmode = "group", color_discrete_sequence = px.colors.qualitative.Plotly, 
               labels = {"State": "State","variable": "Metric","value": "Growth (%)"},
               title = "States Showing Consistent Growth in User Registration and Repeat Transaction")

# Query 13

query13 = text("""WITH district_metrics AS (
                  SELECT State, District_name, SUM(Registered_users) AS total_registered_users,
                  SUM(Number_of_app_opens) AS total_app_opens
                  FROM map_user
                  GROUP BY State, District_name),
                  state_totals AS (
                  SELECT State, SUM(total_registered_users) AS state_total_registered,
                  SUM(total_app_opens) AS state_total_app_opens
                  FROM district_metrics
                  GROUP BY State),
                  joined_data AS (
                  SELECT dm.State, dm.District_name, dm.total_registered_users, dm.total_app_opens,
                  ROUND((dm.total_app_opens / st.state_total_app_opens) * 100, 2) AS app_open_share_percent
                  FROM district_metrics dm 
                  JOIN state_totals st ON dm.State = st.State),
                  state_ranking AS (
                  SELECT State, SUM(total_registered_users) AS state_registered_users
                  FROM district_metrics
                  GROUP BY State),
                  top3_states AS (
                  SELECT State FROM state_ranking ORDER BY state_registered_users DESC LIMIT 3),
                  ranked_districts AS (
                  SELECT jd.*, ROW_NUMBER() OVER (PARTITION BY State ORDER BY total_registered_users DESC) AS district_rank
                  FROM joined_data jd
                  WHERE jd.State IN (SELECT State FROM top3_states))
                  SELECT State, District_name AS 'District Name', total_app_opens AS 'Total App Opens',
                  app_open_share_percent AS 'App Open Share'
                  FROM ranked_districts
                  WHERE district_rank <= 5
                  ORDER BY State, district_rank;""")

with engine.connect() as conn:
    df_district_metrics = pd.read_sql(query13, conn)

import plotly.express as px

state_pie_charts = {}
states = df_district_metrics['State'].unique()

for state in states:
    df_state = df_district_metrics[df_district_metrics['State'] == state]
    
    fig13 = px.pie(
        df_state,
        values = 'App Open Share',
        names = 'District Name',
        title = f'App Open Share Percent for {state}',
        hole = 0.3 
    )
    fig13.update_traces(textposition ='inside', textinfo='percent+label')
    state_pie_charts[state] = fig13

# Query 14

query14 = text("""WITH state_insurance AS (
                  SELECT state, SUM(insurance_amount) AS total_insurance_amount FROM top_ins 
                  WHERE year = 2024
                  GROUP BY state)
                  SELECT state, ROUND(total_insurance_amount, 2) AS total_insurance_amount
                  FROM state_insurance 
                  ORDER BY total_insurance_amount DESC 
                  LIMIT 3;""")

with engine.connect() as conn:
    df_high_total_trans_region = pd.read_sql(query14, conn)

df_high_total_trans_region = df_high_total_trans_region.rename(columns = {"state":"State",
                                                                        "total_insurance_amount":"Total Insurance Amount"})

df_high_total_trans_region = df_high_total_trans_region.sample(frac = 1).reset_index(drop = True)
fig14 = px.bar(df_high_total_trans_region, x = "State", y = "Total Insurance Amount", 
               color = "State",
               color_discrete_sequence = px.colors.qualitative.Plotly, 
               labels = {"State": "Regions","Total Insurance Amount":"Total Insurance Amount"},
               title = "Top 3 Regions Recording the Highest Total Insurance Transaction Amount in 2024")

# Query 15

query15 = text("""SELECT year, quarter, total_insurance_trans_volume FROM (
                  SELECT year, quarter, SUM(insurance_amount) AS total_insurance_trans_volume,
                  RANK() OVER (PARTITION BY year ORDER BY SUM(insurance_amount) DESC) AS rnk
                  FROM top_ins GROUP BY year, quarter) ranked
                  WHERE rnk = 1;""")

with engine.connect() as conn:
    df_y_q_high_trans = pd.read_sql(query15, conn)

df_y_q_high_trans = df_y_q_high_trans.rename(columns = {"year":"Year","quarter":"Quarter",
                                                        "total_insurance_trans_volume":"Total Insurance Trans Volume"})

df_y_q_high_trans["Quarter"] = df_y_q_high_trans["Quarter"].astype(str)
fig15 = px.bar(df_y_q_high_trans, x = "Year", y = "Total Insurance Trans Volume",
               color = "Quarter", 
               color_discrete_sequence = px.colors.qualitative.Plotly,
               labels = {"State": "State","Total Insurance Trans Volume":"Total Insurance Trans Volume","Quarter":"Quarter"},
               title = "Year and Quarter Combinations With Highest Total Insurance Transaction Volume")

# Query 16

query16 = text("""SELECT district_name, SUM(insurance_amount) AS total_insurance_value
                  FROM map_ins WHERE year = 2024
                  GROUP BY district_name
                  ORDER BY total_insurance_value DESC
                  LIMIT 5;""")

with engine.connect() as conn:
    df_top5_districts_ins = pd.read_sql(query16, conn)

df_top5_districts_ins = df_top5_districts_ins.rename(columns = {"district_name":"District Name", 
                                                                  "total_insurance_value":"Total Insurance Volume"})

fig16 = px.bar(df_top5_districts_ins, x = "District Name", y = "Total Insurance Volume",
               color = "District Name",
               color_discrete_sequence = px.colors.qualitative.Plotly,
               labels = {"District Name":"District Name","Total Insurance Volume":"Total Insurance Volume"},
               title = "Top 5 Districts With Highest Total Insurance Transaction Volume In 2024")

# Query 17

query17 = text("""WITH yearly_pin_data AS (
                  SELECT pincode, year, SUM(insurance_count) AS yearly_transaction_count
                  FROM top_ins GROUP BY pincode, year),
                  growth_calc AS (
                  SELECT p1.pincode, (p2.yearly_transaction_count - p1.yearly_transaction_count) AS growth_in_count,
                  p2.year
                  FROM yearly_pin_data p1
                  JOIN yearly_pin_data p2
                  ON p1.pincode = p2.pincode
                  AND p2.year = p1.year + 1)
                  SELECT pincode, growth_in_count AS growth_from_prev_year
                  FROM growth_calc
                  WHERE year = 2024
                  ORDER BY growth_from_prev_year DESC
                  LIMIT 5;""")

with engine.connect() as conn:
    df_pincode_ins_trans = pd.read_sql(query17, conn)

df_pincode_ins_trans = df_pincode_ins_trans.rename(columns = {"pincode":"Pincode",
                                                              "growth_from_prev_year":"Growth From Prev Year"})

df_pincode_ins_trans["Pincode"] = df_pincode_ins_trans["Pincode"].astype(int)
df_pincode_ins_trans["Growth From Prev Year"] = df_pincode_ins_trans["Growth From Prev Year"].astype(int)

df_pincode_ins_trans["Pincode"] = df_pincode_ins_trans["Pincode"].astype(str) 
fig17 = px.bar(df_pincode_ins_trans, x = "Pincode", y = "Growth From Prev Year", 
               color = "Pincode",
               color_discrete_sequence = px.colors.qualitative.Plotly,
               labels = {"Pincode":"Pincode","Growth From Prev Year":"Growth From Previous Year"},
               title = "Top 5 Pincodes With Highest Growth in Insurance Transactions in 2024")

fig17.update_layout(xaxis_type = "category", bargap = 0.2)

