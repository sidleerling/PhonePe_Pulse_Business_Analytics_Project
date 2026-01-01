import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import text
import streamlit as st


@st.cache_data(show_spinner=False)
def load_business_figures(engine):
    figs = {}

    # ======================================================
    # QUERY 1
    # ======================================================
    q1 = text("""
        SELECT Year,
               SUM(Transaction_count) AS total_txn,
               SUM(Transaction_amount) AS total_amt
        FROM agg_trans
        GROUP BY Year
        ORDER BY Year;
    """)
    df = pd.read_sql(q1, engine)
    df["txn_growth"] = df["total_txn"].pct_change() * 100
    df["amt_growth"] = df["total_amt"].pct_change() * 100
    df = df[df["Year"] != 2018]

    figs["fig1"] = px.bar(
        df,
        x="Year",
        y=["txn_growth", "amt_growth"],
        barmode="group",
        title="Growth in Transaction Volume Over Years"
    )

    # ======================================================
    # QUERY 2
    # ======================================================
    q2 = text("""
        WITH y AS (
            SELECT State, Year,
            ROUND(
                (SUM(Transaction_count) -
                 LAG(SUM(Transaction_count)) OVER (PARTITION BY State ORDER BY Year))
                 * 100 /
                NULLIF(LAG(SUM(Transaction_count)) OVER (PARTITION BY State ORDER BY Year),0),
            2) AS yoy
            FROM agg_trans
            GROUP BY State, Year
        )
        SELECT State, AVG(yoy) AS avg_growth
        FROM y
        WHERE Year >= 2019
        GROUP BY State
        ORDER BY avg_growth DESC
        LIMIT 5;
    """)
    df = pd.read_sql(q2, engine)
    figs["fig2"] = px.bar(df, x="State", y="avg_growth",
                          title="Top States by Average YoY Transaction Growth")

    # ======================================================
    # QUERY 3
    # ======================================================
    q3 = text("""
        WITH s AS (
            SELECT State, Year, SUM(Transaction_count) t
            FROM agg_trans
            WHERE Year BETWEEN 2020 AND 2024
            GROUP BY State, Year
        )
        SELECT State, Year,
        ROUND(
            (t - LAG(t) OVER (PARTITION BY State ORDER BY Year)) * 100 /
            LAG(t) OVER (PARTITION BY State ORDER BY Year),
        2) AS growth
        FROM s
        WHERE LAG(t) OVER (PARTITION BY State ORDER BY Year) IS NOT NULL;
    """)
    df = pd.read_sql(q3, engine)
    figs["fig3"] = px.line(df, x="Year", y="growth", color="State",
                           title="Transaction Growth Decline Trends")

    # ======================================================
    # QUERY 4
    # ======================================================
    q4 = text("""
        WITH q AS (
            SELECT Year, Quarter, SUM(Transaction_count) t
            FROM agg_trans
            GROUP BY Year, Quarter
        )
        SELECT Year, Quarter,
        ROUND((t - LAG(t) OVER (ORDER BY Year, Quarter)) * 100 /
              LAG(t) OVER (ORDER BY Year, Quarter),2) AS spike
        FROM q
        WHERE LAG(t) OVER (ORDER BY Year, Quarter) IS NOT NULL;
    """)
    df = pd.read_sql(q4, engine)
    figs["fig4"] = px.bar(df, x="Year", y="spike", color="Quarter",
                          title="Quarterly Transaction Spike")

    # ======================================================
    # QUERY 5
    # ======================================================
    q5 = text("""
        WITH y AS (
            SELECT Year, SUM(Transaction_amount) total
            FROM agg_trans
            GROUP BY Year
        )
        SELECT a.Transaction_type, ROUND(AVG(a.Transaction_amount * 100 / y.total),2) share
        FROM agg_trans a
        JOIN y ON a.Year = y.Year
        GROUP BY a.Transaction_type;
    """)
    df = pd.read_sql(q5, engine)
    figs["fig5"] = px.pie(df, names="Transaction_type", values="share",
                          title="Transaction Type Share")

    # ======================================================
    # QUERY 6
    # ======================================================
    q6 = text("""
        SELECT Brand_name, SUM(User_count) users
        FROM agg_user
        GROUP BY Brand_name
        ORDER BY users DESC
        LIMIT 6;
    """)
    df = pd.read_sql(q6, engine)
    fig6 = make_subplots(rows=1, cols=2, subplot_titles=("Top 3 Brands", "Bottom 3 Brands"))
    fig6.add_bar(x=df.head(3)["Brand_name"], y=df.head(3)["users"], row=1, col=1)
    fig6.add_bar(x=df.tail(3)["Brand_name"], y=df.tail(3)["users"], row=1, col=2)
    figs["fig6"] = fig6

    # ======================================================
    # QUERY 7
    # ======================================================
    q7 = text("""
        SELECT State,
        ROUND(AVG(Number_of_app_opens) / AVG(Registered_users),2) engagement
        FROM map_user
        GROUP BY State
        ORDER BY engagement DESC
        LIMIT 6;
    """)
    df = pd.read_sql(q7, engine)
    fig7 = make_subplots(rows=1, cols=2, subplot_titles=("Top 3", "Bottom 3"))
    fig7.add_bar(x=df.head(3)["State"], y=df.head(3)["engagement"], row=1, col=1)
    fig7.add_bar(x=df.tail(3)["State"], y=df.tail(3)["engagement"], row=1, col=2)
    figs["fig7"] = fig7

    # ======================================================
    # QUERY 8
    # ======================================================
    q8 = text("""
        SELECT Year, Quarter,
        ROUND(SUM(Number_of_app_opens)/SUM(Registered_users),4) engagement
        FROM map_user
        GROUP BY Year, Quarter;
    """)
    df = pd.read_sql(q8, engine)
    figs["fig8"] = px.bar(df, x="Year", y="engagement", color="Quarter",
                          title="Quarterly User Engagement")

    # ======================================================
    # QUERY 9
    # ======================================================
    q9 = text("""
        SELECT Year,
        SUM(Insurance_count) ins_txn,
        SUM(Insurance_amount) ins_amt
        FROM agg_ins
        GROUP BY Year;
    """)
    df = pd.read_sql(q9, engine)
    figs["fig9"] = px.bar(df, x="Year",
                          y=["ins_txn", "ins_amt"],
                          barmode="group",
                          title="Insurance Growth Over Years")

    # ======================================================
    # QUERY 10
    # ======================================================
    q10 = text("""
        SELECT State,
        MAX(Insurance_amount) - MIN(Insurance_amount) value
        FROM agg_ins
        GROUP BY State
        ORDER BY value DESC
        LIMIT 5;
    """)
    df = pd.read_sql(q10, engine)
    figs["fig10"] = px.bar(df, x="State", y="value",
                           title="Top Insurance Value States")

    # ======================================================
    # QUERY 11
    # ======================================================
    q11 = text("""
        SELECT t.State,
        ROUND(SUM(i.Insurance_count) * 100 / SUM(t.Transaction_count),4) penetration
        FROM agg_trans t
        JOIN agg_ins i ON t.State = i.State
        GROUP BY t.State
        ORDER BY penetration ASC
        LIMIT 5;
    """)
    df = pd.read_sql(q11, engine)
    figs["fig11"] = px.bar(df, x="State", y="penetration",
                           title="Untapped Insurance States")

    # ======================================================
    # QUERY 12
    # ======================================================
    q12 = text("""
        SELECT State,
        ROUND(AVG(Transaction_count),2) avg_txn
        FROM agg_trans
        GROUP BY State
        ORDER BY avg_txn DESC
        LIMIT 10;
    """)
    df = pd.read_sql(q12, engine)
    figs["fig12"] = px.bar(df, x="State", y="avg_txn",
                           title="Consistent Transaction Growth States")

    # ======================================================
    # QUERY 13 (STATE PIE CHARTS)
    # ======================================================
    q13 = text("""
        SELECT State, District_name, SUM(Number_of_app_opens) opens
        FROM map_user
        GROUP BY State, District_name;
    """)
    df = pd.read_sql(q13, engine)
    state_pies = {}
    for state in df["State"].unique():
        sdf = df[df["State"] == state]
        state_pies[state] = px.pie(
            sdf, names="District_name", values="opens",
            title=f"{state} App Open Share"
        )
    figs["state_pie_charts"] = state_pies

    # ======================================================
    # QUERY 14
    # ======================================================
    q14 = text("""
        SELECT State, SUM(insurance_amount) total
        FROM top_ins
        WHERE Year = 2024
        GROUP BY State
        ORDER BY total DESC
        LIMIT 3;
    """)
    df = pd.read_sql(q14, engine)
    figs["fig14"] = px.bar(df, x="State", y="total",
                           title="Top Insurance States 2024")

    # ======================================================
    # QUERY 15
    # ======================================================
    q15 = text("""
        SELECT Year, Quarter, SUM(insurance_amount) total
        FROM top_ins
        GROUP BY Year, Quarter;
    """)
    df = pd.read_sql(q15, engine)
    figs["fig15"] = px.bar(df, x="Year", y="total", color="Quarter",
                           title="Highest Insurance Volume Quarters")

    # ======================================================
    # QUERY 16
    # ======================================================
    q16 = text("""
        SELECT District_name, SUM(insurance_amount) total
        FROM map_ins
        WHERE Year = 2024
        GROUP BY District_name
        ORDER BY total DESC
        LIMIT 5;
    """)
    df = pd.read_sql(q16, engine)
    figs["fig16"] = px.bar(df, x="District_name", y="total",
                           title="Top Districts by Insurance Volume")

    # ======================================================
    # QUERY 17
    # ======================================================
    q17 = text("""
        SELECT Pincode, SUM(insurance_count) growth
        FROM top_ins
        WHERE Year = 2024
        GROUP BY Pincode
        ORDER BY growth DESC
        LIMIT 5;
    """)
    df = pd.read_sql(q17, engine)
    figs["fig17"] = px.bar(df, x="Pincode", y="growth",
                           title="Top Pincodes Insurance Growth")

    return figs
