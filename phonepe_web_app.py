import streamlit as st

import pandas as pd
import numpy as np

import mysql.connector as msql
from mysql.connector import Error
from sqlalchemy import create_engine, text, inspect

import geopandas as gpd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns

import json
import requests

import warnings

# Database Connection Setup

DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'project_phonepe_pulse'

# MYSQL Connection

engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def state_list():
    query = "SELECT state FROM aggregated_transaction;"
    df = pd.read_sql(query, engine)
    state_list = df['state'].drop_duplicates().to_list()
    return sorted(state_list)

def district_list():
    query = "SELECT state, district FROM top_transaction_districtwise;"
    df = pd.read_sql(query, engine)
    india_dict = df.groupby('state')['district'].apply(lambda x: sorted(set(x))).to_dict()
    return india_dict

def year_list():
    query = "SELECT year FROM aggregated_transaction;"
    df = pd.read_sql(query, engine)
    year_list = df['year'].drop_duplicates().to_list()
    return year_list

def quarter_list():
    query = "SELECT quarter FROM aggregated_transaction;"
    df = pd.read_sql(query, engine)
    quarter_list = df['quarter'].drop_duplicates().to_list()
    return sorted(quarter_list)

def get_iqr_bounds(series):
    s = series.sort_values()
    Q1 = s.quantile(0.25)
    Q2 = s.quantile(0.50)
    Q3 = s.quantile(0.75)
    IQR = Q3-Q1
    lower = Q1-1.5*IQR
    upper = Q3+1.5*IQR
    print(lower, upper)
    print(Q1, Q2, Q3)
    return int(Q2), int(upper)

def value_formats(n):
    if n > 1e12:
        return f"{n/1e12:.2f} T"
    elif n > 1e9:
        return f"{n/1e9:.2f} B"
    elif n > 1e7:
        return f"{n/1e7:.2f} Cr"
    elif n > 1e6:
        return f'{n/1e6:.2f} M'
    elif n > 1e3:
        return f"{n/1e3:.2f} k"
    else:
        return str(n)
    
def geo_choropleth_plot(final_df, location_column, color_column, title, animation_column, mini, maxi, title_x=0.1):
    final_df[color_column + '_log'] = np.log1p(final_df[color_column])
    color_column = color_column + '_log'

    if mini is None:
        mini = final_df[color_column].min()
    if maxi is None:
        maxi = final_df[color_column].max()

    fig = px.choropleth(final_df,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations=location_column,
                            color=color_column,
                            color_continuous_scale="RdBu",
                            range_color=(mini,maxi),
                            animation_frame=animation_column,
                            title=title)

    fig.update_geos(fitbounds='locations', visible=False, projection_type='natural earth', bgcolor='rgba(0,0,0,0)')
    fig.update_layout(title=title,
                          title_x=title_x,
                          title_y=0.93,
                          title_font=dict(size=15), 
                          margin=dict(l=10, r=10, t=40, b=10),
                          paper_bgcolor="rgba(0,0,0,0)",
                          plot_bgcolor="rgba(0,0,0,0)")
    fig.update_coloraxes(colorbar_title=None)
    return fig

def geo_choropleth_plot_statewise(final_df, location_column, color_column, title, selected_state, animation_column, title_x=0.15):
    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    geojson_data = requests.get(geojson_url).json()

    # filtering geojson state file
    filtered_features = [feature for feature in geojson_data['features']
                                if feature['properties']['ST_NM'] == selected_state]
    filtered_geojson = {'type': 'FeatureCollection', 'features': filtered_features}

    fig = px.choropleth(final_df,
                            geojson=filtered_geojson,
                            featureidkey='properties.ST_NM',
                            locations=location_column,
                            color=color_column,
                            color_continuous_scale="rainbow",
                            animation_frame=animation_column,
                            title=title,
                            height=400)

    fig.update_geos(fitbounds='locations', visible=False, projection_type='natural earth', bgcolor='rgba(0,0,0,0)')
    fig.update_layout(title=title,
                          title_x=title_x,
                          title_y=0.93,
                          title_font=dict(size=25), 
                          margin=dict(l=10, r=10, t=40, b=10),
                          coloraxis_showscale=False)
    fig.update_traces(showscale=False)
    fig.update_coloraxes(colorbar_title=None)
    return fig

# ----------------------------------------------- HOME PAGE -------------------------------------------------- #

def main_page():
    st.markdown("<h1 style='color: violet;'>PHONEPE PULSE DATA INSIGHTS</h1>", unsafe_allow_html=True)
    st.markdown("PhonePe Pulse is an open data platform launched by PhonePe that provides insights into digital payment trends across India. It includes transaction statistics categorized by geography (state, district, pincode), time (year, quarter), and type (peer-to-peer, merchant payments, recharges, etc.). The data is made publicly accessible to promote research and innovation in the fintech space.")
    st.markdown("[Visit Phonepe Pulse Website](https://www.phonepe.com/pulse/)\n")

    col1, col2, col3 = st.columns(3)

    with col1:
        query = "SELECT SUM(registered_users) as total_users FROM map_user;"
        df = pd.read_sql(query, engine)

        st.markdown("### Registered Users")
        st.markdown(f"<h2 style='color: green;'> {value_formats(df.iloc[0,0])}+ 📈</h2>", unsafe_allow_html=True)

    with col2:
        query = "SELECT SUM(transaction_count) AS total_trans FROM aggregated_transaction;"
        df = pd.read_sql(query, engine)

        st.markdown("### Transactions")
        st.markdown(f"<h2 style='color: green;'> {value_formats(df.iloc[0,0])}+ 📈</h2>", unsafe_allow_html=True)

    with col3:
        query = "SELECT SUM(insurance_count) AS total FROM map_insurance;"
        df = pd.read_sql(query, engine)

        st.markdown("### Insurance Transactions")
        st.markdown(f"<h2 style='color: green;'> {value_formats(df.iloc[0,0])}+ 📈</h2>", unsafe_allow_html=True)
    st.markdown("\n")
    st.markdown("<h4 style='color: blue;'> Phonepe User Registeration Trends </h4>", unsafe_allow_html=True)
    with st.container(height=500):
        query = """ 
                SELECT
                    year,
                    quarter,
                    SUM(registered_users) as user_count,
                    SUM(appopen_count) as open_count
                FROM
                    map_user
                GROUP BY 
                    year, quarter; 
                    """
        df = pd.read_sql(text(query), engine)
        df['user_counts_f'] = df['user_count'].apply(value_formats)
        df['open_counts_f'] = df['open_count'].apply(value_formats)

        fig = px.bar(df, x='year', y='user_count', color='quarter', barmode='group', color_discrete_sequence=px.colors.qualitative.T10)
        for trace in fig.data:
            quarter = trace.name
            quarter_df = df[df['quarter'] == quarter]
            trace.customdata = quarter_df[['quarter','user_counts_f', 'open_counts_f']].values
            trace.hovertemplate=("Year = %{x}<br>Quarter = %{customdata[0]}<br>Registered Users = %{customdata[1]}<br>Appopen Count = %{customdata[2]}<extra></extra>")
        fig.update_layout(xaxis_title="Year", yaxis_title="Number_of_Users", legend_title="Quarter", bargap=0.2, margin=dict(l=40, r=40, t=40, b=20))            
        st.plotly_chart(fig)
    with st.expander("Detailed Info on User Registeration Trends"):
        st.dataframe(df)

    st.markdown("\n")
    st.markdown("<h4 style='color: blue;'> Phonepe Transaction Trends </h4>", unsafe_allow_html=True)
    with st.container(height=500):
        query = """ 
                SELECT
                    year,
                    quarter,
                    SUM(transaction_count) as number_of_transactions,
                    SUM(transaction_amount) as total_transaction_amount
                FROM
                    aggregated_transaction
                GROUP BY 
                    year, quarter; 
                    """
        df = pd.read_sql(text(query), engine)
        df['number_of_transactions_f'] = df['number_of_transactions'].apply(value_formats)
        df['total_transaction_amount_f'] = df['total_transaction_amount'].apply(value_formats)


        fig = px.bar(df, x='year', y='number_of_transactions', color='quarter', barmode='group', color_discrete_sequence=px.colors.qualitative.Set2)
        for trace in fig.data:
            quarter = trace.name
            quarter_df = df[df['quarter'] == quarter]
            trace.customdata = quarter_df[['quarter','number_of_transactions_f', 'total_transaction_amount_f']].values
            trace.hovertemplate=("Year = %{x}<br>Quarter = %{customdata[0]}<br><br>Transaction Count = %{customdata[1]}<br>Transaction Amount = ₹ %{customdata[2]}<extra></extra>")
        fig.update_layout(xaxis_title="Year", yaxis_title="Number_of_transactions", legend_title="Quarter", bargap=0.2, margin=dict(l=40, r=40, t=40, b=20))            
        st.plotly_chart(fig)
    with st.expander("Detailed Info on Transaction Trends"):
        st.dataframe(df)

    st.markdown("\n")
    st.markdown("<h4 style='color: blue;'> Phonepe Insurance Trends </h4>", unsafe_allow_html=True)
    with st.container(height=500):
        query = """SELECT year, quarter, SUM(insurance_count) AS count, SUM(insurance_amount) AS amount
                    FROM aggregated_insurance GROUP BY year, quarter;"""
        df = pd.read_sql(query, engine)

        new_df = pd.DataFrame([{'year' : 2020, 'quarter' : 'Q1', 'count' : 0, 'amount' : 0}])
        df1 = pd.concat([new_df, df], ignore_index=True)
        df1['count_f'] = df1['count'].apply(value_formats)
        df1['amount_f'] = df1['amount'].apply(value_formats)

        fig = px.bar(df1, x='year', y='count', color='quarter', barmode='group', color_discrete_sequence=['#66C2A5', '#377EB8', '#984EA3','#E78AC3'])
        for trace in fig.data:
            quarter = trace.name
            quarter_df = df1[df1['quarter'] == quarter]
            trace.customdata = quarter_df[['quarter','count_f', 'amount_f']].values
            trace.hovertemplate=("Year = %{x}<br>Quarter = %{customdata[0]}<br><br>Transaction Count = %{customdata[1]}<br>Transaction Amount = ₹ %{customdata[2]}<extra></extra>")
        fig.update_layout(xaxis_title="Year", yaxis_title="Number_of_insurance_transactions", legend_title="Quarter", bargap=0.2, margin=dict(l=40, r=40, t=40, b=20))            
        st.plotly_chart(fig)

    with st.expander("Detailed Info on Insurance Trends"):
        st.dataframe(df)

# -------------------------------------------------- USER PAGE ------------------------------------------------ #
    

def user_engage_analysis():
    st.markdown("<h3 style='color: blue;'>User Engagement Analysis</h3>", unsafe_allow_html=True)
    st.markdown("<h4 style ='color: Skyblue;'>Registered Users Trend Across States Over Years</h4>", unsafe_allow_html=True)

    query = """SELECT state, year, SUM(registered_users) as user_count, SUM(appopen_count) as open_count 
                FROM map_user GROUP BY state, year ORDER BY user_count;"""
    df = pd.read_sql(query, engine)
    df['user_counts_f'] = df['user_count'].apply(value_formats)
    df['open_counts_f'] = df['open_count'].apply(value_formats)

    year_dict = {}
    for year in year_list():
        df_y = df[df['year'] == year]
        year_dict[year] = []
        year_dict[year].append(value_formats(df_y['user_count'].sum()))
        year_dict[year].append(value_formats(df_y['open_count'].sum()))

    years = year_list()+['All']
    cols = st.columns(len(years))
    for j, year in enumerate(years):
        with cols[j]:
            if year != "All":
                with st.popover(f"{year}"):
                    st.markdown(f"<h5 style ='color: Green;'>Registered User Count: {year_dict[year][0]}</h5>", unsafe_allow_html=True)
                    st.markdown(f"<h5 style ='color: Green;'>App Open Count : {year_dict[year][1]}</h5>", unsafe_allow_html=True)
            else:
                with st.popover("Gross"):
                    st.markdown(f"<h5 style ='color: Green;'>Registered User Count: {value_formats(df['user_count'].sum())}</h5>", unsafe_allow_html=True)
                    st.markdown(f"<h5 style ='color: Green;'>App Open Count : {value_formats(df['open_count'].sum())}</h5>", unsafe_allow_html=True)

    with st.container(border=True):
        fig = geo_choropleth_plot(df, 'state', 'user_count', "", 'year', None, None)

        initial_year = df[df['year'] == df['year'].max()]
        fig.update_traces(customdata = initial_year[['state', 'year', 'user_counts_f', 'open_counts_f']].values,
                        hovertemplate="Year : %{customdata[1]}<br>State : %{customdata[0]}<br>Registered Users : %{customdata[2]}<br>Appopen Count : %{customdata[3]}<extra></extra>")
        
        for frame in fig.frames:
            frame_df = df[df['year'] == int(frame.name)]
            frame.data[0].customdata = frame_df[['state', 'year', 'user_counts_f', 'open_counts_f']].values
            frame.data[0].hovertemplate = ("Year : %{customdata[1]}<br>State : %{customdata[0]}<br>Registered Users : %{customdata[2]}<br>Appopen Count :  %{customdata[3]}<extra></extra>")
                
        st.plotly_chart(fig, use_container_width=True)

        df1 = df.groupby(['state', 'year'])[['user_count', 'open_count']].sum().reset_index()
        df1['user_counts_f'] = df1['user_count'].apply(value_formats)
        df1['open_counts_f'] = df1['open_count'].apply(value_formats)

        p_df1 = df1.pivot_table(index='year', columns='state', values='user_count')
        c = df1.pivot_table(index='year', columns='state', values='user_counts_f', aggfunc='first')
        a = df1.pivot_table(index='year', columns='state', values='open_counts_f', aggfunc='first')
        zmin = p_df1.values.min()
        zmax = p_df1.values.max()
        custom_data = np.dstack((c.values, a.values))
        fig = go.Figure(data=go.Heatmap(x=p_df1.columns,
                                        y=p_df1.index,
                                        z=p_df1.values,
                                        colorscale='Blues',
                                        customdata=custom_data,
                                        hovertemplate="State : %{x}"+
                                                "<br>Year : %{y}"+
                                                "<br>Registered User Count: %{customdata[0]}"+
                                                "<br>App Open Count: %{customdata[1]}<extra></extra>",
                                        zmin=zmin, zmax=zmax))
        fig.update_layout(height=400,
                            width=400,
                            xaxis_tickangle=-45,
                            margin=dict(t=0,b=0))
        st.plotly_chart(fig, use_container_width=True)
        #with st.expander("Detailed Info of Users and App Open Volume"):
            #st.dataframe(df1)

    with st.expander("Detailed Info of Registered Users Data"):
            st.dataframe(df)

    st.markdown("<h3 style='color: blue;'>Device Dominance Distribution</h3>", unsafe_allow_html=True)

    col1, col2 = st.columns([0.3, 0.7])
    query = """SELECT brand, SUM(user_count) as user_count FROM aggregated_user GROUP BY brand ORDER BY user_count ASC;"""
    df = pd.read_sql(query, engine)
    with col1.container(border=True):
        st.markdown("<h4 style ='color: Skyblue;'> Brands</h4>", unsafe_allow_html=True)
        fig = px.bar(df, x='user_count', y='brand')
        fig.update_layout(margin=dict(t=0,b=0), xaxis_title=None, yaxis_title=None)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander(f"Detailed info on brand usage"):
            st.dataframe(df)

    query = """SELECT state, brand, year, quarter, user_count FROM aggregated_user WHERE year!= 2022 GROUP BY state, brand, year, quarter;"""
    with engine.connect() as conn:
        conn.execute(text("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))"))
        df = pd.read_sql(query, conn)
    df['count'] = df['user_count'].apply(value_formats)
    df1 = df.groupby(['state', 'year', 'brand'])[['user_count']].sum().reset_index()
    df1['count_f'] = df1['user_count'].apply(value_formats)
    brands = df['brand'].unique()
    selected_brand = st.sidebar.selectbox("Choose Brand:",brands)

    df2 = df1[df1["brand"] == selected_brand]
    with col2.container(border=True):
        st.markdown(f"<h4 style ='color: Skyblue;'>Yearly and State-wise Trends for {selected_brand} Brand</h4>", unsafe_allow_html=True)
        pivot_df = df2.pivot_table(index='year', columns='state', values='user_count')
        c = df2.pivot_table(index='year', columns='state', values='count_f', aggfunc='first')
        b = df2.pivot_table(index='year', columns='state', values='brand', aggfunc='first')
        zmin = pivot_df.values.min()
        zmax = pivot_df.values.max()
        custom_data = np.dstack((c.values, b.values))
        fig = go.Figure(data=go.Heatmap(x=pivot_df.columns,
                                        y=pivot_df.index,
                                        z=pivot_df.values,
                                        colorscale='Blues',
                                        customdata=custom_data,
                                        hovertemplate="State : %{x}"+
                                                "<br>Year : %{y}"+
                                                "<br>Brand : %{customdata[1]}"+
                                                "<br>User Count: %{customdata[0]}<extra></extra>",
                                                zmin=zmin, zmax=zmax))
        fig.update_layout(width=400,
                        xaxis_tickangle=-45,
                        margin=dict(t=0,b=0,l=0,r=0))
        st.plotly_chart(fig, use_container_width=True)
        with st.expander(f"Detailed info on {selected_brand}"):
            st.dataframe(df2)

    st.markdown(f"<h4 style ='color: Skyblue;'>App Open Rate Trend by {selected_brand} Brand</h4>", unsafe_allow_html=True)
    query = """WITH brand_usage AS (
                SELECT state, year, brand, SUM(user_count) AS brand_users
                    FROM aggregated_user
                    GROUP BY state, year, brand
                ), app_usage AS (
                    SELECT state, year, SUM(registered_users) as users, SUM(appopen_count) as counts
                    FROM map_user
                    GROUP BY state, year
                ) SELECT
                    agg.state, agg.year, agg.brand, agg.brand_users, 
                    map.users, map.counts, map.counts/NULLIF(agg.brand_users, 0) AS app_open_rate
                FROM brand_usage agg
                JOIN app_usage map
                ON agg.state = map.state AND agg.year = map.year;"""
    df = pd.read_sql(query, engine)
    df1 = df[df['brand'] == f"{selected_brand}"]
    with st.container(border=True):
        fig = px.bar(df1, x='state', y='app_open_rate',color='year', barmode='group')
        fig.update_layout(height=500,
                                width=400,
                                xaxis_tickangle=-45,
                                margin=dict(t=0,b=0), xaxis_title=None)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander(f"Detailed Info on {selected_brand} App Open Rate"):
            st.dataframe(df1)

    st.markdown("<h4 style ='color: Skyblue;'>Underutilized Brands and App Open Rates</h4>", unsafe_allow_html=True)

    high_user_threshold = 5000
    low_engagement_threshold = 100

    df['underutilized'] = (df['brand_users'] > high_user_threshold) & (df['app_open_rate'] < low_engagement_threshold)
    with st.container(border=True):
        under_df = df[df['underutilized']]

        fig = px.bar(under_df.sort_values("brand_users", ascending=False).head(20),
            x="brand",
            y="app_open_rate",
            color="state",
            hover_data=["brand_users","year"])
        st.plotly_chart(fig, use_container_width=True)

        with st.expander(f"Detailed Info on Underutilized data"):
            st.dataframe(under_df)
def user_reg_analysis():
    st.markdown("<h3 style='color: blue;'>User Registration Analysis</h3>", unsafe_allow_html=True)
    # -------------------- GEO BUBBLE MAP -------------------- # 
    query1 = """CREATE TEMPORARY TABLE agg_users_info AS 
                SELECT state, district, SUM(registered_users) AS users
                FROM top_user_districtwise GROUP BY state, district;"""
    query2 = """SELECT u.state, u.district, u.users, l.longitude, l.latitude
                FROM agg_users_info AS u INNER JOIN state_level_location_metrics AS l
                ON u.state = l.state AND u.district = l.district GROUP BY u.state, u.district;"""
    
    with engine.connect() as conn:
        conn.execute(text(query1))
        df = pd.read_sql(query2, conn)    

    df['users_f'] = df['users'].apply(value_formats)
    with st.container(border=True):
        # Setting India States as background map
        with open("india_states.geojson") as f:
            india_geojson = json.load(f)
        state_names = [feature["properties"]["ST_NM"] for feature in india_geojson["features"]]
        fig = go.Figure()
        fig.add_trace(go.Choroplethmapbox(
                    geojson=india_geojson,
                    locations=state_names,
                    z=[0]*len(state_names),
                    featureidkey="properties.ST_NM",
                    colorscale=[[0, 'rgba(0,0,0,0)'], [1, 'rgba(0,0,0,0)']],
                    showscale=False,
                    marker_line_color='white',
                    marker_line_width=0.5))
        
        # Plotting Bubbles on top
        min_size = 5
        max_size = 50
        print(df.head)
        # Normalize users to marker sizes between 5 and 30
        size_scaled = np.interp(df['users'], (df['users'].min(), df['users'].max()), (min_size, max_size))
        fig.add_trace(go.Scattermapbox(lon=df['longitude'],
            lat=df['latitude'],
            mode="markers",
            marker=dict(size=size_scaled,
                color="#8D14FF",
                opacity=0.6),
            customdata=df[['state', 'district', 'users_f', 'latitude', 'longitude']],
            hovertemplate="<b>State: %{customdata[0]}</b><br>District: %{customdata[1]}<br>Users: %{customdata[2]}<br>Latitude: %{customdata[3]}<br>Longitude: %{customdata[4]}<extra></extra>"))

        fig.update_layout(mapbox=dict(style="carto-darkmatter",center={"lat": 22.9734, "lon": 78.6569},zoom=4,),
                        margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig, use_container_width=True)
        
        with st.expander(f"Detailed Info"):
            st.dataframe(df)

    # -------------------------------------------- TOP 15 USERS STATEWISE -------------------------------------- #

    selected_year = st.sidebar.selectbox("Choose Year: ", year_list()+["All"])
    selected_quarter = st.sidebar.selectbox("Choose Quarter: ", quarter_list()+["All"])
    st.markdown(f"<h4 style ='color: Skyblue;'>Statewise - Top Registered Users [Year-({selected_year}) & Quarter-({selected_quarter})]</h4>", unsafe_allow_html=True)

    if selected_year == "All" and selected_quarter != "All":
        query = f""" SELECT state, year, quarter, SUM(registered_users) as users
                FROM map_user WHERE quarter='{selected_quarter}'
                GROUP BY state, year, quarter ORDER BY users DESC LIMIT 25;"""
    elif selected_year != "All" and selected_quarter == "All":
        query = f""" SELECT state, year, quarter, SUM(registered_users) as users
                FROM map_user WHERE year={selected_year}
                GROUP BY state, year, quarter  ORDER BY users DESC LIMIT 25;"""
    elif selected_year == "All" and selected_quarter == "All":
        query = f""" SELECT state, SUM(registered_users) as users
                FROM map_user
                GROUP BY state ORDER BY users DESC LIMIT 15;"""
    else:
        query = f""" SELECT state, year, quarter, SUM(registered_users) as users
                FROM map_user WHERE year={selected_year} AND quarter='{selected_quarter}'
                GROUP BY state, year, quarter ORDER BY users DESC LIMIT 15;"""
    with engine.connect() as conn:
        conn.execute(text("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))"))
        df = pd.read_sql(query, conn)
    
    df['users_f'] = df['users'].apply(value_formats)
    with st.container(border=True):
        df_top_states = df.sort_values(by='users', ascending=True)
        fig = px.bar(df_top_states, x='users', y='state', orientation='h', color='users', color_continuous_scale='MAGMA', text_auto=True)
        fig.update_layout(xaxis_title='Total Users', yaxis_title=None, margin=dict(l=0, r=0, t=0, b=0))
        if selected_year == "All" and selected_quarter == "All":
            fig.update_traces(customdata=df_top_states[['users_f', 'state']],
                            hovertemplate='State : %{y}<br>' +
                                            'Users: %{customdata[0]}<extra></extra>')
        else:
            fig.update_traces(customdata=df_top_states[['users_f', 'year', 'quarter']],
                            hovertemplate='State : %{y}<br>' +
                                            'Users: %{customdata[0]}<br>'+
                                            'Year-Quarter : %{customdata[1]}-%{customdata[2]}<extra></extra>')

        st.plotly_chart(fig, use_container_width=True)
        with st.expander(f"Detailed Info on Top 15 States for Year({selected_year}) - Quarter({selected_quarter})"):
            st.dataframe(df)
    
    # -------------------------------------------- TOP 15 USERS DISTRICTWISE -------------------------------------- #
    
    st.markdown(f"<h4 style ='color: Skyblue;'>Districtwise - Top Registered Users [Year-({selected_year}) & Quarter-({selected_quarter})]</h4>", unsafe_allow_html=True)

    if selected_year == "All" and selected_quarter != "All":
        query = f"""SELECT state, district, year, quarter, SUM(registered_users) as users
                    FROM top_user_districtwise WHERE quarter='{selected_quarter}'
                    GROUP BY district, year, quarter ORDER BY users DESC LIMIT 40;"""
    elif selected_year != "All" and selected_quarter == "All":
        query = f"""SELECT state, district, year, quarter, SUM(registered_users) as users
                    FROM top_user_districtwise WHERE year='{selected_year}'
                    GROUP BY district, year, quarter ORDER BY users DESC LIMIT 40;"""
    elif selected_year == "All" and selected_quarter == "All":
        query = f"""SELECT state, district, SUM(registered_users) as users
                    FROM top_user_districtwise
                    GROUP BY district ORDER BY users DESC LIMIT 15;"""
    else:
        query = f"""SELECT state, district, year, quarter, SUM(registered_users) as users
                    FROM top_user_districtwise WHERE year='{selected_year}' AND quarter='{selected_quarter}'
                    GROUP BY district, year, quarter ORDER BY users DESC LIMIT 15;"""
        
    with engine.connect() as conn:
        conn.execute(text("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))"))
        df = pd.read_sql(query, conn)
    
    df['users_f'] = df['users'].apply(value_formats)
    with st.container(border=True):
        df_top_districts = df.sort_values(by='users', ascending=True)
        fig = px.bar(df_top_districts, x='users', y='district', orientation='h', color='users', color_continuous_scale='sunsetdark', text_auto=True)
        fig.update_layout(yaxis_title=None, xaxis_title='Total Users', margin=dict(l=0, r=0, t=0, b=0))
        if selected_year == "All" and selected_quarter == "All":
            fig.update_traces(customdata=df_top_districts[['users_f', 'state']],
                            hovertemplate='State : %{customdata[1]}<br>' +
                                            'District : %{y}<br>'+
                                            'Users : %{customdata[0]}<extra></extra>')
        else:
            fig.update_traces(customdata=df_top_districts[['users_f', 'state', 'year', 'quarter']],
                            hovertemplate='State : %{customdata[1]}<br>' +
                                            'District : %{y}<br>'+
                                            'Users : %{customdata[0]}<br>'+
                                            'Year-Quarter : %{customdata[2]}-%{customdata[3]}<extra></extra>')

        st.plotly_chart(fig, use_container_width=True)
        with st.expander(f"Detailed Info on Top 15 States for Year({selected_year}) - Quarter({selected_quarter})"):
            st.dataframe(df)

    # -------------------------------------------- TOP 15 USERS PINCODEWISE -------------------------------------- #
    
    st.markdown(f"<h4 style ='color: Skyblue;'>Pincodewise - Top Registered Users [Year-({selected_year}) & Quarter-({selected_quarter})]</h4>", unsafe_allow_html=True)

    if selected_year == "All" and selected_quarter != "All":
        query = f"""SELECT state, pincode, year, quarter, SUM(registered_users) as users
                    FROM top_user_pincodewise WHERE quarter='{selected_quarter}'
                    GROUP BY pincode, year, quarter ORDER BY users DESC LIMIT 40;"""
    elif selected_year != "All" and selected_quarter == "All":
        query = f"""SELECT state, pincode, year, quarter, SUM(registered_users) as users
                    FROM top_user_pincodewise WHERE year='{selected_year}'
                    GROUP BY pincode, year, quarter ORDER BY users DESC LIMIT 40;"""
    elif selected_year == "All" and selected_quarter == "All":
        query = f"""SELECT state, pincode, SUM(registered_users) as users
                    FROM top_user_pincodewise
                    GROUP BY pincode ORDER BY users DESC LIMIT 15;"""
    else:
        query = f"""SELECT state, pincode, year, quarter, SUM(registered_users) as users
                    FROM top_user_pincodewise WHERE year='{selected_year}' AND quarter='{selected_quarter}'
                    GROUP BY pincode, year, quarter ORDER BY users DESC LIMIT 15;"""
        
    with engine.connect() as conn:
        conn.execute(text("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))"))
        df = pd.read_sql(query, conn)

    df['pincode'] = df['pincode'].astype(str)
    df['users_f'] = df['users'].apply(value_formats)
    with st.container(border=True):
        df_top_pincode = df.sort_values(by='users', ascending=True)
        fig = px.bar(df_top_pincode, x='users', y='pincode', orientation='h', color='users', color_continuous_scale='sunset', text_auto=True, text='users_f')
        fig.update_layout(yaxis_title=None, xaxis_title='Total Users', margin=dict(l=0, r=0, t=0, b=0), yaxis=dict(type='category') )
        if selected_year == "All" and selected_quarter == "All":
            fig.update_traces(customdata=df_top_pincode[['users_f', 'state']],
                            hovertemplate='State : %{customdata[1]}<br>' +
                                            'Pincode : %{y}<br>'+
                                            'Users : %{customdata[0]}<extra></extra>')
        else:
            fig.update_traces(customdata=df_top_pincode[['users_f', 'state', 'year', 'quarter']],
                            hovertemplate='State : %{customdata[1]}<br>' +
                                            'Pincode : %{y}<br>'+
                                            'Users : %{customdata[0]}<br>'+
                                            'Year-Quarter : %{customdata[2]}-%{customdata[3]}<extra></extra>')

        st.plotly_chart(fig, use_container_width=True)
        with st.expander(f"Detailed Info on Top 15 Pincodes for Year({selected_year}) - Quarter({selected_quarter})"):
            st.dataframe(df)


def second_page():
    st.markdown("<h2 style='color: violet;'>PHONEPE USER DATA INSIGHTS</h1>", unsafe_allow_html=True)
    sub_page = st.sidebar.radio("Choose Analysis Variants:", ["User & Device Engagement Analysis", "User Registration Analysis"])
    if sub_page =="User & Device Engagement Analysis":
        user_engage_analysis()
    elif sub_page == "User Registration Analysis":
        user_reg_analysis()

# ----------------------------------------------  TRANSACTION PAGE -------------------------------------------- #

def payment_mode_analysis():
    st.markdown("<h3 style ='color: blue;'>Transaction Dynamics based on State, Payment Types and Quarters over Years</h3>", unsafe_allow_html=True)

    selected_state = st.sidebar.selectbox("Choose State: ", ['All'] + state_list())
    selected_year = st.sidebar.selectbox("Choose Year: ", ['All'] + year_list())
    selected_quarter = st.sidebar.selectbox("Choose Quarter:", ['All'] + quarter_list())

    if selected_quarter == "All" and selected_year == "All" and selected_state == "All":
        query = """SELECT * FROM aggregated_transaction;"""
        df = pd.read_sql(query, engine)
        df['count'] = df['transaction_count'].apply(value_formats)
        df['amount'] = df['transaction_amount'].apply(value_formats)

        count_sum = value_formats(df['transaction_count'].sum())
        amount_sum = value_formats(df['transaction_amount'].sum())
        st.markdown("<h4 style ='color: Skyblue;'> India - Overall Transaction Behaviour</h4>", unsafe_allow_html=True)

        year_dict = {}
        for year in year_list():
            df_y = df[df['year'] == year]
            year_dict[year] = []
            year_dict[year].append(value_formats(df_y['transaction_count'].sum()))
            year_dict[year].append(value_formats(df_y['transaction_amount'].sum()))

        years = year_list()+['All']
        cols = st.columns(len(years))
        for j, year in enumerate(years):
            with cols[j]:
                if year != "All":
                    with st.popover(f"{year}"):
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Count: {year_dict[year][0]}</h5>", unsafe_allow_html=True)
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Amount : ₹ {year_dict[year][1]}</h5>", unsafe_allow_html=True)
                else:
                    with st.popover("Gross"):
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Count: {count_sum}</h5>", unsafe_allow_html=True)
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Amount : ₹ {amount_sum}</h5>", unsafe_allow_html=True)

        with st.container(border=True):         
            fig = geo_choropleth_plot(df, 'state', 'transaction_count', "", 'year', None, None)

            initial_year = df[df['year'] == df['year'].min()]
            fig.update_traces(customdata = initial_year[['state', 'year', 'count', 'amount']].values,
                        hovertemplate="Year : %{customdata[1]}<br>State : %{customdata[0]}<br>Transaction Count : %{customdata[2]}<br>Transaction Amount : ₹ %{customdata[3]}<extra></extra>")
        
            for frame in fig.frames:
                frame_df = df[df['year'] == int(frame.name)]
                frame.data[0].customdata = frame_df[['state', 'year', 'count', 'amount']].values
                frame.data[0].hovertemplate = ("Year : %{customdata[1]}<br>State : %{customdata[0]}<br>Transaction Count : %{customdata[2]}<br>Transaction Amount : ₹ %{customdata[3]}<extra></extra>")
                
            st.plotly_chart(fig, use_container_width=True)

            df1 = df.groupby(['state', 'year'])[['transaction_count', 'transaction_amount']].sum().reset_index()
            df1['count_s'] = df1['transaction_count'].apply(value_formats)
            df1['amount_s'] = df1['transaction_amount'].apply(value_formats)
            
            p_df1 = df1.pivot_table(index='year', columns='state', values='transaction_count')
            c = df1.pivot_table(index='year', columns='state', values='count_s', aggfunc='first')
            a = df1.pivot_table(index='year', columns='state', values='amount_s', aggfunc='first')
            zmin = p_df1.values.min()
            zmax = p_df1.values.max()
            custom_data = np.dstack((c.values, a.values))
            fig = go.Figure(data=go.Heatmap(x=p_df1.columns,
                                            y=p_df1.index,
                                            z=p_df1.values,
                                            colorscale='RdBu',
                                            customdata=custom_data,
                                            hovertemplate="State : %{x}"+
                                                "<br>Year : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>",
                                            zmin=zmin, zmax=zmax))
            fig.update_layout(height=500,
                              width=400,
                              xaxis_tickangle=-45,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Detailed Info of Yearly Transaction Count behaviour"):
                st.dataframe(df1)

        st.markdown("<h4 style ='color: skyblue;'> India - Overall Transaction Payment Type Distribution</h4>", unsafe_allow_html=True)

        with st.container(border=True):
            pivot_df = df.pivot_table(index='transaction_type', columns='state', values='transaction_count')
            c = df.pivot_table(index='transaction_type', columns='state', values='count', aggfunc='first')
            a = df.pivot_table(index='transaction_type', columns='state', values='amount', aggfunc='first')
            zmin = pivot_df.values.min()
            zmax = pivot_df.values.max()
            custom_data = np.dstack((c.values, a.values))
            fig = go.Figure(data=go.Heatmap(x=pivot_df.columns,
                                            y=pivot_df.index,
                                            z=pivot_df.values,
                                            colorscale='YlOrRd',
                                            customdata=custom_data,
                                            hovertemplate="State : %{x}"+
                                                "<br>Type : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>",
                                            zmin=zmin, zmax=zmax))
            fig.update_layout(height=500,
                              width=400,
                              xaxis_tickangle=-45,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)

        with st.expander("Detailed Info of Regionwise Transaction Count behaviour"):
            st.dataframe(df)
    elif selected_quarter == "All" and selected_year != "All" and selected_state == "All":
        query = f"""SELECT * FROM aggregated_transaction WHERE year={selected_year};"""
        df = pd.read_sql(query, engine)
        df['count'] = df['transaction_count'].apply(value_formats)
        df['amount'] = df['transaction_amount'].apply(value_formats)

        count_sum = value_formats(df['transaction_count'].sum())
        amount_sum = value_formats(df['transaction_amount'].sum())

        st.markdown(f"<h4 style ='color: Skyblue;'>India - {selected_year} Transaction Behaviour</h4>", unsafe_allow_html=True)

        quarter_dict = {}
        for quarter in quarter_list():
            df_y = df[df['quarter'] == quarter]
            quarter_dict[quarter] = []
            quarter_dict[quarter].append(value_formats(df_y['transaction_count'].sum()))
            quarter_dict[quarter].append(value_formats(df_y['transaction_amount'].sum()))

        quarters = quarter_list()+['All']
        cols = st.columns(len(quarters))
        for j, quarter in enumerate(quarters):
            with cols[j]:
                if quarter != "All":
                    with st.popover(f"{selected_year} - {quarter}"):
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Count: {quarter_dict[quarter][0]}</h5>", unsafe_allow_html=True)
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Amount : ₹ {quarter_dict[quarter][1]}</h5>", unsafe_allow_html=True)
                else:
                    with st.popover(f"Gross {selected_year}"):
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Count: {count_sum}</h5>", unsafe_allow_html=True)
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Amount : ₹ {amount_sum}</h5>", unsafe_allow_html=True)
        with st.container(border=True):
            fig = geo_choropleth_plot(df, 'state', 'transaction_count', "", 'quarter', None, None)
            ig = geo_choropleth_plot(df, 'state', 'transaction_count', "", 'quarter', None, None)

            initial_year = df[df['quarter'] == "Q4"]
            fig.update_traces(customdata = initial_year[['state', 'year', 'quarter', 'count', 'amount']].values,
                        hovertemplate="Year : %{customdata[1]}<br>Quarter :%{customdata[2]}<br>State : %{customdata[0]}<br>Transaction Count : %{customdata[3]}<br>Transaction Amount : ₹ %{customdata[4]}<extra></extra>")
        
            for frame in fig.frames:
                frame_df = df[df['quarter'] == frame.name]
                frame.data[0].customdata = frame_df[['state', 'year', 'quarter', 'count', 'amount']].values
                frame.data[0].hovertemplate = ("Year : %{customdata[1]}<br>Quarter :%{customdata[2]}<br>State : %{customdata[0]}<br>Transaction Count : %{customdata[3]}<br>Transaction Amount : ₹ %{customdata[4]}<extra></extra>")
                
            st.plotly_chart(fig, use_container_width=True)

            df1 = df.groupby(['state', 'year', 'quarter'])[['transaction_count', 'transaction_amount']].sum().reset_index()
            df1['count_s'] = df1['transaction_count'].apply(value_formats)
            df1['amount_s'] = df1['transaction_amount'].apply(value_formats)
            
            p_df1 = df1.pivot_table(index='quarter', columns='state', values='transaction_count')
            c = df1.pivot_table(index='quarter', columns='state', values='count_s', aggfunc='first')
            a = df1.pivot_table(index='quarter', columns='state', values='amount_s', aggfunc='first')
            zmin = p_df1.values.min()
            zmax = p_df1.values.max()
            custom_data = np.dstack((c.values, a.values))
            fig = go.Figure(data=go.Heatmap(x=p_df1.columns,
                                            y=p_df1.index,
                                            z=p_df1.values,
                                            colorscale='RdBu',
                                            customdata=custom_data,
                                            hovertemplate="State : %{x}"+
                                                "<br>Quarter : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>",
                                            zmin=zmin, zmax=zmax))
            fig.update_layout(height=500,
                              width=400,
                              xaxis_tickangle=-45,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander(f"Detailed Info of {selected_year} Quarterly Transaction Count behaviour"):
                st.dataframe(df1)

        st.markdown(f"<h4 style ='color: skyblue;'> India - {selected_year} Transaction Payment Type Distribution</h4>", unsafe_allow_html=True)

        with st.container(border=True):
            pivot_df = df.pivot_table(index='transaction_type', columns='state', values='transaction_count')
            c = df.pivot_table(index='transaction_type', columns='state', values='count', aggfunc='first')
            a = df.pivot_table(index='transaction_type', columns='state', values='amount', aggfunc='first')
            year = df.pivot_table(index='transaction_type', columns='state', values='year', aggfunc='first')
            zmin = pivot_df.values.min()
            zmax = pivot_df.values.max()
            custom_data = np.dstack((c.values, a.values, year.values))
            fig = go.Figure(data=go.Heatmap(x=pivot_df.columns,
                                            y=pivot_df.index,
                                            z=pivot_df.values,
                                            colorscale='YlOrRd',
                                            customdata=custom_data,
                                            hovertemplate="State : %{x}"+
                                                "<br>Year : %{customdata[2]}" +
                                                "<br>Type : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>",
                                            zmin=zmin, zmax=zmax))
            fig.update_layout(height=500,
                              width=400,
                              xaxis_tickangle=-45,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
        with st.expander(f"Detailed Info on {selected_state} Overall {selected_quarter}"):
            st.dataframe(df)
    elif selected_quarter != "All" and selected_year == "All" and selected_state == "All":
        query = f"""SELECT * FROM aggregated_transaction WHERE quarter='{selected_quarter}';"""
        df = pd.read_sql(query, engine)
        df['count'] = df['transaction_count'].apply(value_formats)
        df['amount'] = df['transaction_amount'].apply(value_formats)

        count_sum = value_formats(df['transaction_count'].sum())
        amount_sum = value_formats(df['transaction_amount'].sum())
        st.markdown(f"<h4 style ='color: skyblue;'> India - Overall {selected_quarter} Transaction Payment Type Distribution</h4>", unsafe_allow_html=True)

        year_dict = {}
        for year in year_list():
            df_y = df[df['year'] == year]
            year_dict[year] = []
            year_dict[year].append(value_formats(df_y['transaction_count'].sum()))
            year_dict[year].append(value_formats(df_y['transaction_amount'].sum()))

        years = year_list()+['All']
        cols = st.columns(len(years))
        for j, year in enumerate(years):
            with cols[j]:
                if year != "All":
                    with st.popover(f"{year}"):
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Count: {year_dict[year][0]}</h5>", unsafe_allow_html=True)
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Amount : ₹ {year_dict[year][1]}</h5>", unsafe_allow_html=True)
                else:
                    with st.popover("Gross"):
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Count: {count_sum}</h5>", unsafe_allow_html=True)
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Amount : ₹ {amount_sum}</h5>", unsafe_allow_html=True)

        with st.container(border=True):
            fig = geo_choropleth_plot(df, 'state', 'transaction_count', "", 'year', None, None)
            initial_year = df[df['year'] == df['year'].max()]
            fig.update_traces(customdata = initial_year[['state', 'year', 'quarter', 'count', 'amount']].values,
                        hovertemplate="Year : %{customdata[1]}<br>Quarter :%{customdata[2]}<br>State : %{customdata[0]}<br>Transaction Count : %{customdata[3]}<br>Transaction Amount : ₹ %{customdata[4]}<extra></extra>")
                
            for frame in fig.frames:
                frame_df = df[df['year'] == int(frame.name)]
                frame.data[0].customdata = frame_df[['state', 'year', 'quarter', 'count', 'amount']].values
                frame.data[0].hovertemplate = ("Year : %{customdata[1]}<br>Quarter :%{customdata[2]}<br>State : %{customdata[0]}<br>Transaction Count : %{customdata[3]}<br>Transaction Amount : ₹ %{customdata[4]}<extra></extra>")
                
            st.plotly_chart(fig, use_container_width=True)

            df1 = df.groupby(['state', 'year', 'quarter'])[['transaction_count', 'transaction_amount']].sum().reset_index()
            df1['count_s'] = df1['transaction_count'].apply(value_formats)
            df1['amount_s'] = df1['transaction_amount'].apply(value_formats)
            
            p_df1 = df1.pivot_table(index='year', columns='state', values='transaction_count')
            c = df1.pivot_table(index='year', columns='state', values='count_s', aggfunc='first')
            a = df1.pivot_table(index='year', columns='state', values='amount_s', aggfunc='first')
            zmin = p_df1.values.min()
            zmax = p_df1.values.max()
            custom_data = np.dstack((c.values, a.values))
            fig = go.Figure(data=go.Heatmap(x=p_df1.columns,
                                            y=p_df1.index,
                                            z=p_df1.values,
                                            colorscale='blues',
                                            customdata=custom_data,
                                            hovertemplate="State : %{x}"+
                                                "<br>Year : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>",
                                            zmin=zmin, zmax=zmax))
            fig.update_layout(height=500,
                              width=400,
                              xaxis_tickangle=-45,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander(f"Detailed Info of Yearly {selected_quarter} Transaction behaviour"):
                st.dataframe(df1)

        st.markdown(f"<h4 style ='color: skyblue;'> India - Overall {selected_quarter} Transaction Payment Type Distribution</h4>", unsafe_allow_html=True)

        with st.container(border=True):
            st.caption(f"{selected_state} (Overall {selected_quarter}) - Payment Categorywise Transaction")
            pivot_df = df.pivot_table(index='transaction_type', columns='state', values='transaction_count')
            c = df.pivot_table(index='transaction_type', columns='state', values='count', aggfunc='first')
            a = df.pivot_table(index='transaction_type', columns='state', values='amount', aggfunc='first')
            q = df.pivot_table(index='transaction_type', columns='state', values='quarter', aggfunc='first')

            custom_data = np.dstack((c.values, a.values, q.values))
            fig = go.Figure(data=go.Heatmap(x=pivot_df.columns,
                                            y=pivot_df.index,
                                            z=pivot_df.values,
                                            colorscale='YlOrRd',
                                            customdata=custom_data,
                                            hovertemplate="State : %{x}"+
                                                "<br>Quarter : %{customdata[2]}" +
                                                "<br>Type : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>"))
            fig.update_layout(height=500,
                              width=400,
                              xaxis_tickangle=-45,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
        with st.expander(f"Detailed Info on {selected_state} Overall {selected_quarter}"):
            st.dataframe(df)

    elif selected_quarter != "All" and selected_year != "All" and selected_state == "All":
        query = f"""SELECT * FROM aggregated_transaction 
                    WHERE quarter='{selected_quarter}' and year={selected_year};"""
        df = pd.read_sql(query, engine)
        df['count'] = df['transaction_count'].apply(value_formats)
        df['amount'] = df['transaction_amount'].apply(value_formats)

        count_sum = value_formats(df['transaction_count'].sum())
        amount_sum = value_formats(df['transaction_amount'].sum())
        st.markdown(f"<h4 style ='color: skyblue;'> India - {selected_year}({selected_quarter}) Transaction Payment Type Distribution</h4>", unsafe_allow_html=True)

        with st.popover(f"Gross {selected_year}-{selected_quarter}"):
            st.markdown(f"<h5 style ='color: Green;'>Transaction Count: {count_sum}</h5>", unsafe_allow_html=True)
            st.markdown(f"<h5 style ='color: Green;'>Transaction Amount : ₹ {amount_sum}</h5>", unsafe_allow_html=True)
        with st.container(border=True):
            fig = geo_choropleth_plot(df, 'state', 'transaction_count', "", None, None, None)
            fig.update_traces(customdata = np.stack((df['state'], df['quarter'], df['count'], df['amount'], df['year']), axis=1),
                            hovertemplate="State : %{customdata[0]}<br>"\
                                        "Year : %{customdata[4]}<br>"\
                                        "Quarter : %{customdata[1]}<br>"\
                                        "Transaction Count : %{customdata[2]}<br>"\
                                        "Transaction Amount : ₹ %{customdata[3]}<extra></extra>")
            st.plotly_chart(fig)
            df1 = df.groupby(['state', 'year', 'quarter'])[['transaction_count', 'transaction_amount']].sum().reset_index()
            df1['count_s'] = df1['transaction_count'].apply(value_formats)
            df1['amount_s'] = df1['transaction_amount'].apply(value_formats)
            
            p_df1 = df1.pivot_table(index='quarter', columns='state', values='transaction_count')
            c = df1.pivot_table(index='quarter', columns='state', values='count_s', aggfunc='first')
            a = df1.pivot_table(index='quarter', columns='state', values='amount_s', aggfunc='first')
            zmin = p_df1.values.min()
            zmax = p_df1.values.max()
            custom_data = np.dstack((c.values, a.values))
            fig = go.Figure(data=go.Heatmap(x=p_df1.columns,
                                            y=p_df1.index,
                                            z=p_df1.values,
                                            colorscale='RdBu',
                                            customdata=custom_data,
                                            hovertemplate="State : %{x}"+
                                                "<br>Quarter : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>",
                                            zmin=zmin, zmax=zmax))
            fig.update_layout(yaxis_title=f"{selected_year}",
                              width=400,
                              xaxis_tickangle=-45,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander(f"Detailed Info of {selected_year}({selected_quarter}) Transaction Count behaviour"):
                st.dataframe(df1)

        st.markdown(f"<h4 style ='color: skyblue;'> India - {selected_year}({selected_quarter}) Transaction Payment Type Distribution</h4>", unsafe_allow_html=True)
        with st.container(border=True):
            pivot_df = df.pivot_table(index='transaction_type', columns='state', values='transaction_count')
            c = df.pivot_table(index='transaction_type', columns='state', values='count', aggfunc='first')
            a = df.pivot_table(index='transaction_type', columns='state', values='amount', aggfunc='first')
            q = df.pivot_table(index='transaction_type', columns='state', values='quarter', aggfunc='first')
            y = df.pivot_table(index='transaction_type', columns='state', values='year', aggfunc='first')

            custom_data = np.dstack((c.values, a.values, q.values, y.values))
            fig = go.Figure(data=go.Heatmap(x=pivot_df.columns,
                                            y=pivot_df.index,
                                            z=pivot_df.values,
                                            colorscale='YlOrRd',
                                            customdata=custom_data,
                                            hovertemplate="State : %{x}"+
                                                "<br>Year : %{customdata[3]}" +
                                                "<br>Quarter : %{customdata[2]}" +
                                                "<br>Type : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>"))
            fig.update_layout(height=500,
                              width=400,
                              xaxis_tickangle=-45,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
        with st.expander(f"Detailed Info on {selected_state} ({selected_quarter})"):
            st.dataframe(df)
    elif selected_quarter != "All" and selected_year == "All" and selected_state != "All":
        query = f"""SELECT * FROM aggregated_transaction
                    WHERE state='{selected_state}' and quarter='{selected_quarter}'"""
        df = pd.read_sql(query, engine)
        st.markdown(f"<h4 style ='color: skyblue;'> {selected_state} (Overall {selected_quarter}) - Transaction Behaviour</h4>", unsafe_allow_html=True)

        with st.popover(f"Gross {selected_quarter}"):
            st.markdown(f"<h5 style ='color: Green;'>Transaction Count: {value_formats(df['transaction_count'].sum())}</h5>", unsafe_allow_html=True)
            st.markdown(f"<h5 style ='color: Green;'>Transaction Amount : ₹ {value_formats(df['transaction_amount'].sum())}</h5>", unsafe_allow_html=True)
        with st.container(border=True):

            fig = geo_choropleth_plot_statewise(df, 'state', 'transaction_count', "", selected_state, 'year')
            initial_year = df[df['year'] == df['year'].max()]
            initial_year['count_sum_col'] = np.full(len(initial_year), value_formats(initial_year['transaction_count'].sum()))
            initial_year['amount_sum_col'] = np.full(len(initial_year), value_formats(initial_year['transaction_amount'].sum()))
            fig.update_traces(customdata = initial_year[['state', 'year', 'quarter', 'count_sum_col', 'amount_sum_col']].values,
                        hovertemplate="Year : %{customdata[1]}<br>Quarter :%{customdata[2]}<br>State : %{customdata[0]}<br>Transaction Count : %{customdata[3]}<br>Transaction Amount : ₹ %{customdata[4]}<extra></extra>")
        
            for frame in fig.frames:
                frame_df = df[df['year'] == int(frame.name)]
                frame_df['count_sum'] = np.full(len(frame_df), value_formats(frame_df['transaction_count'].sum()))
                frame_df['amount_sum'] = np.full(len(frame_df), value_formats(frame_df['transaction_amount'].sum()))
                frame.data[0].customdata = frame_df[['state', 'year', 'quarter', 'count_sum', 'amount_sum']].values
                frame.data[0].hovertemplate = ("Year : %{customdata[1]}<br>Quarter :%{customdata[2]}<br>State : %{customdata[0]}<br>Transaction Count : %{customdata[3]}<br>Transaction Amount : ₹ %{customdata[4]}<extra></extra>")
                
            st.plotly_chart(fig, use_container_width=True)

            df1 = df.groupby(['state', 'year', 'quarter'])[['transaction_count', 'transaction_amount']].sum().reset_index()
            df1['count_s'] = df1['transaction_count'].apply(value_formats)
            df1['amount_s'] = df1['transaction_amount'].apply(value_formats)

            p_df1 = df1.pivot_table(index='quarter', columns='year', values='transaction_count')
            c = df1.pivot_table(index='quarter', columns='year', values='count_s', aggfunc='first')
            a = df1.pivot_table(index='quarter', columns='year', values='amount_s', aggfunc='first')
            zmin = p_df1.values.min()
            zmax = p_df1.values.max()
            custom_data = np.dstack((c.values, a.values))
            fig = go.Figure(data=go.Heatmap(x=p_df1.columns,
                                            y=p_df1.index,
                                            z=p_df1.values,
                                            colorscale='RdBu',
                                            customdata=custom_data,
                                            hovertemplate="Quarter : %{y}"+
                                                "<br>Year : %{x}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>",
                                            zmin=zmin, zmax=zmax))
            fig.update_layout(height=150,
                              width=400,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander(f"Detailed Info of {selected_state}({selected_quarter}) Transaction Count behaviour"):
                st.dataframe(df1)

        st.markdown(f"<h4 style ='color: skyblue;'>{selected_state} (Overall {selected_quarter}) - Transaction Payment Type Distribution</h4>", unsafe_allow_html=True)
        with st.container(border=True):
            df['count'] = df['transaction_count'].apply(value_formats)
            df['amount'] = df['transaction_amount'].apply(value_formats)
            pivot_df = df.pivot_table(index='transaction_type', columns='year', values='transaction_count')
            c = df.pivot_table(index='transaction_type', columns='year', values='count', aggfunc='first')
            a = df.pivot_table(index='transaction_type', columns='year', values='amount', aggfunc='first')
            q = df.pivot_table(index='transaction_type', columns='year', values='quarter', aggfunc='first')

            custom_data = np.dstack((c.values, a.values, q.values))
            fig = go.Figure(data=go.Heatmap(x=pivot_df.columns,
                                            y=pivot_df.index,
                                            z=pivot_df.values,
                                            colorscale='YlOrRd',
                                            customdata=custom_data,
                                            hovertemplate="Year : %{x}"+
                                                "<br>Quarter : %{customdata[2]}" +
                                                "<br>Type : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>"))
            fig.update_layout(height=400,
                              width=400,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
        with st.expander(f"Detailed Info on {selected_state} Overall {selected_quarter}"):
            st.dataframe(df)
    elif selected_state != "All" and selected_quarter == "All" and selected_year == "All":
        query = f"""SELECT * FROM aggregated_transaction
                    WHERE state='{selected_state}'"""
        df = pd.read_sql(query, engine)
        df['count'] = df['transaction_count'].apply(value_formats)
        df['amount'] = df['transaction_amount'].apply(value_formats)

        st.markdown(f"<h4 style ='color: skyblue;'> {selected_state} (Overall) - Transaction Behaviour</h4>", unsafe_allow_html=True)

        year_dict = {}
        for year in year_list():
            df_y = df[df['year'] == year]
            year_dict[year] = []
            year_dict[year].append(value_formats(df_y['transaction_count'].sum()))
            year_dict[year].append(value_formats(df_y['transaction_amount'].sum()))

        years = year_list()+['All']
        cols = st.columns(len(years))

        for j, year in enumerate(years):
            with cols[j]:
                if year != "All":
                    with st.popover(f"{year}"):
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Count: {year_dict[year][0]}</h5>", unsafe_allow_html=True)
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Amount : ₹ {year_dict[year][1]}</h5>", unsafe_allow_html=True)
                else:
                    with st.popover("Gross"):
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Count: {value_formats(df['transaction_count'].sum())}</h5>", unsafe_allow_html=True)
                        st.markdown(f"<h5 style ='color: Green;'>Transaction Amount : ₹ {value_formats(df['transaction_amount'].sum())}</h5>", unsafe_allow_html=True)
        with st.container(border=True):
            fig = geo_choropleth_plot_statewise(df, 'state', 'transaction_count', "", selected_state, 'year')
            initial_year = df[df['year'] == df['year'].max()]
            initial_year['count_sum_col'] = np.full(len(initial_year), value_formats(initial_year['transaction_count'].sum()))
            initial_year['amount_sum_col'] = np.full(len(initial_year), value_formats(initial_year['transaction_amount'].sum()))
            fig.update_traces(customdata = initial_year[['state', 'year', 'count_sum_col', 'amount_sum_col']].values,
                        hovertemplate="Year : %{customdata[1]}<br>State : %{customdata[0]}<br>Transaction Count : %{customdata[2]}<br>Transaction Amount : ₹ %{customdata[3]}<extra></extra>")
        
            for frame in fig.frames:
                frame_df = df[df['year'] == int(frame.name)]
                frame_df['count_sum'] = np.full(len(frame_df), value_formats(frame_df['transaction_count'].sum()))
                frame_df['amount_sum'] = np.full(len(frame_df), value_formats(frame_df['transaction_amount'].sum()))
                frame.data[0].customdata = frame_df[['state', 'year', 'count_sum', 'amount_sum']].values
                frame.data[0].hovertemplate = ("Year : %{customdata[1]}<br>State : %{customdata[0]}<br>Transaction Count : %{customdata[2]}<br>Transaction Amount : ₹ %{customdata[3]}<extra></extra>")
                
            st.plotly_chart(fig, use_container_width=True)

            df1 = df.groupby(['state', 'year', 'quarter'])[['transaction_count', 'transaction_amount']].sum().reset_index()
            df1['count_s'] = df1['transaction_count'].apply(value_formats)
            df1['amount_s'] = df1['transaction_amount'].apply(value_formats)

            p_df1 = df1.pivot_table(index='quarter', columns='year', values='transaction_count')
            c = df1.pivot_table(index='quarter', columns='year', values='count_s', aggfunc='first')
            a = df1.pivot_table(index='quarter', columns='year', values='amount_s', aggfunc='first')
            zmin = p_df1.values.min()
            zmax = p_df1.values.max()
            custom_data = np.dstack((c.values, a.values))
            fig = go.Figure(data=go.Heatmap(x=p_df1.columns,
                                            y=p_df1.index,
                                            z=p_df1.values,
                                            colorscale='RdBu',
                                            customdata=custom_data,
                                            hovertemplate="Quarter : %{y}"+
                                                "<br>Year : %{x}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>",
                                            zmin=zmin, zmax=zmax))
            fig.update_layout(height=300,
                              width=400,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander(f"Detailed Info of {selected_state}(Overall) Transaction Count behaviour"):
                st.dataframe(df1)

        st.markdown(f"<h4 style ='color: skyblue;'> {selected_state} (Overall) - Transaction Payment Type Distribution</h4>", unsafe_allow_html=True)
        with st.container(border=True):
            pivot_df = df.pivot_table(index='transaction_type', columns='year', values='transaction_count')
            c = df.pivot_table(index='transaction_type', columns='year', values='count', aggfunc='first')
            a = df.pivot_table(index='transaction_type', columns='year', values='amount', aggfunc='first')
            q = df.pivot_table(index='transaction_type', columns='year', values='quarter', aggfunc='first')

            custom_data = np.dstack((c.values, a.values, q.values))
            fig = go.Figure(data=go.Heatmap(x=pivot_df.columns,
                                            y=pivot_df.index,
                                            z=pivot_df.values,
                                            colorscale='YlOrRd',
                                            customdata=custom_data,
                                            hovertemplate="Year : %{x}"+
                                                "<br>Quarter : %{customdata[2]}" +
                                                "<br>Type : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>"))
            fig.update_layout(height=400,
                              width=400,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
        with st.expander(f"Detailed Info on {selected_state} - All years"):
            st.dataframe(df)
    elif selected_quarter == "All" and selected_year != "All":
        query = f"""SELECT * FROM aggregated_transaction
                    WHERE state='{selected_state}' AND year={selected_year};"""
        df = pd.read_sql(query, engine)
        st.markdown(f"<h4 style ='color: skyblue;'> {selected_state} ({selected_year}) - Transaction Behaviour</h4>", unsafe_allow_html=True)

        with st.popover(f"Gross {selected_year}"):
            st.markdown(f"<h5 style ='color: Green;'>Transaction Count: {value_formats(df['transaction_count'].sum())}</h5>", unsafe_allow_html=True)
            st.markdown(f"<h5 style ='color: Green;'>Transaction Amount : ₹ {value_formats(df['transaction_amount'].sum())}</h5>", unsafe_allow_html=True)

        with st.container(border=True):
            fig = geo_choropleth_plot_statewise(df, 'state', 'transaction_count', "", selected_state, 'quarter')
            initial = df[df['quarter'] == "Q1"]
            initial['count_sum_col'] = np.full(len(initial), value_formats(initial['transaction_count'].sum()))
            initial['amount_sum_col'] = np.full(len(initial), value_formats(initial['transaction_amount'].sum()))
            fig.update_traces(customdata = initial[['state', 'year', 'quarter', 'count_sum_col', 'amount_sum_col']].values,
                        hovertemplate="Year : %{customdata[1]}<br>Quarter :%{customdata[2]}<br>State : %{customdata[0]}<br>Transaction Count : %{customdata[3]}<br>Transaction Amount : ₹ %{customdata[4]}<extra></extra>")
        
            for frame in fig.frames:
                frame_df = df[df['quarter'] == frame.name]
                frame_df['count_sum'] = np.full(len(frame_df), value_formats(frame_df['transaction_count'].sum()))
                frame_df['amount_sum'] = np.full(len(frame_df), value_formats(frame_df['transaction_amount'].sum()))
                frame.data[0].customdata = frame_df[['state', 'year', 'quarter', 'count_sum', 'amount_sum']].values
                frame.data[0].hovertemplate = ("Year : %{customdata[1]}<br>Quarter :%{customdata[2]}<br>State : %{customdata[0]}<br>Transaction Count : %{customdata[3]}<br>Transaction Amount : ₹ %{customdata[4]}<extra></extra>")
                
            st.plotly_chart(fig, use_container_width=True)

            df1 = df.groupby(['state', 'year', 'quarter'])[['transaction_count', 'transaction_amount']].sum().reset_index()
            df1['count_s'] = df1['transaction_count'].apply(value_formats)
            df1['amount_s'] = df1['transaction_amount'].apply(value_formats)

            p_df1 = df1.pivot_table(index='year', columns='quarter', values='transaction_count')
            c = df1.pivot_table(index='year', columns='quarter', values='count_s', aggfunc='first')
            a = df1.pivot_table(index='year', columns='quarter', values='amount_s', aggfunc='first')
            zmin = p_df1.values.min()
            zmax = p_df1.values.max()
            custom_data = np.dstack((c.values, a.values))
            fig = go.Figure(data=go.Heatmap(x=p_df1.columns,
                                            y=p_df1.index,
                                            z=p_df1.values,
                                            colorscale='RdBu',
                                            customdata=custom_data,
                                            hovertemplate="Quarter : %{x}"+
                                                "<br>Year : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>",
                                            zmin=zmin, zmax=zmax))
            fig.update_layout(height=150,
                              width=400,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander(f"Detailed Info of {selected_state}({selected_year}) Transaction Count behaviour"):
                st.dataframe(df1)

        st.markdown(f"<h4 style ='color: skyblue;'>{selected_state} ({selected_year}) - Transaction Payment Type Distribution</h4>", unsafe_allow_html=True)
        with st.container(border=True):
            df['count'] = df['transaction_count'].apply(value_formats)
            df['amount'] = df['transaction_amount'].apply(value_formats)
            pivot_df = df.pivot_table(index='transaction_type', columns='quarter', values='transaction_count')
            c = df.pivot_table(index='transaction_type', columns='quarter', values='count', aggfunc='first')
            a = df.pivot_table(index='transaction_type', columns='quarter', values='amount', aggfunc='first')
            q = df.pivot_table(index='transaction_type', columns='quarter', values='year', aggfunc='first')

            custom_data = np.dstack((c.values, a.values, q.values))
            fig = go.Figure(data=go.Heatmap(x=pivot_df.columns,
                                            y=pivot_df.index,
                                            z=pivot_df.values,
                                            colorscale='YlOrRd',
                                            customdata=custom_data,
                                            hovertemplate="Year : %{customdata[2]}"+
                                                "<br>Quarter : %{x}" +
                                                "<br>Type : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>"))
            fig.update_layout(height=400,
                              width=400,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig, use_container_width=True)
        with st.expander(f"Detailed Info on {selected_state} in {selected_year} (for all quarters)"):
            st.dataframe(df)
    else:
        query = f"""SELECT * FROM aggregated_transaction
                    WHERE state='{selected_state}' AND year={selected_year} AND quarter='{selected_quarter}';"""
        df = pd.read_sql(query, engine)
        df['count'] = df['transaction_count'].apply(value_formats)
        df['amount'] = df['transaction_amount'].apply(value_formats)
        count_sum = value_formats(df['transaction_count'].sum())
        amount_sum = value_formats(df['transaction_amount'].sum())

        st.markdown(f"<h4 style ='color: skyblue;'> {selected_state} {selected_year}({selected_quarter}) - Transaction Behaviour</h4>", unsafe_allow_html=True)

        with st.popover(f"Gross {selected_year} - {selected_quarter}"):
            st.markdown(f"<h5 style = 'color: green;'> Transaction Amount : ₹ {amount_sum}</h5>", unsafe_allow_html=True)
            st.markdown(f"<h5 style = 'color: green;'> Transaction Count : {count_sum}</h5>", unsafe_allow_html=True)
        with st.container(border=True):
            count_sum_col = np.full(len(df), count_sum)
            amount_sum_col = np.full(len(df), amount_sum)
            fig = geo_choropleth_plot_statewise(df, 'state', 'transaction_count', "", selected_state, None)
            fig.update_traces(customdata = np.stack((df['state'], df['year'], df['quarter'], count_sum_col, amount_sum_col), axis=1),
                            hovertemplate="State : %{customdata[0]}<br>"\
                                        "Year : %{customdata[1]}<br>" \
                                        "Quarter : %{customdata[2]}<br>"\
                                        "Transaction Count : %{customdata[3]}<br>"\
                                        "Transaction Amount : ₹ %{customdata[4]}<extra></extra>")
            st.plotly_chart(fig)
        st.markdown(f"<h4 style ='color: skyblue;'>{selected_state} ({selected_year}-{selected_quarter}) - Transaction Payment Type Distribution</h4>", unsafe_allow_html=True)
        with st.container(border=True):
            pivot_df = df.pivot_table(index='transaction_type', columns='year', values='transaction_count')
            c = df.pivot_table(index='transaction_type', columns='year', values='count', aggfunc='first')
            a = df.pivot_table(index='transaction_type', columns='year', values='amount', aggfunc='first')
            q = df.pivot_table(index='transaction_type', columns='year', values='quarter', aggfunc='first')

            custom_data = np.dstack((c.values, a.values, q.values))
            fig = go.Figure(data=go.Heatmap(x=pivot_df.columns,
                                            y=pivot_df.index,
                                            z=pivot_df.values,
                                            colorscale='YlOrRd',
                                            customdata=custom_data,
                                            hovertemplate="Year : %{x}"+
                                                "<br>Quarter : %{customdata[2]}" +
                                                "<br>Type : %{y}"+
                                                "<br>Transaction Count: %{customdata[0]}"+
                                                "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>"))
            fig.update_layout(height=400,
                              margin=dict(t=0,b=0))
            st.plotly_chart(fig)
        with st.expander(f"Detailed Info on {selected_state} in {selected_year} - {selected_quarter}"):
            st.dataframe(df)

def yearwise_analysis():    
    selected_year = st.sidebar.selectbox("Choose Year: ", ["All"]+year_list(), key="year_selectbox")
    st.markdown(f"<h4 style ='color: skyblue;'>Year({selected_year}) Statewise - High and Low Volumed Transaction</h4>", unsafe_allow_html=True)
    if selected_year != "All":
        query = f"""SELECT state, year, SUM(transaction_count) as count, SUM(transaction_amount) as amount
                    FROM aggregated_transaction WHERE year={selected_year} GROUP BY state ORDER BY count DESC;"""
    else:
        query = f"""SELECT state, SUM(transaction_count) as count, SUM(transaction_amount) as amount
                    FROM aggregated_transaction GROUP BY state ORDER BY count DESC;"""
        
    with engine.connect() as conn:
        conn.execute(text("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))"))
        df = pd.read_sql(query, conn)
    df['count_f'] = df['count'].apply(value_formats)
    df['amount_f'] = df['amount'].apply(value_formats)

    with st.container(border=True):
        if selected_year != "All":
            tab1, tab2, tab3 = st.tabs([f"TOP 10 States({selected_year})",f"MODERATE States({selected_year})", f"BOTTOM 10 States({selected_year})"])
        else:
            tab1, tab2, tab3 = st.tabs(["TOP 10 States(All years)","MODERATE States(All Years)","BOTTOM 10 States(All years)"])
        with tab1:
            if selected_year != "All":
                top_df = df.head(10)[::-1]
            else:
                top_df = df.head(10)[::-1]
            fig = px.bar(top_df, x="count", y="state", color="count", color_continuous_scale="sunsetdark", text_auto=True)
            fig.update_traces(customdata=top_df[["count_f", "amount_f"]].values,
                              hovertemplate="State: %{y}" \
                                            "<br>Transaction Volume: %{customdata[0]}" \
                                            "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>")
            fig.update_layout(xaxis_title="Volume", yaxis_title="State", height=450, margin=dict(t=0,b=0,l=0,r=0))
            st.plotly_chart(fig, use_container_width=True)
            if selected_year != "All":
                with st.expander(f"Detailed Info on TOP 10 States({selected_year})"):
                    st.dataframe(top_df)
            else:
                with st.expander("Detailed Info on TOP 10 States(All years)"):
                    st.dataframe(top_df)
        with tab2:
            if selected_year != "All":
                mid_df = df.iloc[10:-10][::-1]
            else:
                mid_df = df.iloc[10:-10][::-1]
            fig = px.bar(mid_df, x="count", y="state", color="count", color_continuous_scale="sunsetdark", text_auto=True)
            fig.update_traces(customdata=mid_df[["count_f", "amount_f"]].values,
                              hovertemplate="State: %{y}" \
                                            "<br>Transaction Volume: %{customdata[0]}" \
                                            "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>")
            fig.update_layout(xaxis_title="Volume", yaxis_title="State", height=450, margin=dict(t=0,b=0,l=0,r=0))
            st.plotly_chart(fig, use_container_width=True)
            if selected_year != "All":
                with st.expander(f"Detailed Info on Moderate States({selected_year})"):
                    st.dataframe(mid_df)
            else:
                with st.expander("Detailed Info on Moderate States(All years)"):
                    st.dataframe(mid_df)
        with tab3:
            if selected_year != "All":
                bottom_df = df.tail(10)
            else:
                bottom_df = df.tail(10)
            fig = px.bar(bottom_df, x="count", y="state", color="count", color_continuous_scale="sunsetdark", text_auto=True)
            fig.update_traces(customdata=top_df[["count_f", "amount_f"]].values,
                              hovertemplate="State: %{y}" \
                                            "<br>Transaction Volume: %{customdata[0]}" \
                                            "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>")
            fig.update_layout(xaxis_title="Volume", yaxis_title="State",height=450, margin=dict(t=0,b=0,l=0,r=0))
            st.plotly_chart(fig, use_container_width=True)
            if selected_year != "All":
                with st.expander(f"Detailed Info on BOTTOM 10 States({selected_year})"):
                    st.dataframe(bottom_df)
            else:
                with st.expander("Detailed Info on BOTTOM 10 States(All years)"):
                    st.dataframe(bottom_df)
    st.markdown("\n")
    #selected_year = st.selectbox("Choose Year: ", ["All"]+year_list(), key="year_selectbox1")
    st.markdown(f"<h4 style ='color: skyblue;'>Year({selected_year}) Districtwise - High and Low Volumed Transaction</h4>", unsafe_allow_html=True)

    if selected_year != "All":
        query = f"""SELECT state, district, year, SUM(transaction_count) as count, SUM(transaction_amount) as amount
                    FROM top_transaction_districtwise WHERE year={selected_year}
                    GROUP BY state, district ORDER BY count DESC;"""
    else:
        query = """SELECT state, district, SUM(transaction_count) as count, SUM(transaction_amount) as amount
                    FROM top_transaction_districtwise
                    GROUP BY state, district ORDER BY count DESC;"""

    with engine.connect() as conn:
        conn.execute(text("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))"))
        df = pd.read_sql(query, conn)
    df['count_f'] = df['count'].apply(value_formats)
    df['amount_f'] = df['amount'].apply(value_formats)

    with st.container(border=True):
        if selected_year != "All":
            tab1, tab2 = st.tabs([f"TOP 10 Districts({selected_year})",f"BOTTOM 10 Districts({selected_year})"])
        else:
            tab1, tab2 = st.tabs(["TOP 10 Districts(All years)","BOTTOM 10 Districts(All years)"])
        with tab1:
            if selected_year != "All":
                top_df = df.head(10)[::-1]
            else:
                top_df = df.head(10)[::-1]
            fig = px.bar(top_df, x="count", y="district", color="count", color_continuous_scale="oranges", text_auto=True)
            fig.update_traces(customdata=top_df[["count_f", "amount_f","state"]].values,
                              hovertemplate="State: %{customdata[2]}"
                                            "<br>District: %{y}" \
                                            "<br>Transaction Volume: %{customdata[0]}" \
                                            "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>")
            fig.update_layout(xaxis_title="Volume", yaxis_title="District",height=450, margin=dict(t=0,b=0,l=0,r=0))
            st.plotly_chart(fig, use_container_width=True)
            if selected_year != "All":
                with st.expander(f"Detailed Info on TOP 10 Districts({selected_year})"):
                    st.dataframe(top_df)
            else:
                with st.expander("Detailed Info on TOP 10 Districts(All years)"):
                    st.dataframe(top_df)
        with tab2:
            if selected_year != "All":
                bottom_df = df.tail(10)
            else:
                bottom_df = df.tail(10)
            fig = px.bar(bottom_df, x="count", y="district", color="count", color_continuous_scale="oranges", text_auto=True)
            fig.update_traces(customdata=bottom_df[["count_f", "amount_f", "state"]].values,
                              hovertemplate="State: %{customdata[2]}"
                                            "<br>District: %{y}" \
                                            "<br>Transaction Volume: %{customdata[0]}" \
                                            "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>")
            fig.update_layout(xaxis_title="Volume", yaxis_title="District",height=450, margin=dict(t=0,b=0,l=0,r=0))
            st.plotly_chart(fig, use_container_width=True)
            if selected_year != "All":
                with st.expander(f"Detailed Info on BOTTOM 10 Districts({selected_year})"):
                    st.dataframe(bottom_df)
            else:
                with st.expander("Detailed Info on BOTTOM 10 Districts(All years)"):
                    st.dataframe(bottom_df)  
    
    st.markdown("\n")
    #selected_year = st.selectbox("Choose Year: ", ["All"]+year_list(), key="year_selectbox2")
    st.markdown(f"<h4 style ='color: skyblue;'>Year({selected_year}) Pincodewise - High and Low Volumed Transaction</h4>", unsafe_allow_html=True)

    if selected_year != "All":
        query = f"""SELECT state, pincode, year, SUM(transaction_count) as count, SUM(transaction_amount) as amount
                    FROM top_transaction_pincodewise WHERE year={selected_year}
                    GROUP BY state, pincode ORDER BY count DESC;"""
    else:
        query = """SELECT state, pincode, SUM(transaction_count) as count, SUM(transaction_amount) as amount
                    FROM top_transaction_pincodewise
                    GROUP BY state, pincode ORDER BY count DESC;"""

    with engine.connect() as conn:
        conn.execute(text("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))"))
        df = pd.read_sql(query, conn)
    df['pincode'] = df['pincode'].astype(str)
    df['count_f'] = df['count'].apply(value_formats)
    df['amount_f'] = df['amount'].apply(value_formats)

    with st.container(border=True):
        if selected_year != "All":
            tab1, tab2 = st.tabs([f"TOP 10 Pincodes({selected_year})",f"BOTTOM 10 Pincodes({selected_year})"])
        else:
            tab1, tab2 = st.tabs(["TOP 10 Pincodes(All years)","BOTTOM 10 Pincodes(All years)"])
        with tab1:
            if selected_year != "All":
                top_df = df.head(10)[::-1]
            else:
                top_df = df.head(10)[::-1]
            fig = px.bar(top_df, x="count", y="pincode", color="count", color_continuous_scale="tropic", text_auto=True)
            fig.update_traces(customdata=top_df[["count_f", "amount_f","state"]].values,
                              hovertemplate="State: %{customdata[2]}"
                                            "<br>Pincode: %{y}" \
                                            "<br>Transaction Volume: %{customdata[0]}" \
                                            "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>")
            fig.update_layout(xaxis_title="Volume", yaxis_title="Pincode", yaxis=dict(type="category"), height=450, margin=dict(t=0,b=0,l=0,r=0))
            st.plotly_chart(fig, use_container_width=True)
            if selected_year != "All":
                with st.expander(f"Detailed Info on TOP 10 Pincodes({selected_year})"):
                    st.dataframe(top_df)
            else:
                with st.expander("Detailed Info on TOP 10 Pincodes(All years)"):
                    st.dataframe(top_df)
        with tab2:
            if selected_year != "All":
                bottom_df = df.tail(10)
            else:
                bottom_df = df.tail(10)
            fig = px.bar(bottom_df, x="count", y="pincode", color="count", color_continuous_scale="tropic", text_auto=True)
            fig.update_traces(customdata=top_df[["count_f", "amount_f", "state"]].values,
                              hovertemplate="State: %{customdata[2]}"
                                            "<br>Pincode: %{y}" \
                                            "<br>Transaction Volume: %{customdata[0]}" \
                                            "<br>Transaction Amount: ₹ %{customdata[1]}<extra></extra>")
            fig.update_layout(xaxis_title="Volume", yaxis_title="Pincode", yaxis=dict(type="category"), height=450, margin=dict(t=0,b=0,l=0,r=0))
            st.plotly_chart(fig, use_container_width=True)
            if selected_year != "All":
                with st.expander(f"Detailed Info on BOTTOM 10 Pincodes({selected_year})"):
                    st.dataframe(bottom_df)
            else:
                with st.expander("Detailed Info on BOTTOM 10 Pincodes(All years)"):
                    st.dataframe(bottom_df) 
    st.markdown("<h4 style ='color: skyblue;'>Year Over Year Rising Transaction Volume</h4>", unsafe_allow_html=True)

    query = """SELECT state, year, district, SUM(transaction_count) as count 
                FROM map_transaction GROUP BY state, year, district;"""
    df_yearly = pd.read_sql(query, engine)

    df_yearly.sort_values(by=['state', 'district', 'year'], inplace=True)
    df_yearly['prev_year_count'] = df_yearly.groupby(['state', 'district'])['count'].shift(1)
    df_yearly['growth_percentage'] = ((df_yearly['count'] - df_yearly['prev_year_count']) / df_yearly['prev_year_count']) * 100
    rising = df_yearly[(df_yearly['prev_year_count'] < 10000) & (df_yearly['growth_percentage'] > 100)].reset_index()
    with st.container(border=True):
        rising_sorted = rising.sort_values(by='growth_percentage', ascending=False)
        rising_sorted['state_year'] = rising_sorted['state'] + ' - ' + rising_sorted['year'].astype(str)
        fig = px.bar(rising_sorted, x='district', y='growth_percentage', color='state_year',
                    hover_data=['state', 'year', 'prev_year_count', 'count'],
                    labels={'growth_pct': 'YoY Growth (%)', 'district': 'District'})

        fig.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Year Over year Rising Growth"):
            st.dataframe(rising)

def overall_analysis():
    selected_state = st.sidebar.selectbox("Choose State: ", ['All'] + state_list(), key="state_selectbox")

    if selected_state == 'All':
        st.markdown("<h4 style ='color: skyblue;'>India Overall - Transaction Volume</h4>", unsafe_allow_html=True)
        with st.container(border=True):
            query = "SELECT * FROM top_transaction_districtwise;"
            df = pd.read_sql(query, engine)
            df['count'] = df['transaction_count'].apply(value_formats)

            fig = px.sunburst(df, path=['state', 'district', 'year', 'quarter'], values='transaction_count', color='transaction_count', color_continuous_scale='Plasma')        
            fig.update_traces(insidetextorientation='radial',
                              hovertemplate="Label= %{label}" \
                                            "<br>Id= %{id}" \
                                            "<br>Parent= %{percentParent:.2%}<br>"\
                                            "Root= %{percentRoot:.2%}<extra></extra>")
            fig.update_layout(height=600, margin=dict(t=0,b=0,l=0,r=0), uniformtext=dict(minsize=10, mode='hide'))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Detailed info overall"):
                st.dataframe(df)
    else:
        dis_dict = district_list()
        selected_district = st.sidebar.selectbox("Choose District:", ['All'] + dis_dict[selected_state])
        if selected_district == 'All':
            st.markdown(f"<h4 style ='color: skyblue;'>{selected_state} - Overall Transaction Volume</h4>", unsafe_allow_html=True)
            with st.container(border=True):
                query = f"SELECT * FROM top_transaction_districtwise WHERE state='{selected_state}';"
                df = pd.read_sql(query, engine)

                fig = px.sunburst(df, path=['state', 'district', 'year', 'quarter'], values='transaction_count', color='transaction_count', color_continuous_scale='Plasma')        
                fig.update_traces(insidetextorientation='radial',
                                hovertemplate="Label= %{label}" \
                                                "<br>Id= %{id}" \
                                                "<br>Parent= %{percentParent:.2%}<br>"\
                                                "Root= %{percentRoot:.2%}<extra></extra>")
                fig.update_layout(height=600, margin=dict(t=0,b=0,l=0,r=0), uniformtext=dict(minsize=10, mode='hide'))
                st.plotly_chart(fig, use_container_width=True)
                with st.expander(f"Detailed info on {selected_state} Overall"):
                    st.dataframe(df)
        else:
            st.markdown(f"<h4 style ='color: skyblue;'>{selected_state} - {selected_district} Transaction Volume</h4>", unsafe_allow_html=True)
            with st.container(border=True):
                query = f"SELECT * FROM top_transaction_districtwise WHERE state='{selected_state}' and district='{selected_district}';"
                df = pd.read_sql(query, engine)

                fig = px.sunburst(df, path=['district', 'year', 'quarter'], values='transaction_count', color='transaction_count', color_continuous_scale='Plasma')        
                fig.update_traces(insidetextorientation='radial',
                                hovertemplate="Label= %{label}" \
                                                "<br>Id= %{id}" \
                                                "<br>Parent= %{percentParent:.2%}<br>"\
                                                "Root= %{percentRoot:.2%}<extra></extra>")
                fig.update_layout(height=600, margin=dict(t=0,b=0,l=0,r=0), uniformtext=dict(minsize=10, mode='hide'))
                st.plotly_chart(fig, use_container_width=True)
                with st.expander(f"Detailed info on {selected_state} - {selected_district}"):
                    st.dataframe(df)
def location_mode_analysis():
    st.markdown("<h3 style ='color: blue;'>Transaction Volume Analysis across States and Districts</h3>", unsafe_allow_html=True)
    st.markdown("\n")
    sub_page = st.sidebar.radio("Select Sub-Analysis:", ["Yearwise", "Overall"])
    if sub_page == "Yearwise":
        yearwise_analysis()
    elif sub_page == "Overall":
        overall_analysis()

def third_page():
    st.markdown("<h2 style='color: violet;'>PHONEPE TRANSACTION DATA INSIGHTS</h1>", unsafe_allow_html=True)

    sub_page = st.sidebar.radio("Choose Analysis Variants:", ["Volume vs Payment Mode Analysis", "Volume vs Location Mode Analysis"])
    if sub_page == "Volume vs Payment Mode Analysis":
        payment_mode_analysis()
    elif sub_page == "Volume vs Location Mode Analysis":
        location_mode_analysis()
    
# ------------------------------------------ INSURANCE PAGE -------------------------------------------------- #


def fourth_page():
    st.markdown("<h2 style='color: violet;'>PHONEPE INSURANCE DATA INSIGHTS</h1>", unsafe_allow_html=True)
    warnings.simplefilter(action='ignore', category=FutureWarning)
    st.markdown("<h3 style ='color: blue;'>Insurance Penetration and Growth Potential Analysis</h3>", unsafe_allow_html=True)
    st.markdown("\n")

    query = """SELECT state, latitude, longitude, metric FROM india_level_location_metrics;"""
    df = pd.read_sql(query, engine)
    with st.container(border=True):
        fig = px.scatter_mapbox(df,
            lat='latitude',
            lon='longitude',
            size='metric',
            color='metric',
            color_continuous_scale='Magma',
            hover_name='state',
            hover_data={'latitude': True, 'longitude': True, 'metric': True},
            zoom=4)

        fig.update_layout(mapbox_style="carto-positron",
            mapbox_center={"lat": df['latitude'].mean(), "lon": df['longitude'].mean()},
            margin={"r":0, "t":0, "l":0, "b":0}
        )
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Detailed Info On Insurance Metrics"):
            st.dataframe(df)

    st.markdown(f"<h4 style ='color: Skyblue;'>Statewise Proiritization</h4>", unsafe_allow_html=True)

    query1 = """CREATE TEMPORARY TABLE growth_rate AS
                SELECT state, 
                        SUM(CASE WHEN year=2024 THEN insurance_count ELSE 0 END) AS count_2024,
                        SUM(CASE WHEN year=2023 THEN insurance_count ELSE 0 END) AS count_2023,
                        ROUND(	(SUM(CASE WHEN year=2024 THEN insurance_count ELSE 0 END) - 
                                SUM(CASE WHEN year=2023 THEN insurance_count ELSE 0 END) ) * 100 /
                            NULLIF(SUM(CASE WHEN year=2023 THEN insurance_count ELSE 0 END), 0), 2) AS growth_percent 
                FROM aggregated_insurance GROUP BY state;"""
    query2 = """CREATE TEMPORARY TABLE volume AS
                SELECT state, SUM(insurance_count) as total_volume
                FROM map_insurance GROUP BY state;"""
    query = """	SELECT g.state, g.growth_percent, v.total_volume,
                        CASE
                            WHEN g.growth_percent <= 20 AND v.total_volume > 100000 THEN "Saturated"
                            WHEN g.growth_percent > 20 AND v.total_volume > 100000 THEN "Best"
                            WHEN g.growth_percent > 20 AND v.total_volume < 100000 THEN "Rising"
                            ELSE "Idle"
                        END AS state_category
                FROM growth_rate as g
                JOIN volume as v
                ON g.state=v.state;"""
    
    with engine.connect() as conn:
        conn.execute(text(query1))
        conn.execute(text(query2))
        df = pd.read_sql(query, conn)

    df['volume_f'] = df['total_volume'].apply(value_formats)
    with st.container(border=True):
        df['state_category'] = pd.Categorical(df['state_category'], categories=['Best', 'Saturated', 'Rising', 'Idle'], ordered=True)

        #col1, col2 = st.columns([0.5,0.5])
        #with col1:
            #st.markdown("BEST - High Growth & High Volume")
            #st.markdown("SATURATED - Low Growth & High Volume")
        #with col2:
            #st.markdown("RISING - High Growth & Low Volume")
            #st.markdown("IDLE - Low Growth & Low Volume")
        
        fig = px.treemap(df, path=[px.Constant("States by Category"), 'state_category', 'state'],
                        values=None,
                        color='state_category',
                        hover_data={'growth_percent': True, 'total_volume': True})

        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Detailed Info On State prioritization"):
            st.dataframe(df)


# ------------------------------------------- MAIN FUNCTION -------------------------------------------------- #

pages = {"HOME" : main_page,
         "USER" : second_page,
         "TRANSACTION" : third_page,
         "INSURANCE" : fourth_page}

st.set_page_config(layout="wide")

selected_page = st.sidebar.radio("Phonepe Pulse Insights", list(pages.keys()))

pages[selected_page]()
