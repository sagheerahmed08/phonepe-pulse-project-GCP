import json
import streamlit as st
import pandas as pd
import requests
import plotly.express as px
from streamlit_option_menu import option_menu
from google.cloud import storage
from io import BytesIO
import os
from google.oauth2 import service_account

def safe_groupby(df, group_cols, agg_dict):
    if df.empty or not all(col in df.columns for col in group_cols):
        return pd.DataFrame()
    return df.groupby(group_cols).agg(agg_dict).reset_index()
    
def plot_bar(df, x, y, title, color=None, color_scale="Rainbow", text=None, hover_data=None,barmode =None):
    if df.empty:
        st.warning(f"No data available for {title}.")
        return
    if x == "Pincodes":
        df[x] = df[x].astype(str)
    fig = px.bar(df, x=x, y=y, color=color or y, color_continuous_scale=color_scale,
                title=title, text=text, hover_data=hover_data)
    fig.update_layout(xaxis_tickangle=45, xaxis_type="category")
    st.plotly_chart(fig, use_container_width=True)

def plot_line(df, x, y, title, color=None, color_scale="Rainbow", text=None, hover_data=None, markers=True, line_dash=None):
    if df.empty:
        st.warning(f"⚠️ No data available for '{title}'.")
        return
    df = df.copy()
    if x in ["Pincodes", "Years"]:
        df[x] = df[x].astype(str)
    fig = px.line(
        df,
        x=x,
        y=y,
        color=color or y,
        line_dash=line_dash,
        markers=markers,
        text=text,
        hover_data=hover_data,
        title=title,
        color_discrete_sequence=px.colors.qualitative.Plotly if color is None else None,
    )
    fig.update_traces(textposition='top center')
    fig.update_layout(
        xaxis_title=x,
        yaxis_title=y,
        title={'x':0.5, 'xanchor':'center'},
        template="plotly_white",
        xaxis_tickangle=45,
        xaxis_type='category',
        hovermode="x unified"
    )
    st.plotly_chart(fig, use_container_width=True)


def plot_scatter(df, x, y, color, title, hover_data=None):
    if df.empty:
        st.warning(f"No data available for {title}.")
        return
    fig = px.scatter(df, x=x, y=y, color=color, title=title, hover_data=hover_data)
    st.plotly_chart(fig, use_container_width=True)

creds_json = st.secrets["GCP_SERVICE_ACCOUNT"]
creds_dict = json.loads(creds_json)
credentials = service_account.Credentials.from_service_account_info(creds_dict)
project_id = creds_dict["project_id"]

@st.cache_data(show_spinner=True)
def list_csv_files(bucket_name: str, prefix: str = ""):
    """List all CSV files in a GCP bucket under a given prefix."""
    client = storage.Client(project=project_id, credentials=credentials)
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    csv_files = [blob.name for blob in blobs if blob.name.endswith(".csv")]
    return csv_files

@st.cache_data(show_spinner=True)
def load_csvs_to_dataframes(bucket_name: str, prefix: str = ""):
    
    client = storage.Client(project=project_id, credentials=credentials)
    bucket = client.bucket(bucket_name)
    
    csv_files = list_csv_files(bucket_name, prefix)
    dataframes = {}
    
    for file_name in csv_files:
        blob = bucket.blob(file_name)
        data = blob.download_as_bytes()
        df = pd.read_csv(BytesIO(data))
        key = file_name.split("/")[-1] 
        dataframes[key] = df
    return dataframes


bucket_name = "phonepe-insight-transaction"
prefix = "output/" 

dataframes = load_csvs_to_dataframes(bucket_name, prefix)

df1 = dataframes["agg_insurance.csv"]

# Access an individual file DataFrame:Aggre_insurance = pd.DataFrame(table1,columns = ("States", "Years", "Quarter", "Transaction_type", "Transaction_count","Transaction_amount"))
df1 = dataframes["agg_insurance.csv"]
Aggre_insurance = pd.DataFrame(df1,columns = ("States", "Years", "Quarter", "Transaction_type", "Transaction_count","Transaction_amount"))
print(Aggre_insurance)


df2 = dataframes["agg_trans.csv"]
Aggre_transaction = pd.DataFrame(df2,columns = ("States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))
print(Aggre_transaction)

df3 = dataframes["agg_user.csv"]
Aggre_user = pd.DataFrame(df3, columns=("States", "Years", "Quarter", "Brand", "Transaction_count", "Transaction_Percentage"))
print(Aggre_user)

df4 = dataframes["map_insurance.csv"]
Map_insurance = pd.DataFrame(df4,columns = ("States", "Years", "Quarter", "District", "Transaction_count","Transaction_amount"))
print(Map_insurance)

df5 = dataframes["map_transaction.csv"]
Map_transaction = pd.DataFrame(df5,columns = ("States", "Years", "Quarter", "District", "Transaction_count", "Transaction_amount"))
print(Map_transaction)

df6 = dataframes["map_user.csv"]
Map_user = pd.DataFrame(df6,columns = ("States", "Years", "Quarter", "District", "RegisteredUser", "AppOpens"))
print(Map_user)

df7 = dataframes["top_insurance.csv"]
Top_insurance = pd.DataFrame(df7,columns = ("States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"))
Top_insurance["Pincodes"] = Top_insurance["Pincodes"].astype('object')
print(Top_insurance)

df8 = dataframes["top_transaction.csv"]
Top_transaction = pd.DataFrame(df8,columns = ("States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"))
Top_transaction["Pincodes"] = Top_transaction["Pincodes"].astype('object')
print(Top_transaction)

df9 = dataframes["top_user.csv"]
Top_user = pd.DataFrame(df9, columns = ("States", "Years", "Quarter", "Pincodes", "RegisteredUser"))
Top_user["Pincodes"] = Top_user["Pincodes"].astype('object')
print(Top_user)

df10 = dataframes["top_district.csv"]
Top_district = pd.DataFrame(df10, columns = ("States", "Years", "Quarter", "District", "Transaction_count","Transaction_amount"))
print(Top_district)

#QUERY AND FUNCTIONS FOR BUSINESS CASES
def plot_transaction_dynamics(df_transaction):
    st.write("Transaction Dynamics by each States, Year and Quarter")
    
    year_of_agg_transaction = df_transaction["Years"].unique()
    quarter_of_agg_transaction = df_transaction["Quarter"].unique()
    states_of_agg_transaction = df_transaction["States"].unique()
    sel_year = st.selectbox("Select Year", year_of_agg_transaction, key="year_select_plot_transaction_dynamics")
    sel_quarter = st.selectbox("Select Quarter", quarter_of_agg_transaction, key="quarter_select_plot_transaction_dynamics")
    sel_state = st.selectbox("Select State", states_of_agg_transaction, key="state_select_plot_transaction_dynamics")
   
    df = df_transaction[
        (df_transaction["States"] == sel_state) &
        (df_transaction["Years"] == sel_year) &
        (df_transaction["Quarter"] == sel_quarter)
    ]
   
    if df.empty:
        st.warning("No data available for the selected filters.")
        return
    
    fig = px.bar(df, x="Transaction_type", y="Transaction_amount",
                color="Transaction_type",
                title=f"Transaction Amount by Type in {sel_state} {sel_year}- Q{sel_quarter} ")
    
    return df, fig

def most_transaction(df_transaction):
    st.subheader("Transaction Amount in each Transaction Type by Year ")
    
    year_of_agg_transaction = df_transaction["Years"].unique()
    sel_year = st.selectbox("Select Year", year_of_agg_transaction, key="year_select_agg")
    transaction_type_of_agg_transaction = df_transaction["Transaction_type"].unique()
    sel_transaction_type = st.selectbox("Select Transaction Type", transaction_type_of_agg_transaction, key="transaction_type_select_agg")
   
    df1 = df_transaction[
        (df_transaction["Transaction_type"] == sel_transaction_type) &
        (df_transaction["Years"] == sel_year)       
    ].groupby("States").agg({
        "Transaction_amount": "sum",
        "Transaction_count": "sum"
    }).reset_index().sort_values("Transaction_amount", ascending=False, ignore_index=True)
    
    if df1.empty:
        st.warning("No data available for the selected filters.")
        return
    
    fig1 = px.bar(df1, x="States", y="Transaction_amount",
                 title=f"State wise {sel_transaction_type} Amount — {sel_year}")
    
    return df1, fig1

def Aggre_plot(df):
    """
    Plots Transaction Amount and Count by State
    Filters by year and/or quarter if provided.
    """
    st.write("********************************************************")
    st.subheader("Total Transaction Amount and Count by State")
    year_of_agg_transaction = df["Years"].unique()
    sel_year = st.selectbox("Select Year", year_of_agg_transaction, key="year_select_agg_trans_plot")  
    quarter_of_agg_transaction = df["Quarter"].unique()
    sel_quarter = st.selectbox("Select Quarter", quarter_of_agg_transaction, key="quarter_select_agg_trans_plot")
    
    if sel_year is not None and sel_quarter is not None:
        filtered_df = df[(df["Years"] == sel_year) & (df["Quarter"] == sel_quarter)]
        title_prefix = f"Year {sel_year} - Quarter {sel_quarter}"
    elif sel_year is not None:
        filtered_df = df[df["Years"] == sel_year]
        title_prefix = f"Year {sel_year}"
    elif sel_quarter is not None:
        filtered_df = df[df["Quarter"] == sel_quarter]
        title_prefix = f"Quarter {sel_quarter}"
    else:
        filtered_df = df.copy()
        title_prefix = "All Data"

    filtered_df.reset_index(drop=True, inplace=True)

    grouped = filtered_df.groupby("States")[["Transaction_count", "Transaction_amount"]].sum().reset_index()
    
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    geo_data = json.loads(requests.get(url).content)

    tab1, tab2 = st.tabs(["TRANSACTION AMOUNT", "TRANSACTION COUNT"])
    #Transaction Amount
    with tab1:
        st.subheader(f"{title_prefix} - TRANSACTION AMOUNT")
        st.bar_chart(grouped.set_index("States")["Transaction_amount"])
        st.write("********************************************************")
        fig_amount = px.choropleth(
            grouped,
            geojson=geo_data,
            locations="States",
            scope="asia",
            featureidkey="properties.ST_NM",
            color="Transaction_amount",
            color_continuous_scale="Sunsetdark",
            range_color=(grouped["Transaction_amount"].min(), grouped["Transaction_amount"].max()),
            hover_name="States",
            title=f"{title_prefix} - Transaction Amount",
            fitbounds="locations",
            width=800,
            height=600
        )
        fig_amount.update_geos(visible=False)
        st.plotly_chart(fig_amount,use_container_width=True)
        st.write("********************************************************")
    #Transaction Count
    with tab2:
        st.subheader(f"{title_prefix} - TRANSACTION COUNT")
        st.bar_chart(grouped.set_index("States")["Transaction_count"])
        
        fig_count = px.choropleth(
            grouped,
            geojson=geo_data,
            locations="States",
            featureidkey="properties.ST_NM",
            color="Transaction_count",
            color_continuous_scale="Sunsetdark",
            range_color=(grouped["Transaction_count"].min(), grouped["Transaction_count"].max()),
            hover_name="States",
            title=f"{title_prefix} - Transaction Count",
            fitbounds="locations",
            width=800,
            height=600
        )
        fig_count.update_geos(visible=False)
        st.plotly_chart(fig_count,use_container_width = True)
        st.write("********************************************************")

    return filtered_df

def plot_insurance_in_each_quarter(df_insurance):
    st.subheader("Insurance Transaction Amount by Quarter in each year")
    
    year_of_agg_insurance = df_insurance["Years"].unique()
    states_of_agg_insurance = df_insurance["States"].unique()
    sel_year_agg_insurance = st.selectbox("Select Year", year_of_agg_insurance, key="year_select_plot_transaction_dynamics")
    sel_state_agg_insurance = st.selectbox("Select State", states_of_agg_insurance, key="state_select_plot_transaction_dynamics")
    
    df = (
        df_insurance[
            (df_insurance["States"] == sel_state_agg_insurance) &
            (df_insurance["Years"] == sel_year_agg_insurance)
        ]
        .groupby("Quarter", as_index=False)
        .agg({
            "Transaction_amount": "sum",
            "Transaction_count": "sum"
        })
    )
    if df.empty:
        st.warning("No data available for the selected filters.")
        return
    fig = px.bar(df, x="Quarter", y="Transaction_amount", color="Transaction_count",
                 title=f"{sel_state_agg_insurance} - {sel_year_agg_insurance} Insurance Transaction Amount by Quarter")
    
    return df, fig

def user_brand_in_each_state(df_user):
    st.subheader("User Engagement by Brand in each year, quarter and states")
    
    Aggre_user_State = df_user["States"].unique()
    Aggre_user_Years = df_user["Years"].unique()
    Aggre_user_Quarter = df_user["Quarter"].unique()
    sel_year = st.selectbox("Select Year", Aggre_user_Years, key="year_select_agg_user")
    sel_quarter = st.selectbox("Select Quarter", Aggre_user_Quarter, key="quarter_select_agg_user")
    sel_state = st.selectbox("Select State", Aggre_user_State, key="state_select_agg_user")
    
    df = df_user[
        (df_user["States"] == sel_state) &
        (df_user["Years"] == sel_year) &
        (df_user["Quarter"] == sel_quarter)
    ]  
    
    if df.empty:
        st.warning("No data available for the selected filters.")
        return
    
    fig = px.bar(df, x="Brand", y="Transaction_Percentage",
                 title=f"User Engagement by Brand in {sel_state} - {sel_year} Q{sel_quarter} ",
                 color="Transaction_Percentage")
    
    return df, fig

def most_used_device_in_each_state_in_india_map(df_user):
    """
    Plots the most used device in each state on an India map.
    Filters by year and quarter.
    """
    st.subheader("Most Used Device in Each States in each Quarter and Year")
    
    Aggre_user_Years = df_user["Years"].unique()
    Aggre_user_Quarter = df_user["Quarter"].unique()
    sel_year = st.selectbox("Select Year", Aggre_user_Years, key="year_select_most_used_device")
    sel_quarter = st.selectbox("Select Quarter", Aggre_user_Quarter, key="quarter_select_most_used_device")
   
    df = df_user[(df_user["Years"] == sel_year) & (df_user["Quarter"] == sel_quarter)]
    
    if df.empty:
        st.warning("No data available for the selected filters.")
        return
    
    most_used = (
        df.sort_values(["States", "Transaction_Percentage"], ascending=[True, False])
        .groupby("States")
        .first()
        .reset_index()
    )
    
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    geo_data = json.loads(requests.get(url).content)
    col1, col2 = st.columns(2)
    with col1:
        tab1, tab2 = st.tabs(["Bar Charts", "Raw Data"])    
        with tab1:
            fig = px.choropleth(
                most_used,
                geojson=geo_data,
                locations="States",
                scope="asia",
                featureidkey="properties.ST_NM",
                color="Brand",
                hover_name="States",
                hover_data={"Transaction_Percentage": True, "Brand": True},
                title=f"Most Used Device in Each States - {sel_year} Q{sel_quarter}",
                fitbounds="locations",
                width=800,
                height=600
            )
            fig.update_geos(visible=False)
            st.plotly_chart(fig)
        with tab2:    
            st.dataframe(most_used,hide_index=True)
    with col2:    
        st.write("Pie Chart for Percentage")
        fig1 = px.pie(most_used,"Brand","Transaction_Percentage",title="Most Used Device in Each States by percentage")
        st.plotly_chart(fig1)
    return most_used
  
def map_bar_for_state_sum_for_each_quarter(df_transaction):
    """
    Shows bar charts of Transaction Count and Transaction Amount for a state,
    summed for each quarter in the given year.
    """
    states_of_map_transaction = df_transaction["States"].unique()
    sel_state = st.selectbox("Select State", states_of_map_transaction, key="state_select_map_transaction") 
    year_of_map_transaction = df_transaction["Years"].unique()
    sel_year = st.selectbox("Select Year", year_of_map_transaction, key="year_select_map_transaction")

    df = df_transaction[
        (df_transaction["Years"] == sel_year) &
        (df_transaction["States"] == sel_state)
    ]

    if df.empty:
        st.warning("No data available for the selected filters.")
        return

    df_summary = (
        df.groupby("Quarter", as_index=False)
        .agg({
            "Transaction_count": "sum",
            "Transaction_amount": "sum"
        })
        .sort_values("Quarter")
    )

    # Create tabs
    tab1, tab2 = st.tabs(["Transaction Count", "Transaction Amount"])
    #Transaction Count
    with tab1:
        fig_count = px.bar(
            df_summary,
            x="Quarter",
            y="Transaction_count",
            title=f"Transaction Count by Quarter in {sel_state} - {sel_year}",
            color="Transaction_count",
            color_continuous_scale="Sunsetdark",
            text_auto=True
        )
        st.plotly_chart(fig_count, use_container_width=True)

    #Transaction Amount
    with tab2:
        fig_amount = px.bar(
            df_summary,
            x="Quarter",
            y="Transaction_amount",
            title=f"Transaction Amount by Quarter in {sel_state} - {sel_year}",
            color="Transaction_amount",
            color_continuous_scale="Sunsetdark",
            text_auto=True
        )
        st.plotly_chart(fig_amount, use_container_width=True)

    return df_summary

def map_bar(df_transaction):
    """
    Plots district-wise Transaction Count and Amount for a specific state, year, and quarter.
    Uses bar charts instead of a district map.
    """
    states_of_map_transaction = df_transaction["States"].unique()
    sel_state = st.selectbox("Select State", states_of_map_transaction, key="state_select_map_transaction_map_bar1") 
    year_of_map_transaction = df_transaction["Years"].unique()
    sel_year = st.selectbox("Select Year", year_of_map_transaction, key="year_select_map_transaction_map_bar")
    quarter_of_map_transaction = df_transaction["Quarter"].unique()
    sel_quarter = st.selectbox("Select Quarter", quarter_of_map_transaction, key="quarter_select_map_transaction_map_bar2") 
    
    filtered_df = df_transaction[
        (df_transaction["Years"] == sel_year) &
        (df_transaction["Quarter"] == sel_quarter) &
        (df_transaction["States"] == sel_state)
    ]

    if filtered_df.empty:
        st.warning("No data available for the selected filters.")
        return

    filtered_df = filtered_df.sort_values(by="Transaction_count", ascending=False)
    tab1, tab2 = st.tabs(["Transaction Count", "Transaction Amount"])
    with tab1:
        fig_count = px.bar(
            filtered_df,
            x="District",
            y="Transaction_count",
            title=f"Transaction Count in {sel_state} - {sel_year} Q{sel_quarter}",
            color="Transaction_count",
            color_continuous_scale="Sunsetdark"
        )
        fig_count.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig_count, use_container_width=True)

    with tab2:
        fig_amount = px.bar(
            filtered_df,
            x="District",
            y="Transaction_amount",
            title=f"Transaction Amount in {sel_state} - {sel_year} Q{sel_quarter}",
            color="Transaction_amount",
            color_continuous_scale="Sunsetdark"
        )
        fig_amount.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig_amount, use_container_width=True)

    return filtered_df

def map_filter_by_state_and_district(df_map):
    """Filters insurance data by year, state, and district,
    then displays transaction count and amount by quarter.
    """
    states_of_map_transaction = df_map["States"].unique()
    sel_state = st.selectbox("Select State", states_of_map_transaction, key="state_map_filter_by_state_and_district") 
    districts_for_state = (Map_transaction[Map_transaction["States"] == sel_state]["District"].unique().tolist())
    sel_district = st.selectbox("Select District", districts_for_state, key="district_map_filter_by_state_and_district")
    year_of_map_transaction = df_map["Years"].unique()
    sel_year = st.selectbox("Select Year", year_of_map_transaction, key="year_map_filter_by_state_and_district")

    df = df_map[
        (df_map["Years"] == sel_year) &
        (df_map["States"] == sel_state) &
        (df_map["District"] == sel_district)
    ]

    if df.empty:
        st.warning("No data available for the selected filters.")
        return

    df_grouped = df.groupby("Quarter", as_index=False).agg({
        "Transaction_count": "sum",
        "Transaction_amount": "sum"
    })
    tab1, tab2 = st.tabs(["Bar Charts", "Raw Data"])

    with tab1:
        sub_tab1, sub_tab2 = st.tabs(["Transaction Count", "Transaction Amount"])
        # Transaction Count
        with sub_tab1:
            fig_count = px.bar(
                df_grouped,
                x="Quarter",
                y="Transaction_count",
                title=f"Insurance Transaction Count in {sel_district}, {sel_state} - {sel_year}",
                color="Transaction_count",
                color_continuous_scale="Sunsetdark",
                width=800,
                height=500,
                text_auto=True,
                labels={"Quarter": "Quarter", "Transaction_count": "Count of Transactions"},
                template="plotly_white",
            )
            st.plotly_chart(fig_count, use_container_width=True)
        # Transaction Amount
        with sub_tab2:
            fig_amount = px.bar(
                df_grouped,
                x="Quarter",
                y="Transaction_amount",
                title=f"Insurance Transaction Amount in {sel_district}, {sel_state} - {sel_year}",
                color="Transaction_amount",
                color_continuous_scale="Sunsetdark",
                width=800,
                height=500,
                text_auto=True,
                labels={"Quarter": "Quarter", "Transaction_amount": "Transaction Amount"},
                template="plotly_white",
            )
            st.plotly_chart(fig_amount, use_container_width=True)
    with tab2:
        st.dataframe(df_grouped, hide_index=True)   
        st.write("*****************************************************")

def map_user_total_registered_user_and_app_open(df_transaction):
    """Shows bar charts of Registered Users and App Opens for a state in a given year.
    """
    st.subheader("Registered Users and App Opens count in each district by State and Year wise")
    states_of_map_user = df_transaction["States"].unique()
    sel_state = st.selectbox("Select State", states_of_map_user, key="state_select_map_user")
    year_of_map_user = df_transaction["Years"].unique()
    sel_year = st.selectbox("Select Year", year_of_map_user, key="year_select_map_user")
   
    df = df_transaction[
        (df_transaction["Years"] == sel_year) &
        (df_transaction["States"] == sel_state)
     ]
    if df.empty:
        st.warning("No data available for the selected filters.")
        return
    df_summary = (
        df.groupby("District", as_index=False)
        .agg({
            "RegisteredUser": "sum",
            "AppOpens": "sum"
        })
    )

    # Create tabs
    tab1, tab2 = st.tabs(["Registered Users", "App Opens"])

    with tab1:
        fig_users = px.bar(
            df_summary,
            x="District",
            y="RegisteredUser",
            title=f"Registered Users in {sel_state} - {sel_year}",
            color="RegisteredUser",
            color_continuous_scale="Sunsetdark"
        )
        fig_users.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig_users, use_container_width=True)

    with tab2:
        fig_opens = px.bar(
            df_summary,
            x="District",
            y="AppOpens",
            title=f"App Opens in {sel_state} - {sel_year}",
            color="AppOpens",
            color_continuous_scale="Sunsetdark"
        )
        fig_opens.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig_opens, use_container_width=True)

    return df_summary

def map_use_registered_user_and_app_open(df_user):
    """
    Plots district-wise Registered Users and App Opens for a specific state, year, and quarter.
    Uses bar charts instead of a district map.
    """
    st.write("******************************************************")
    st.subheader("Registered User and App open count in each quarter ")
    states_of_map_user = df_user["States"].unique()
    sel_state = st.selectbox("Select State", states_of_map_user, key="state_map_use_registered_user_and_app_open")
    year_of_map_user = df_user["Years"].unique()
    sel_year = st.selectbox("Select Year", year_of_map_user, key="year_map_use_registered_user_and_app_open")
    quarter_of_map_user = df_user["Quarter"].unique()
    sel_quarter = st.selectbox("Select Quarter", quarter_of_map_user, key="quarter_map_use_registered_user_and_app_open")

    filtered_df = df_user[
        (df_user["Years"] == sel_year) &
        (df_user["Quarter"] == sel_quarter) &
        (df_user["States"] == sel_state)
    ]
    if filtered_df.empty:
        st.warning("No data available for the selected filters.")
        return
    filtered_df = filtered_df.sort_values(by="RegisteredUser", ascending=False)
    tab1, tab2 = st.tabs(["Registered Users", "App Opens"])
    with tab1:
        fig_users = px.bar(
            filtered_df,
            x="District",
            y="RegisteredUser",
            title=f"Registered Users in {sel_state} - {sel_year} Q{sel_quarter}",
            color="RegisteredUser",
            color_continuous_scale="Sunsetdark"
        )
        fig_users.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig_users, use_container_width=True)
    with tab2:
        fig_opens = px.bar(
            filtered_df,
            x="District",
            y="AppOpens",
            title=f"App Opens in {sel_state} - {sel_year} Q{sel_quarter}",
            color="AppOpens",
            color_continuous_scale="Sunsetdark"
        )
        fig_opens.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig_opens, use_container_width=True)

    return filtered_df

def map_user_filter_by_state_and_district(df_user):
    """Filters user data by year, state, and district,
    then displays registeduser and appopens by quarter."""
    st.write("******************************************************")
    st.subheader("Registered User and App open count in each quarter by year, state, and district wise ")
    year_of_map_user = df_user["Years"].unique()
    sel_year = st.selectbox("Select Year", year_of_map_user, key="year_map_user_filter_by_state_and_district")
    states_of_map_user = df_user["States"].unique()
    sel_state = st.selectbox("Select State", states_of_map_user, key="state_map_user_filter_by_state_and_district")
    districts_for_state = (Map_user[Map_user["States"] == sel_state]["District"].unique().tolist())
    sel_district = st.selectbox("Select District", districts_for_state, key="district_map_user_filter_by_state_and_district")
    df = df_user[
        (df_user["Years"] == sel_year) &
        (df_user["States"] == sel_state) &
        (df_user["District"] == sel_district)
    ]
    if df.empty:
        st.warning("No data available for the selected filters.")
        return
    df_grouped = df.groupby("Quarter", as_index=False).agg({
        "RegisteredUser": "sum",
        "AppOpens": "sum"
    })
    tab1, tab2 = st.tabs(["Bar Charts", "Raw Data"])
    with tab1:
        sub_tab1, sub_tab2 = st.tabs(["Registered Users", "App Opens"])
        with sub_tab1:
            fig_users = px.bar(
                df_grouped,
                x="Quarter",
                y="RegisteredUser",
                title=f"Registered Users in {sel_district}, {sel_state} - {sel_year}",
                color="RegisteredUser",
                color_continuous_scale="Sunsetdark",
                width=800,
                height=500,
                text_auto=True,
                labels={"Quarter": "Quarter", "RegisteredUser": "Count of Registered Users"},
                template="plotly_white",
            )
            st.plotly_chart(fig_users, use_container_width=True)
        with sub_tab2:
            fig_opens = px.bar(
                df_grouped,     
                x="Quarter",
                y="AppOpens",
                title=f"App Opens in {sel_district}, {sel_state} - {sel_year}",
                color="AppOpens",
                color_continuous_scale="Sunsetdark",
                width=800,
                height=500,
                text_auto=True,
                labels={"Quarter": "Quarter", "AppOpens": "Count of App Opens"},
                template="plotly_white",
            )
            st.plotly_chart(fig_opens, use_container_width=True)
    with tab2:
        st.dataframe(df_grouped, hide_index=True)
        st.markdown("### Data Summary")
        st.write(df_grouped.describe())
        
    return df_grouped

def Top_count_amount(df_top):
    """
    Shows bar charts of Transaction Count and Transaction Amount for a state,
    summed for each quarter in the given year.
    """

    st.header("Transation amount and count in each quarter by state, year wise")
    Top_transaction_States = df_top["States"].unique()
    Top_transaction_Years = df_top["Years"].unique()
    sel_state = st.selectbox("Select State", Top_transaction_States, key="state_select_top_transaction")
    sel_year = st.selectbox("Select Year", Top_transaction_Years, key="year_select_top_transaction")
    
    df = df_top[
        (df_top["Years"] == sel_year) &
        (df_top["States"] == sel_state) 
    ]

    if df.empty:
        st.warning("No data available for the selected filters.")
        return
    
    df_summary = (
        df.groupby("Quarter", as_index=False)
        .agg({
            "Transaction_count": "sum",
            "Transaction_amount": "sum"
        })
        .sort_values("Quarter")
    )
    tab1, tab2 = st.tabs(["Transaction Count", "Transaction Amount"])

    with tab1:
        fig_count = px.bar(
            df_summary,
            x="Quarter",
            y="Transaction_count",
            title=f"Transaction Count by Quarter wise in {sel_state} - {sel_year}",
            color="Transaction_count",
            color_continuous_scale="Sunsetdark",
            text_auto=True
        )
        st.plotly_chart(fig_count, use_container_width=True)

    with tab2:
        fig_amount = px.bar(
            df_summary,
            x="Quarter",
            y="Transaction_amount",
            title=f"Transaction Amount by Quarter wise in {sel_state} - {sel_year}",
            color="Transaction_amount",
            color_continuous_scale="Sunsetdark",
            text_auto=True
        )
        st.plotly_chart(fig_amount, use_container_width=True)
    return df_summary

def Top_pie(df_transaction):
    """
    Plots district-wise Transaction Count and Amount for a specific state, year, and quarter
    using Pie Charts.
    """
    st.write("********************************************************")
    st.header("Percentage of Transation amount and count in each Pincode by state, year and quarter  ")
    Top_transaction_Years = df_transaction["Years"].unique()
    Top_transaction_States = df_transaction["States"].unique()
    Top_transaction_Quarter = df_transaction["Quarter"].unique()
    sel_state = st.selectbox("Select State", Top_transaction_States, key="state_Top_pie")
    sel_year = st.selectbox("Select Year", Top_transaction_Years, key="year_Top_pie")
    sel_quarter = st.selectbox("Select Quarter", Top_transaction_Quarter, key="quarter_Top_pie")
    # Filter data
    filtered_df = df_transaction[
        (df_transaction["Years"] == sel_year) &
        (df_transaction["Quarter"] == sel_quarter) &
        (df_transaction["States"] == sel_state)
    ]

    if filtered_df.empty:
        st.warning("No data available for the selected filters.")
        return
    filtered_df["Pincodes"] = filtered_df["Pincodes"].astype(str)
    filtered_df = filtered_df.sort_values(by="Transaction_count", ascending=False)

    tab1, tab2 = st.tabs(["Transaction Count", "Transaction Amount"])
    with tab1:
        fig_count = px.pie(
            filtered_df,
            names="Pincodes",
            values="Transaction_count",
            title=f"Transaction Count in {sel_state} - {sel_year} Q{sel_quarter}",
            hole=0.4,  # Makes donut chart
            color_discrete_sequence=px.colors.sequential.Sunsetdark
        )
        fig_count.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_count, use_container_width=True)

    with tab2:
        fig_amount = px.pie(
            filtered_df,
            names="Pincodes",
            values="Transaction_amount",
            title=f"Transaction Amount in {sel_state} - {sel_year} Q{sel_quarter}",
            hole=0.4,
            color_discrete_sequence=px.colors.sequential.Sunsetdark
        )
        fig_amount.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_amount, use_container_width=True)

    return filtered_df

def Top_filter_by_state_and_pincode(df_user):
    """Filters user data by year, state, and Pincodes,
    then displays Transaction count and Transaction Amount."""
    st.write("********************************************************")
    st.header("Transation amount and count in each quater by state, year and pincode")
    Top_transaction_Years = df_user["Years"].unique()
    Top_transaction_States = df_user["States"].unique()
    sel_state = st.selectbox("Select State", Top_transaction_States, key="state_Top_filter_by_state_and_pincode")
    sel_year = st.selectbox("Select Year", Top_transaction_Years, key="year_Top_filter_by_state_and_pincode")
    pincode_for_state = df_user[df_user["States"] == sel_state]["Pincodes"].unique().tolist()
    sel_pincode = st.selectbox("Select Pincode", pincode_for_state, key="Pincodes_Top_filter_by_state_and_pincode")
    df = df_user[
        (df_user["Years"] == sel_year) &
        (df_user["States"] == sel_state) &
        (df_user["Pincodes"] == sel_pincode)
    ]
    if df.empty:
        st.warning("No data available for the selected filters.")
        return

    df_grouped = df.groupby("Quarter", as_index=False).agg({
        "Transaction_count": "sum",
        "Transaction_amount": "sum"
    })

    tab1, tab2 = st.tabs(["Bar Charts", "Raw Data"])
    with tab1:
        sub_tab1, sub_tab2 = st.tabs(["Transaction count", "Transaction amount"])

        with sub_tab1:
            fig_users = px.bar(
                df_grouped,
                x="Quarter",
                y="Transaction_count",
                title=f"Transaction count in {sel_pincode}, {sel_state} - {sel_year}",
                color="Transaction_count",
                color_continuous_scale="Sunsetdark",
                width=800,
                height=500,
                text_auto=True,
                labels={"Quarter": "Quarter", "Transaction_count": "Count of Transaction"},
                template="plotly_white",
            )
            st.plotly_chart(fig_users, use_container_width=True)

        with sub_tab2:
            fig_opens = px.bar(
                df_grouped,     
                x="Quarter",
                y="Transaction_amount",
                title=f"Transaction amount in {sel_pincode}, {sel_state} - {sel_year}",
                color="Transaction_amount",
                color_continuous_scale="Sunsetdark",
                width=800,
                height=500,
                text_auto=True,
                labels={"Quarter": "Quarter", "Transaction_amount": "Transaction amount"},
                template="plotly_white",
            )
            st.plotly_chart(fig_opens, use_container_width=True)
    with tab2:
        st.dataframe(df_grouped, hide_index=True)
        st.markdown("### Data Summary")
        st.write(df_grouped.describe())
        
    return df_grouped

def Top_register_user(df_top):
    """
    Shows bar charts of register_user for a state,
    summed for each quarter in the given year.
    """
    st.subheader("Registered User count in each quarter in State, Year wise")
    Top_user_States = Top_user["States"].unique()
    Top_user_Years = Top_user["Years"].unique()
    sel_year = st.selectbox("Select Year", Top_user_Years, key="year_select_Top_user")
    sel_state = st.selectbox("Select State", Top_user_States, key="state_select_Top_user")
    df = df_top[
        (df_top["Years"] == sel_year) &
        (df_top["States"] == sel_state) 
    ]

    if df.empty:
        st.warning("No data available for the selected filters.")
        return
    
    df_summary = (
        df.groupby("Quarter", as_index=False)
        .agg({
            "RegisteredUser": "sum",
        })
        .sort_values("Quarter")
    )   

    fig_count = px.bar(
        df_summary,
        x="Quarter",
        y="RegisteredUser",
        title=f"RegisteredUser by Quarter wise in {sel_state} - {sel_year}",
        color="RegisteredUser",
        color_continuous_scale="Sunsetdark",
        text_auto=True
        )
    st.plotly_chart(fig_count, use_container_width=True)

    return df_summary

def Top_use_pie(df_transaction):
    """
    Plots district-wise Transaction Count and Amount for a specific state, year, and quarter
    using Pie Charts.
    """
    st.write("********************************************************")
    st.subheader("Registered User count in each Pincode in State, Year and quarter wise")
    Top_user_States = Top_user["States"].unique()
    Top_user_Years = Top_user["Years"].unique()
    Top_user_Quarter = Top_user["Quarter"].unique()
    sel_year = st.selectbox("Select Year", Top_user_Years, key="year_Top_use_pie")
    sel_state = st.selectbox("Select State", Top_user_States, key="state_Top_use_pie")
    sel_quarter = st.selectbox("Select Quarter", Top_user_Quarter, key="quarter_Top_use_pie")
    filtered_df = df_transaction[
        (df_transaction["Years"] == sel_year) &
        (df_transaction["Quarter"] == sel_quarter) &
        (df_transaction["States"] == sel_state)
    ]

    if filtered_df.empty:
        st.warning("No data available for the selected filters.")
        return

    filtered_df["Pincodes"] = filtered_df["Pincodes"].astype(str)
    filtered_df = filtered_df.sort_values(by="RegisteredUser", ascending=False)
    fig_count = px.pie(
        filtered_df,
        names="Pincodes",
        values="RegisteredUser",
        title=f"RegisteredUser in {sel_state} - {sel_year} Q{sel_quarter}",
        hole=0.4,  # Makes it a donut chart
        color_discrete_sequence=px.colors.sequential.Sunsetdark
    )
    fig_count.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig_count, use_container_width=True)

    return filtered_df

def Top_Registered_by_state_and_pincode(df_user):
    """Filters user data by year, state, and Pincodes,
    then displays Registered Users."""
    
    st.write("********************************************************")
    st.subheader("Registered User count in each quarter in State, Year and Pincode wise")
    Top_user_States = Top_user["States"].unique()
    Top_user_Years = Top_user["Years"].unique()
    sel_year = st.selectbox("Select Year", Top_user_Years, key="year_Top_Registered_by_state_and_pincode")
    sel_state = st.selectbox("Select State", Top_user_States, key="state_Top_Registered_by_state_and_pincode")
    pincode_for_state = Top_user[Top_user["States"] == sel_state]["Pincodes"].unique().tolist()
    sel_pincode = st.selectbox("Select Pincode", pincode_for_state, key="Pincodes_select_Top_user")
    df = df_user[
        (df_user["Years"] == sel_year) &
        (df_user["States"] == sel_state) &
        (df_user["Pincodes"] == sel_pincode)
    ]   
    if df.empty:
        st.warning("No data available for the selected filters.")
        return
    df_grouped = df.groupby("Quarter", as_index=False).agg({
        "RegisteredUser": "sum",
    })
    tab1, tab2 = st.tabs(["Bar Charts", "Raw Data"])
    with tab1:
        fig_users = px.bar(
            df_grouped,
            x="Quarter",
            y="RegisteredUser",
            title=f"RegisteredUser in Pincode - {sel_pincode}, {sel_state} - {sel_year}",
            color="RegisteredUser",
            color_continuous_scale="Sunsetdark",
            width=800,
            height=500,
            text_auto=True,
            labels={"Quarter": "Quarter", "RegisteredUser": "Count of RegisteredUser"},
            template="plotly_white",
        )
        st.plotly_chart(fig_users, use_container_width=True)
    with tab2:
        st.dataframe(df_grouped, hide_index=True)
        st.markdown("### Data Summary")
        st.write(df_grouped.describe())
    return df_grouped

def ques1(df):
    st.write("*****************************************************************************************************")
    st.header("Transaction Type Trends Analysis")
    st.subheader("Most Used Transaction Type by State")
    year_list_map = ["All Years"] + sorted(df["Years"].unique().tolist())
    quarter_list_map = ["All Quarters"] + sorted(df["Quarter"].unique().tolist())
    sel_year_map = st.selectbox("Select Year (Map View)", year_list_map, key="map_year")
    sel_quarter_map = st.selectbox("Select Quarter (Map View)", quarter_list_map, key="map_quarter")

    map_df = df.copy() 
    if sel_year_map != "All Years":
        map_df = map_df[map_df["Years"] == int(sel_year_map)]
    if sel_quarter_map != "All Quarters":
        map_df = map_df[map_df["Quarter"] == int(sel_quarter_map)]
        
    most_used = (
        map_df.groupby(["States", "Transaction_type"], as_index=False)["Transaction_amount"].sum()
        .sort_values(["States", "Transaction_amount"], ascending=[True, False])
        .groupby("States").first().reset_index()
    )
    totalamount_per_state = map_df.groupby("States", as_index=False)["Transaction_amount"].sum()
    totalcount_per_state = map_df.groupby("States", as_index=False)["Transaction_count"].sum()
    
    most_used1 = pd.merge(totalamount_per_state,totalcount_per_state,on="States")
    total_transaction = most_used1["Transaction_amount"].sum()
    total_count = most_used1["Transaction_count"].sum()
    most_used1["Count_Percentage"] = (most_used1["Transaction_count"]/total_count) * 100
    most_used1["Transaction_Percentage"] = (most_used1["Transaction_amount"]/total_transaction) * 100
    most_used1["Transaction_type"] = most_used["Transaction_type"]

    # Choropleth Map
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    geo_data = json.loads(requests.get(url).content)
    col1,col2 = st.columns(2)
    with col1:
        fig_map = px.choropleth(
            most_used1,
            geojson=geo_data,
            locations="States",
            scope="asia",
            featureidkey="properties.ST_NM",
            color="Transaction_Percentage",
            hover_name="States",
            hover_data={
                "Transaction_amount": True,
                "Transaction_type": True,
                "Transaction_Percentage": True,
            },
            title=f"Most Used Transaction Type by Transaction Amount - {sel_year_map if sel_year_map!='All Years' else 'Overall'} {('Q'+str(sel_quarter_map)) if sel_quarter_map!='All Quarters' else ''}",
            fitbounds="locations",
            width=800,
            height=600
        )
        fig_map.update_geos(visible=False)
        st.plotly_chart(fig_map, use_container_width=True)
    
    with col2:
        fig_map1 = px.choropleth(
            most_used1,
            geojson=geo_data,
            locations="States",
            scope="asia",
            featureidkey="properties.ST_NM",
            color="Count_Percentage",
            hover_name="States",
            hover_data={
                "Transaction_count": True,
                "Transaction_type": True,
                "Count_Percentage": True,
            },
            title=f"Most Used Transaction Type by Transaction Count - {sel_year_map if sel_year_map!='All Years' else 'Overall'} {('Q'+str(sel_quarter_map)) if sel_quarter_map!='All Quarters' else ''}",
            fitbounds="locations",
            width=800,
            height=600
        )
        fig_map1.update_geos(visible=False)
        st.plotly_chart(fig_map1, use_container_width=True)
    
    # State-wise Trend
    st.subheader("State-wise Transaction Trends")
    state_list = ["All States"] + sorted(df["States"].unique().tolist())
    sel_state_sw = st.selectbox("Select State (State-wise View)", state_list, key="sw_state")
    df_sw = df.copy()
    if sel_state_sw != "All States":
        df_sw = df_sw[df_sw["States"] == sel_state_sw]

    state_data = df_sw.groupby(["States", "Transaction_type"], as_index=False)["Transaction_amount"].sum()
    fig_state = px.bar(state_data, x="States", y="Transaction_amount", color="Transaction_type", barmode="group")
    st.plotly_chart(fig_state, use_container_width=True)
    
    # Year-wise Trends
    st.subheader("Year-wise Transaction Trends")
    year_list = ["All Years"] + sorted(df["Years"].unique().tolist())
    sel_year_yw = st.selectbox("Select Year (Year-wise View)", year_list, key="yw_year")
    df_yw = df.copy()
    if sel_year_yw != "All Years":
        df_yw = df_yw[df_yw["Years"] == int(sel_year_yw)]

    year_data = df_yw.groupby(["Years", "Transaction_type"], as_index=False)["Transaction_amount"].sum()
    fig_year = px.line(year_data, x="Years", y="Transaction_amount", color="Transaction_type", markers=True)
    st.plotly_chart(fig_year, use_container_width=True)
    
    # Quarter wise Trends
    st.subheader("Quarter-wise Transaction Trends")
    year_list_qw = sorted(df["Years"].unique().tolist())
    sel_year_qw = st.selectbox("Select Year (Quarter-wise View)", year_list_qw, key="qw_year")
    df_qw = df[df["Years"] == int(sel_year_qw)]
    quarter_data = df_qw.groupby(["Quarter", "Transaction_type"], as_index=False)["Transaction_amount"].sum()
    fig_quarter = px.bar(quarter_data, x="Quarter", y="Transaction_amount", color="Transaction_type", barmode="group")
    st.plotly_chart(fig_quarter, use_container_width=True)


    # Transaction Type Distribution
    st.subheader("Transaction Type Distribution")
    year_list_tw = ["All Years"] + sorted(df["Years"].unique().tolist())
    quarter_list_tw = ["All Quarters"] + sorted(df["Quarter"].unique().tolist())
    sel_year_tw = st.selectbox("Select Year (Type-wise View)", year_list_tw, key="tw_year")
    sel_quarter_tw = st.selectbox("Select Quarter (Type-wise View)", quarter_list_tw, key="tw_quarter")
    df_tw = df.copy()
    if sel_year_tw != "All Years":
        df_tw = df_tw[df_tw["Years"] == int(sel_year_tw)]
    if sel_quarter_tw != "All Quarters":
        df_tw = df_tw[df_tw["Quarter"] == int(sel_quarter_tw)]

    type_data = df_tw.groupby("Transaction_type", as_index=False)["Transaction_amount"].sum()
    fig_type = px.pie(type_data, names="Transaction_type", values="Transaction_amount", hole=0.4)
    st.plotly_chart(fig_type, use_container_width=True)

    # Top/Bottom 5 States by Transaction Amount
    sort_most_used = most_used1[["States", "Transaction_amount", "Transaction_Percentage"]].sort_values(
        by="Transaction_amount", ascending=False
    )
    sort_most_used1 = most_used1[["States", "Transaction_count", "Count_Percentage"]].sort_values(
        by="Transaction_count", ascending=False
    )

    choice = st.selectbox(
        "Select View",
        ["Top 5 States based on Transaction Amount and Transaction Count", "Bottom 5 States based on Transaction Amount and and Transaction Count"],
        index=0
    )

    tab_chart, tab_data = st.tabs(["Bar Chart", "Raw Data"])

    if choice == "Top 5 States based on Transaction Amount and Transaction Count":
        amount_df = sort_most_used.head(5)
        count_df = sort_most_used1.head(5)
        
    else:
        amount_df = sort_most_used.tail(5)
        count_df = sort_most_used1.tail(5)

    if choice == "Top 5 States based on Transaction Amount and Transaction Count":
        chartname_amount = "Top 5 States based on Transaction Amount"
        chartname_count = "Top 5 States based on Transaction Count"
    else:
        chartname_amount = "Bottom 5 States based on Transaction Amount"
        chartname_count = "Bottom 5 States based on Transaction Count"
    with tab_chart:
        col1,col2 = st.columns(2)
        with col1:
            fig_filtered = px.bar(
                amount_df,
                x="States",
                y="Transaction_amount",
                title=chartname_amount,
                color="Transaction_amount",
                color_continuous_scale="Rainbow",
                hover_data={
                    "Transaction_amount": ":,.0f",
                    "Transaction_Percentage": True,
                    "States": False
                }
            )
            fig_filtered.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig_filtered, use_container_width=True)
        
        with col2:
            fig_count = px.bar(
                count_df,
                x="States",
                y="Transaction_count",
                title=chartname_count,
                color="Transaction_count",
                color_continuous_scale="Rainbow",
                hover_data={
                    "Transaction_count": ":,.0f",
                    "Count_Percentage": True,
                    "States": False
                }
            )
            fig_count.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig_count, use_container_width=True)

    with tab_data:
        col1,col2 = st.columns(2)
        with col1:
            st.dataframe(amount_df, hide_index=True, use_container_width=True)
        with col2:    
            st.dataframe(count_df, hide_index=True, use_container_width=True)

def ques2(Aggre_user, Map_user):
    st.header("Device Dominance and User Engagement Analysis")
    st.subheader("Device Brand Engagement Across States")
    years = ["All"] + sorted(Aggre_user["Years"].unique().tolist())
    selected_years = st.selectbox("Select Year", years, key="brand_year1")

    quarters = ["All"] + sorted(Aggre_user["Quarter"].unique().tolist())
    selected_quarters = st.selectbox("Select Quarter", quarters, key="brand_quarter1")
    def filter_data(df, selected_years, selected_quarters):
        if selected_years != "All":
            df = df[df["Years"] == int(selected_years)]
        if selected_quarters != "All":
            df = df[df["Quarter"] == selected_quarters]   
        return df

    agg_user_filtered = filter_data(Aggre_user, selected_years, selected_quarters)
    brand_state = agg_user_filtered.groupby(["States", "Brand"]).agg({
        "Transaction_count": "sum",
        "Transaction_Percentage": "mean"
    }).reset_index()

    # Engagement Score = Transaction_count × Transaction_Percentage
    brand_state["Engagement_Score"] = (
        brand_state["Transaction_count"] * brand_state["Transaction_Percentage"]
    )
    state_filter = brand_state.groupby("States").agg({
        "Transaction_count": "sum",
        "Transaction_Percentage": "mean"
    }).reset_index()
    state_filter["Engagement_Score"] = (
        state_filter["Transaction_count"] * state_filter["Transaction_Percentage"]
    )
    best_brand = brand_state.loc[
    brand_state.groupby("States")["Engagement_Score"].idxmax()
    ].reset_index(drop=True)
    st.write("Engagement score = Transaction_count * Transaction_Percentage")
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    geo_data = json.loads(requests.get(url).content)
    col1,col2 = st.columns(2)
    with col1:
        fig1 = px.choropleth(
            state_filter ,
            geojson=geo_data, 
            locations="States",
            scope="asia",
            featureidkey="properties.ST_NM",
            color="Engagement_Score",
            hover_data=["Transaction_count","Transaction_Percentage"],
            title=f"Engagement_Score in each States {selected_years} Y and {selected_quarters} Q ",
            fitbounds="locations",
            width=800,
            height=600)
        fig1.update_geos(visible=False)
        st.plotly_chart(fig1, use_container_width=True)
    with col2:
        fig2 = px.choropleth(
            best_brand,
            geojson=geo_data, 
            locations="States",
            scope="asia",
            featureidkey="properties.ST_NM",
            color="Brand",
            hover_data=["Engagement_Score","Brand","Transaction_count"],
            title=f"Best brand in each states ( based on Engagement Score ) in {selected_years} Y and {selected_quarters} Q ",
            fitbounds="locations",
            width=800,
            height=600)
        fig2.update_geos(visible=False)
        st.plotly_chart(fig2, use_container_width=True)
    st.write("*********************************************************")    
    
    # ---------- Device Popularity ----------
    st.subheader("Device Brands by Transaction Count and Transaction Percentage")

    States = ["All"] + sorted(Aggre_user["States"].unique().tolist())
    selected_States= st.selectbox("Select States", States, key="brand_States")
   
    years = ["All"] + sorted(Aggre_user["Years"].unique().tolist())
    selected_year = st.selectbox("Select Year", years, key="brand_year")

    quarters = ["All"] + sorted(Aggre_user["Quarter"].unique().tolist())
    selected_quarter = st.selectbox("Select Quarter", quarters, key="brand_quarter")
    
    filtered_data = Aggre_user.copy()
    if selected_year != "All":
        filtered_data = filtered_data[filtered_data["Years"] == selected_year]
    if selected_quarter != "All":
        filtered_data = filtered_data[filtered_data["Quarter"] == selected_quarter]
    if selected_States != "All":
        filtered_data =  filtered_data[filtered_data["States"] == selected_States]
    
    
    if filtered_data.empty:
        st.warning("⚠️ No data available.")
        
    brand_users = (
        filtered_data.groupby("Brand")["Transaction_count"].sum()
        .reset_index()
        .sort_values(by="Transaction_count", ascending=False)
    )
    top5_users = brand_users.nlargest(5, "Transaction_count")
    bottom5_users= brand_users.nsmallest(5, "Transaction_count")
    
    brand_users1 = (
        filtered_data.groupby("Brand")["Transaction_Percentage"].sum().round(2)
        .reset_index()
        .sort_values(by="Transaction_Percentage", ascending=False)
    )
    top5_users1 = brand_users1.nlargest(5, "Transaction_Percentage")
    bottom5_users1= brand_users1.nsmallest(5, "Transaction_Percentage")
    
    tab1,tab2 = st.tabs(["Transaction count","Transaction Percentage"])
    with tab1:
        col1,col2 = st.columns(2)
        with col1:
        # Bar chart
            fig1 = px.bar(
                brand_users,
                x="Brand",
                y="Transaction_count",
                title=f"Device Brand by Transaction count  ({selected_year}, {selected_quarter})",
                text="Transaction_count"
            )
            fig1.update_traces(textposition="outside")
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            choice= st.selectbox("Select Top or Bottom",["Top 5 Device Brand by Transaction count","Bottom 5 Device Brand by Transaction count"])
            if choice == "Top 5 Device Brand by Transaction count":
                fig1= px.bar(
                top5_users,
                x="Brand",
                y="Transaction_count",
                title=f"Top 5 Transaction count by Device Brand ({selected_year}, {selected_quarter})",
                text="Transaction_count"
                )
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)
            elif choice == "Bottom 5 Device Brand by Transaction count":
                fig2= px.bar(
                bottom5_users,
                x="Brand",
                y="Transaction_count",
                title=f"Bottom 5 Transaction count by Device Brand ({selected_year}, {selected_quarter})",
                text="Transaction_count"
                )
                fig2.update_traces(textposition="outside")
                st.plotly_chart(fig2, use_container_width=True)
    with tab2:
        col1,col2 = st.columns(2)        
        with col1:
            fig1 = px.bar(
                brand_users1,
                x="Brand",
                y="Transaction_Percentage",
                title=f"Device Brand by Transaction Percentage ({selected_year}, {selected_quarter})",
                text="Transaction_Percentage"
            )
            fig1.update_traces(textposition="outside")
            st.plotly_chart(fig1, use_container_width=True)
        with col2:
            choice= st.selectbox("Select Top or Bottom",["Top 5 Device Brand by Transaction Percentage","Bottom 5 Device Brand by Transaction Percentage"],key="Transaction_Percentage")
            if choice == "Top 5 Device Brand by Transaction Percentage":
                fig1= px.bar(
                top5_users1,
                x="Brand",
                y="Transaction_Percentage",
                title=f"Top 5 Transaction count by Device Brand ({selected_year}, {selected_quarter})",
                text="Transaction_Percentage"
                )
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)
            elif choice == "Bottom 5 Device Brand by Transaction Percentage":
                fig2= px.bar(
                bottom5_users1,
                x="Brand",
                y="Transaction_Percentage",
                title=f"Bottom 5 Transaction Percentage by Device Brand ({selected_year}, {selected_quarter})",
                text="Transaction_Percentage"
                )
                fig2.update_traces(textposition="outside")
                st.plotly_chart(fig2, use_container_width=True)

    # Device Trend Over Time
    st.subheader("Device Brand Trend Over Time")
    col1,col2 = st.columns(2)
    with col1:
        brand_trend = (
            Aggre_user.groupby(["Years", "Quarter", "Brand"])["Transaction_count"].sum()
            .reset_index()
        )
        fig3 = px.line(
            brand_trend,
            x="Years",
            y="Transaction_count",
            color="Brand",
            line_group="Quarter",
            title="Device Brand Registered Users Over Time",
            markers=True
        )
        st.plotly_chart(fig3, use_container_width=True)

    # Device Trend Over Time for each Brand 
    with col2: 
        brands = sorted(Aggre_user["Brand"].unique().tolist()) 
        selected_brand = st.selectbox("Select Mobile Brand", brands, key="trend_brand") 
        Aggre_user["Years"] = Aggre_user["Years"].astype(str) 
        
        brand_trend = ( Aggre_user[Aggre_user["Brand"] == selected_brand].groupby(["Years", "Quarter"])["Transaction_count"].sum() .reset_index() ) 
        plot_line(brand_trend, "Years", "Transaction_count", f"Registered Users Trend for {selected_brand}", color="Quarter")
    
    # Engagement by Brand
    st.subheader("Device Brand Engagement Comparison")
    brand_state = Aggre_user.groupby("Brand")["Transaction_count"].sum().reset_index()
    brand_state["Engagement_Score"] = (brand_state["Transaction_count"] * (Aggre_user.groupby("Brand")["Transaction_Percentage"].mean().values) )

    fig5 = px.scatter(
        brand_state,
        x="Transaction_count",
        y="Engagement_Score",
        size="Engagement_Score",
        color="Brand",
        title="Device Dominance vs Engagement",
        hover_data=["Brand"]
    )
    st.plotly_chart(fig5, use_container_width=True)
    
    # User in each states
    st.subheader(" States by Registered Users / App Open ")
    years = ["All"] + sorted(Map_user["Years"].unique().tolist())
    selected_year = st.selectbox("Select Year", years, key="user_year")

    quarters = ["All"] + sorted(Map_user["Quarter"].unique().tolist())
    selected_quarter = st.selectbox("Select Quarter", quarters, key="user_quarter")
    filtered_data = Map_user.copy()
    if selected_year != "All":
        filtered_data = filtered_data[filtered_data["Years"] == selected_year]
    if selected_quarter != "All":
        filtered_data = filtered_data[filtered_data["Quarter"] == selected_quarter]
    
    
    if filtered_data.empty:
        st.warning("⚠️ No data available.")
        
    Registered_users = (
        filtered_data.groupby("States")["RegisteredUser"].sum()
        .reset_index()
        .sort_values(by="RegisteredUser", ascending=False)
    )
    App_open = (
        filtered_data.groupby("States")["AppOpens"].sum()
        .reset_index()
        .sort_values(by="AppOpens", ascending=False)
    )
    
    tab1,tab2,tab3 = st.tabs(["Registered User","App opens","User Engagement"])
    with tab1:
        col1,col2 = st.columns(2)
        with col1:
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            geo_data = json.loads(requests.get(url).content)
            fig4 = px.choropleth(
                Registered_users,
                geojson=geo_data, 
                locations="States",
                scope="asia",
                featureidkey="properties.ST_NM",
                color="RegisteredUser",
                hover_data=["RegisteredUser"],
                title=f"Registered Users in each states {selected_year} Y and {selected_quarter} Q",
                fitbounds="locations",
                width=1000,
                height=600)
            fig4.update_geos(visible=False)
            st.plotly_chart(fig4, use_container_width=True)
        with col2:    
            fig2 = px.bar(
                Registered_users,
                x="States",
                y="RegisteredUser",
                title=f"Registered User in {selected_year} Y, {selected_quarter} Q",
                text="RegisteredUser"
            )
            fig2.update_traces(textposition="outside")
            st.plotly_chart(fig2, use_container_width=True)
        
    with tab2:
        col1,col2 = st.columns(2)
        with col1:
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            geo_data = json.loads(requests.get(url).content)
            
            fig4 = px.choropleth(
                App_open,
                geojson=geo_data, 
                locations="States",
                scope="asia",
                featureidkey="properties.ST_NM",
                color="AppOpens",
                hover_data=["AppOpens"],
                title=f"App Opens in each states in {selected_year} Y and {selected_quarter} Q",
                fitbounds="locations",
                width=1000,
                height=600)
            fig4.update_geos(visible=False)
            st.plotly_chart(fig4, use_container_width=True)
        with col2:
            fig2 = px.bar(
                App_open,
                x="States",
                y="AppOpens",
                title=f"App Opens in {selected_year} Y, {selected_quarter} Q",
                text="AppOpens"
            )
            fig2.update_traces(textposition="outside")
            st.plotly_chart(fig2, use_container_width=True)

    with tab3:
        
        #User Engagement 
        st.subheader("User Engagement (AppOpens per Registered User)")
        map_user_group = (
            filtered_data.groupby("States")[["RegisteredUser", "AppOpens"]].sum()
            .reset_index()
        )
        map_user_group["Engagement_Ratio"] = (
            map_user_group["AppOpens"] / map_user_group["RegisteredUser"]
        )
        
        col1,col2 = st.columns(2)
        with col1:
            fig2 = px.choropleth(
                map_user_group,
                geojson=geo_data, 
                locations="States",
                scope="asia",
                featureidkey="properties.ST_NM",
                color="Engagement_Ratio",
                hover_data=["RegisteredUser", "AppOpens"],
                title=f"User Engagement in each states  {selected_year} Y and {selected_quarter} Q ",
                fitbounds="locations",
                width=800,
                height=600)
            fig2.update_geos(visible=False)
            
            st.plotly_chart(fig2, use_container_width=True)
        with col2:
            fig2 = px.bar(
                    map_user_group,
                    x="States",
                    y="Engagement_Ratio",
                    title=f"User Engagement in each state {selected_year} Y and {selected_quarter} Q",
                    text="Engagement_Ratio"
                )
            fig2.update_traces(textposition="inside")
            st.plotly_chart(fig2, use_container_width=True) 
    
def ques3(df_agg, df_map, df_top, Top_user, Map_user):
    st.write("*****************************************************************************************************")
    st.header("Insurance Penetration & Trends Dashboard ")

    # 1. Insurance Map 
    st.subheader("Insurance Penetration Map")
    years_map = ["All"] + sorted(df_agg.get("Years", pd.Series()).unique().tolist())
    quarters_map = ["All"] + sorted(df_agg.get("Quarter", pd.Series()).unique().tolist())
    sel_year_map = st.selectbox("Select Year (Map)", years_map, key="map_year")
    sel_quarter_map = st.selectbox("Select Quarter (Map)", quarters_map, key="map_quarter")
    

    agg_filt = df_agg.copy()
    if sel_year_map != "All":
        agg_filt = agg_filt[agg_filt["Years"] == sel_year_map]
    if sel_quarter_map != "All":
        agg_filt = agg_filt[agg_filt["Quarter"] == sel_quarter_map]

    if agg_filt.empty:
        st.warning("⚠️ No state-level data available.")
    else:
        agg_filt["States"] = agg_filt["States"].str.title()
        with st.spinner("Loading map..."):
            tab1,tab2 = st.tabs(["Insurance amount" , "Insurance count"])
            with tab1:
                col1,col2 = st.columns(2)
                with col1:
                    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
                    geojson = requests.get(url).json()
                    map_df = safe_groupby(agg_filt, ["States"], {"Transaction_amount": "sum", "Transaction_count": "sum"})
                    fig_map = px.choropleth(
                        map_df,
                        geojson=geojson,
                        featureidkey="properties.ST_NM",
                        locations="States",
                        scope="asia",
                        color="Transaction_amount",
                        hover_data={"Transaction_count": True, "Transaction_amount": True},
                        title=f"Insurance Amount({sel_year_map}, Q{sel_quarter_map})",
                        width=1000, height=600
                    )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    st.plotly_chart(fig_map, use_container_width=True)
                plot_bar(map_df.nlargest(10, "Transaction_amount"), "States", "Transaction_amount", "Top 10 States by Insurance Amount", color="Transaction_amount")
                plot_bar(map_df.nsmallest(10, "Transaction_amount"), "States", "Transaction_amount", "Bottom 10 States by Insurance Amount", color="Transaction_amount")
                with col2:
                    fig1 = px.bar(
                        map_df,
                        x="States",
                        y="Transaction_amount",
                        title =f"Insurance Amount({sel_year_map}, Q{sel_quarter_map})",
                        text = "Transaction_amount"
                        )
                    fig1.update_traces(textposition="outside")
                    st.plotly_chart(fig1, use_container_width=True)
                        
            with tab2:
                col1,col2 = st.columns(2)
                with col1:
                    map_df = safe_groupby(agg_filt, ["States"], {"Transaction_amount": "sum", "Transaction_count": "sum"})
                    fig_map = px.choropleth(
                        map_df,
                        geojson=geojson,
                        featureidkey="properties.ST_NM",
                        locations="States",
                        scope="asia",
                        color="Transaction_count",
                        hover_data={"Transaction_count": True, "Transaction_amount": True},
                        title=f"Insurance Count ({sel_year_map}, Q{sel_quarter_map})",
                        width=1000, height=600
                    )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    st.plotly_chart(fig_map, use_container_width=True)
                with col2:
                    fig1 = px.bar(
                        map_df,
                        x="States",
                        y="Transaction_count",
                        title =f"Insurance Count ({sel_year_map}, Q{sel_quarter_map})",
                        text = "Transaction_count"
                        )
                    fig1.update_traces(textposition="outside")
                    st.plotly_chart(fig1, use_container_width=True)
                plot_bar(map_df.nlargest(10, "Transaction_count"), "States", "Transaction_count", "Top 10 States by Transaction Count", color="Transaction_count")
                plot_bar(map_df.nsmallest(10, "Transaction_count"), "States", "Transaction_count", "Bottom 10 States by Transaction Count", color="Transaction_count")
    
    st.write("*****************************************************************************************************")
    st.subheader("📈 Insurance Growth Trends (All India)")
    trend_data = safe_groupby(df_agg, ["Years", "Quarter"], {"Transaction_amount": "sum"})
    plot_line(trend_data, "Years", "Transaction_amount", "Insurance Growth Trends", color="Quarter")


    st.write("*****************************************************************************************************")
    st.subheader("📍 District & Pincode Hotspots")
    years_hot = ["All"] + sorted(df_map.get("Years", pd.Series()).unique().tolist())
    quarters_hot = ["All"] + sorted(df_map.get("Quarter", pd.Series()).unique().tolist())
    states_hot = ["All"] + sorted(df_map.get("States", pd.Series()).unique().tolist())
    sel_year_hot = st.selectbox("Select Year (Hotspots)", years_hot, key="hot_year")
    sel_quarter_hot = st.selectbox("Select Quarter (Hotspots)", quarters_hot, key="hot_quarter")
    sel_state_hot = st.selectbox("Select State (Hotspots)", states_hot, key="hot_state")
    data = df_map.copy()
    if sel_state_hot != "All":
        data = data[data["States"] == sel_state_hot]
    if sel_year_hot != "All":
            data = data[data["Years"] == int(sel_year_hot)]
    if sel_quarter_hot != "All":
            data = data[data["Quarter"] == int(sel_quarter_hot)]
    grouped = data.groupby(["States","District"])[["Transaction_amount", "Transaction_count"]].sum().reset_index()
    if grouped.empty:
        st.warning("No data available for the selected filters.")
        
    top5_amount = grouped.nlargest(5, "Transaction_amount")
    top5_count = grouped.nlargest(5, "Transaction_count")
    bottom5_amount = grouped.nsmallest(5, "Transaction_amount")
    bottom5_count = grouped.nsmallest(5, "Transaction_count")
    
    data1 = df_top.copy()
    if sel_state_hot != "All":
        data1 = data1[data1["States"] == sel_state_hot]
    if sel_year_hot != "All":
        data1 = data1[data1["Years"] == int(sel_year_hot)]
    if sel_quarter_hot != "All":
        data1 = data1[data1["Quarter"] == int(sel_quarter_hot)]
    grouped1 = data1.groupby(["States","Pincodes"])[["Transaction_amount", "Transaction_count"]].sum().reset_index()
    if grouped.empty:
        st.warning("No data available for the selected filters.")
        
    top5_amount1 = grouped1.nlargest(5, "Transaction_amount")
    top5_count1 = grouped1.nlargest(5, "Transaction_count")
    bottom5_amount1 = grouped1.nsmallest(5, "Transaction_amount")
    bottom5_count1 = grouped1.nsmallest(5, "Transaction_count")

    tab1,tab2 = st.tabs(["District - wise","Pincodes - wise"])
    with tab1:
        tab3,tab4 = st.tabs(["Transaction Amount","Transaction Count"])
        with tab3:
            col1,col2 = st.columns(2)
            with col1:
                fig1 = px.bar(
                    top5_amount,
                    x = "District",
                    y = "Transaction_amount",
                    title = f"Top 5 Insurance amount in {sel_state_hot} {sel_year_hot} Y and {sel_quarter_hot} Q",
                    text = "Transaction_amount",
                    hover_data=["States"]
                )
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)
            with col2:
                fig1 = px.bar(
                    bottom5_amount,
                    x = "District",
                    y = "Transaction_amount",
                    title = f"Bottom 5 Insurance amount in {sel_state_hot} {sel_year_hot} Y and {sel_quarter_hot} Q",
                    text = "Transaction_amount",
                    hover_data=["States"]
                )
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)
        
        with tab4:
            col1,col2 = st.columns(2)
            with col1:
                fig1 = px.bar(
                    top5_count,
                    x = "District",
                    y = "Transaction_count",
                    title = f"Top 5 Insurance count in {sel_state_hot} {sel_year_hot} Y and {sel_quarter_hot} Q",
                    text = "Transaction_count",
                    hover_data=["States"]
                )
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)
            with col2:
                fig1 = px.bar(
                    bottom5_count,
                    x = "District",
                    y = "Transaction_count",
                    title = f"Bottom 5 Insurance count in {sel_state_hot} {sel_year_hot} Y and {sel_quarter_hot} Q",
                    text = "Transaction_count",
                    hover_data=["States"]
                )
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)
    
    with tab2:
        tab3,tab4 = st.tabs(["Transaction Amount","Transaction Count"])
        with tab3:
            col1,col2 = st.columns(2)
            with col1:
                fig1 = px.bar(
                    top5_amount1,
                    x = "Pincodes",
                    y = "Transaction_amount",
                    title = f"Top 5 Insurance amount in {sel_state_hot} {sel_year_hot} Y and {sel_quarter_hot} Q",
                    text = "Transaction_amount",
                    hover_data=["States"]
                )
                fig1.update_xaxes(type="category")
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)
            with col2:
                fig1 = px.bar(
                    bottom5_amount1,
                    x = "Pincodes",
                    y = "Transaction_amount",
                    title = f"Bottom 5 Insurance amount in {sel_state_hot} {sel_year_hot} Y and {sel_quarter_hot} Q",
                    text = "Transaction_amount",
                    hover_data=["States"]
                )
                fig1.update_xaxes(type="category")
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)
        
        with tab4:
            col1,col2 = st.columns(2)
            with col1:
                fig1 = px.bar(
                    top5_count1,
                    x = "Pincodes",
                    y = "Transaction_count",
                    title = f"Top 5 Insurance count in {sel_state_hot} {sel_year_hot} Y and {sel_quarter_hot} Q",
                    text = "Transaction_count",
                    hover_data=["States"]
                )
                fig1.update_xaxes(type="category")
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)
            with col2:
                fig1 = px.bar(
                    bottom5_count1,
                    x = "Pincodes",
                    y = "Transaction_count",
                    title = f"Bottom 5 Insurance count in {sel_state_hot} {sel_year_hot} Y and {sel_quarter_hot} Q",
                    text = "Transaction_count",
                    hover_data=["States"]
                )
                fig1.update_xaxes(type="category")
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)
            
    st.write("*****************************************************************************************************")
    st.subheader("📊 Insurance vs User Growth (State, District & Pincode)")

    tabs = st.tabs(["State wise", "District wise", "Pincode wise"])
    tab_state, tab_dist, tab_pin = tabs

    with tab_state:
        state_ins = safe_groupby(Aggre_insurance, ["States", "Years", "Quarter"], {"Transaction_amount": "sum"})
        state_user = safe_groupby(Map_user, ["States", "Years", "Quarter"], {"RegisteredUser": "sum"})
        state_compare = pd.merge(state_ins, state_user, on=["States", "Years", "Quarter"], how="inner")
        plot_scatter(
            state_compare,
            x="Transaction_amount",
            y="RegisteredUser",
            color="States",
            title="State-wise Insurance vs User Growth",
            hover_data=["Years", "Quarter"]
        )

    with tab_dist:
        dist_ins = safe_groupby(Map_insurance, ["States", "District", "Years", "Quarter"], {"Transaction_amount": "sum"})
        dist_user = safe_groupby(Map_user, ["States", "District", "Years", "Quarter"], {"RegisteredUser": "sum"})
        dist_compare = pd.merge(dist_ins, dist_user, on=["States", "District", "Years", "Quarter"], how="inner")
        plot_scatter(
            dist_compare,
            x="Transaction_amount",
            y="RegisteredUser",
            color="States",
            title="District-wise Insurance vs User Growth",
            hover_data=["District", "Years", "Quarter"]
        )

    with tab_pin:
        pin_ins = safe_groupby(Top_insurance, ["States", "Pincodes", "Years", "Quarter"], {"Transaction_amount": "sum"})
        pin_user = safe_groupby(Top_user, ["States", "Pincodes", "Years", "Quarter"], {"RegisteredUser": "sum"})
        pin_ins["Pincodes"] = pin_ins["Pincodes"].astype(str)
        pin_user["Pincodes"] = pin_user["Pincodes"].astype(str)
        pin_compare = pd.merge(pin_ins, pin_user, on=["States", "Pincodes", "Years", "Quarter"], how="inner")
        plot_scatter(
            pin_compare,
            x="Transaction_amount",
            y="RegisteredUser",
            color="States",
            title="Pincode-wise Insurance vs User Growth",
            hover_data=["Pincodes", "Years", "Quarter"]
    )
    st.write("*****************************************************************************************************")
    st.subheader("📈 Penetration & Growth Analysis (State, District & Pincode)")

    # Year selection
    available_years = sorted(state_compare["Years"].unique())
    year_options = ["Overall"] + available_years
    current_year = st.selectbox("Select Current Year", year_options, key="current_year_select")
    selected_year = st.selectbox("Select Comparison Year", year_options, key="selected_year_select")

    def calc_penetration(df, group_cols, value_col, user_col):
        df = df.copy()
        df["Penetration"] = df[value_col] / df[user_col]
        return df

    # Apply filtering based on current_year
    def filter_year(df, year):
        if year != "Overall":
            return df[df["Years"] == year]
        return df

    tabs1 = st.tabs(["State wise", "District wise", "Pincode wise"])
    tab_state1, tab_dist1, tab_pin1 = tabs1

    # STATE LEVEL PENETRATION
    with tab_state1:
    
        state_filt1 = filter_year(state_compare, current_year)

        state_filt = (
            state_filt1.groupby("States", as_index=False)
            .agg({"Transaction_amount": "sum", "RegisteredUser": "sum"})
        )
        state_filt["Penetration"] = (
            state_filt["Transaction_amount"] / state_filt["RegisteredUser"]
        )

        # GeoJSON for India states
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        geo_data = json.loads(requests.get(url).content)

        fig = px.choropleth(
            state_filt,
            geojson=geo_data,
            locations="States",
            featureidkey="properties.ST_NM",
            color="Penetration",
            hover_name="States",
            title=f"Penetration - {current_year}",
            fitbounds="locations",
            width=800,
            height=600
        )
        fig.update_geos(visible=False)
        st.plotly_chart(fig, use_container_width=True)
        
        top5_states = state_filt.nlargest(5, "Penetration")
        bottom5_states = state_filt.nsmallest(5, "Penetration")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader(f"Top 5 States by Penetration ({current_year})")
            fig_top5 = px.bar(
            top5_states,
            x="States",
            y="Penetration",
            title="Top 5 States by Penetration",
            color="Penetration"
        )
            st.plotly_chart(fig_top5, use_container_width=True)

        with col2:
            st.subheader(f"Bottom 5 States by Penetration ({current_year})")
            fig_bottom5 = px.bar(
            bottom5_states,
            x="States",
            y="Penetration",
            title="Bottom 5 States by Penetration",
            color="Penetration"
        )
            st.plotly_chart(fig_bottom5, use_container_width=True)

    # DISTRICT LEVEL PENETRATION
    with tab_dist1:
        dist_filt1 = calc_penetration(
            filter_year(dist_compare, current_year), 
            ["District", "States"], "Transaction_amount", "RegisteredUser"
        )

        top_state = dist_filt1.groupby(["District","States"])["Penetration"].mean().sort_values(ascending=False).head(5)
        bottom_state = dist_filt1.groupby(["District","States"])["Penetration"].mean().sort_values().head(5)

        col1, col2 = st.columns(2)
        with col1:
            plot_bar(top_state.reset_index(), "District", "Penetration",
                    f"Top 5 Districts by Penetration ({current_year})", color="Penetration", hover_data=["States"])
        with col2:
            plot_bar(bottom_state.reset_index(), "District", "Penetration",
                    f"Bottom 5 Districts by Penetration ({current_year})", color="Penetration", hover_data=["States"])

    # PINCODE LEVEL PENETRATION
    with tab_pin1:
        pin_filt1 = calc_penetration(
            filter_year(pin_compare, current_year), 
            ["District", "States"], "Transaction_amount", "RegisteredUser"
        )
        
        top_pins = pin_filt1.groupby(["Pincodes","States"])["Penetration"].mean().sort_values(ascending=False).head(5).reset_index()
        bottom_pins = pin_filt1.groupby(["Pincodes","States"])["Penetration"].mean().sort_values().head(5).reset_index()

        col1, col2 = st.columns(2)
        with col1:
            top_pins["Pincodes"] = top_pins["Pincodes"].astype(str)  
            fig_top = px.bar(
                top_pins, x="Pincodes", y="Penetration", 
                title=f"Top 5 Pincodes by Penetration ({current_year})", 
                color="Penetration", hover_data=["States"]
            )
            fig_top.update_xaxes(type="category")
            st.plotly_chart(fig_top, use_container_width=True)

        with col2:
            bottom_pins["Pincodes"] = bottom_pins["Pincodes"].astype(str)  
            fig_bottom = px.bar(
                bottom_pins, x="Pincodes", y="Penetration", 
                title=f"Bottom 5 Pincodes by Penetration ({current_year})", 
                color="Penetration", hover_data=["States"]
            )
            fig_bottom.update_xaxes(type="category")
            st.plotly_chart(fig_bottom, use_container_width=True)


    st.write("*****************************************************************************************************")
    st.subheader("📊 Growth Trend Analysis")
    
    available_years = sorted(state_compare["Years"].unique())
    year_options = ["Overall"] + available_years
    current_year = st.selectbox("Select Current Year", year_options, key="current_year_select_for_growth")
    selected_year = st.selectbox("Select Comparison Year", year_options, key="selected_year_select_for_growth")

    def calculate_year_growth(df, group_cols, current_year, selected_year):
        years = sorted(df["Years"].unique())
        if current_year == 'Overall' or selected_year == 'Overall':
            if len(years) < 2:
                return pd.DataFrame()  # Not enough data
            first_year, last_year = years[0], years[-1]
            df_years = df[df["Years"].isin([first_year, last_year])]
            pivot = df_years.pivot_table(index=group_cols, columns="Years", values="Transaction_amount", aggfunc="sum")
            pivot["Growth(%)"] = ((pivot[last_year] / pivot[first_year]) - 1) * 100
            pivot = pivot.reset_index()
            pivot["Compared Years"] = f"{first_year} vs {last_year}"
            return pivot
        else:
            df_years = df[df["Years"].isin([current_year, selected_year])]
            pivot = df_years.pivot_table(index=group_cols, columns="Years", values="Transaction_amount", aggfunc="sum")
            pivot["Growth(%)"] = ((pivot[current_year] / pivot[selected_year]) - 1) * 100
            pivot = pivot.reset_index()
            pivot["Compared Years"] = f"{selected_year} vs {current_year}"
            return pivot
    tabs2 = st.tabs(["State wise", "District wise", "Pincode wise"])
    tab_state2, tab_dist2, tab_pin2 = tabs2
    
    # State level growth
    with tab_state2:
        state_growth = calculate_year_growth(state_compare, ["States"], current_year, selected_year)
        top5_state = state_growth.nlargest(5, "Growth(%)")
        bottom5_state = state_growth.nsmallest(5, "Growth(%)")
        col1, col2 = st.columns(2)
        with col1:
            plot_bar(top5_state, "States", "Growth(%)", f"Top 5 States by Growth (%) ({current_year} vs {selected_year})", color="Growth(%)", color_scale="Plasma")
        with col2:
            plot_bar(bottom5_state, "States", "Growth(%)", f"Bottom 5 States by Growth (%) ({current_year} vs {selected_year})", color="Growth(%)", color_scale="Magma")
    
    # District level growth
    with tab_dist2: 
        district_growth = calculate_year_growth(dist_compare, ["District", "States"], current_year, selected_year)
        top5_dist = district_growth.nlargest(5, "Growth(%)")
        bottom5_dist = district_growth.nsmallest(5, "Growth(%)")
        col3, col4 = st.columns(2)
        with col3:
            plot_bar(top5_dist, "District", "Growth(%)", f"Top 5 Districts by Growth (%) ({current_year} vs {selected_year})", color="Growth(%)", color_scale="Plasma",hover_data="States")
        with col4:
            plot_bar(bottom5_dist, "District", "Growth(%)", f"Bottom 5 Districts by Growth (%) ({current_year} vs {selected_year})", color="Growth(%)", color_scale="Magma",hover_data="States")
    
    # Pincode level growth
    with tab_pin2:
        pincode_growth = calculate_year_growth(pin_compare, ["Pincodes", "States"], current_year, selected_year)
        top5_pin = pincode_growth.nlargest(5, "Growth(%)")
        bottom5_pin = pincode_growth.nsmallest(5, "Growth(%)")
        top5_pin["Pincodes"] =top5_pin["Pincodes"].astype(str)
        bottom5_pin["Pincodes"] =bottom5_pin["Pincodes"].astype(str)
        col5, col6 = st.columns(2)
        with col5:
            top_pin=px.bar(
                top5_pin,x="Pincodes", y="Growth(%)", title=f"Top 5 Pincodes by Growth (%) ({current_year} vs {selected_year})", 
                color="Growth(%)", hover_data=["States"]
            )
            top_pin.update_xaxes(type="category")
            st.plotly_chart(top_pin, use_container_width=True)
        with col6:
            
            bottom_pin1=px.bar(
                bottom5_pin,x="Pincodes", y="Growth(%)", title=f"Bottom 5 Pincodes by Growth (%) ({current_year} vs {selected_year})", 
                color="Growth(%)", hover_data=["States"]
            )
            bottom_pin1.update_xaxes(type="category")
            st.plotly_chart(bottom_pin1, use_container_width=True)

# Helper Functions
def calc_penetration(df, group_cols, value_col, user_col):
    df = df.copy()
    df["Penetration"] = df[value_col] / df[user_col]
    return df

def calculate_year_growth(df, group_cols, current_year, selected_year):
    years = sorted(df["Years"].unique())
    if current_year == 'Overall' or selected_year == 'Overall':
        if len(years) < 2:
            return pd.DataFrame()  # Not enough data
        first_year, last_year = years[0], years[-1]
        df_years = df[df["Years"].isin([first_year, last_year])]
        pivot = df_years.pivot_table(index=group_cols, columns="Years",
                                     values="Transaction_amount", aggfunc="sum")
        pivot["Growth(%)"] = ((pivot[last_year] / pivot[first_year])- 1) * 100 
        pivot["Growth(%)"] = pivot["Growth(%)"].round(0)
        pivot = pivot.reset_index()
        pivot["Compared Years"] = f"{first_year} vs {last_year}"
        return pivot
    else:
        df_years = df[df["Years"].isin([current_year, selected_year])]
        pivot = df_years.pivot_table(index=group_cols, columns="Years",
                                     values="Transaction_amount", aggfunc="sum")
        pivot["Growth(%)"] = ((pivot[current_year] / pivot[selected_year]) - 1) * 100
        pivot["Growth(%)"] = pivot["Growth(%)"].round(0)
        pivot = pivot.reset_index()
        pivot["Compared Years"] = f"{selected_year} vs {current_year}"
        return pivot
def calculate_year_growth1(df, group_cols, current_year, selected_year):
    years = sorted(df["Years"].unique())
    if current_year == 'Overall' or selected_year == 'Overall':
        if len(years) < 2:
            return pd.DataFrame()  # Not enough data
        first_year, last_year = years[0], years[-1]
        df_years = df[df["Years"].isin([first_year, last_year])]
        pivot = df_years.pivot_table(index=group_cols, columns="Years",
                                     values="Transaction_amount", aggfunc="sum")
        pivot["Growth(%)"] = ((pivot[last_year] / pivot[first_year])- 1) * 100 
        pivot["Growth(%)"] = pivot["Growth(%)"].round(0).astype(int).astype(str) + "%"
        pivot = pivot.reset_index()
        pivot["Compared Years"] = f"{first_year} vs {last_year}"
        return pivot
    else:
        df_years = df[df["Years"].isin([current_year, selected_year])]
        pivot = df_years.pivot_table(index=group_cols, columns="Years",
                                     values="Transaction_amount", aggfunc="sum")
        pivot["Growth(%)"] = ((pivot[current_year] / pivot[selected_year]) - 1) * 100
        pivot["Growth(%)"] = pivot["Growth(%)"].round(0).astype(int).astype(str) + "%"
        pivot = pivot.reset_index()
        pivot["Compared Years"] = f"{selected_year} vs {current_year}"
        return pivot

def plot_bar(df, x, y, title, color=None, color_scale="Viridis", hover_data=None):
    fig = px.bar(df, x=x, y=y, color=color,
                 color_continuous_scale=color_scale if color else None,
                 hover_data=hover_data, title=title)
    fig.update_traces(texttemplate='%{y}', textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

def ques4(df_transaction, df_user):

    st.header("Transaction Analysis for Market Expansion")
    years = ["All"] + sorted(df_transaction["Years"].unique().tolist())
    selected_year = st.selectbox("Select Year", years, index=0)
    quarters = ["All"] + sorted(df_transaction["Quarter"].unique().tolist())
    selected_quarter = st.selectbox("Select Quarter", quarters, index=0)
    df_txn = df_transaction.copy()
    df_usr = df_user.copy()

    if selected_year != "All":
        df_txn = df_txn[df_txn["Years"] == selected_year]
        df_usr = df_usr[df_usr["Years"] == selected_year]

    if selected_quarter != "All":
        df_txn = df_txn[df_txn["Quarter"] == selected_quarter]
        df_usr = df_usr[df_usr["Quarter"] == selected_quarter]
    df_txn_group = df_txn.groupby(["States", "Years", "Quarter"]).agg({
        "Transaction_amount": "sum",
        "Transaction_count": "sum"
    }).reset_index()

    df_usr_group = df_usr.groupby(["States", "Years", "Quarter"]).agg({
        "RegisteredUser": "sum",
        "AppOpens": "sum",
    }).reset_index()

    df_merge = pd.merge(df_txn_group, df_usr_group, on=["States", "Years", "Quarter"], how="inner")

    # States Aggregated
    df_total = df_merge.groupby("States").agg({
        "Transaction_amount": "sum",
        "Transaction_count": "sum",
        "RegisteredUser": "sum",
        "AppOpens": "sum"
    }).reset_index()
     
    top_state_count = df_total.nlargest(5, "Transaction_count")
    bottom_state_count = df_total.nsmallest(5, "Transaction_count")
    top_state_amount = df_total.nlargest(5, "Transaction_amount")
    bottom_state_amount = df_total.nsmallest(5, "Transaction_amount")
    top_state_user = df_total.nlargest(5, "RegisteredUser")
    bottom_state_user = df_total.nsmallest(5, "RegisteredUser")
    top_state_open = df_total.nlargest(5, "AppOpens")
    bottom_state_open = df_total.nsmallest(5, "AppOpens")
    
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    geo_data = json.loads(requests.get(url).content)

    tab_amt, tab_cnt = st.tabs(["Transaction Amount", "Transaction Count"])
    with tab_amt: 
        col1, col2 = st.columns(2)
        with col1:
            fig = px.choropleth(
                df_total, geojson=geo_data, locations="States",
                featureidkey="properties.ST_NM", color="Transaction_amount",
                hover_name="States", hover_data={"Transaction_amount": True},
                title=f"Transaction Amount in {selected_year} Y and {selected_quarter} Q",
                fitbounds="locations", width=800, height=600
            )
            fig.update_geos(visible=False)
            st.plotly_chart(fig, use_container_width=True)
            plot_bar(top_state_amount, "States", "Transaction_amount", "Top States - Transaction amount", color="Transaction_amount", color_scale="Blues")
           
        with col2:
            plot_bar(df_total, "States", "Transaction_amount", 
                    f"Transaction Amount in {selected_year} Y and {selected_quarter} Q", 
                    color="Transaction_amount")
            
            plot_bar(bottom_state_amount, "States", "Transaction_amount", "Bottom States - Transaction amount", color="Transaction_amount", color_scale="Reds")

    with tab_cnt:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.choropleth(
                df_total, geojson=geo_data, locations="States",
                featureidkey="properties.ST_NM", color="Transaction_count",
                hover_name="States", hover_data={"Transaction_count": True},
                title=f"Transaction Count in {selected_year} Y and {selected_quarter} Q",
                fitbounds="locations", width=800, height=600
            )
            fig.update_geos(visible=False)
            st.plotly_chart(fig, use_container_width=True)
            plot_bar(top_state_count, "States", "Transaction_count", "Top States - Transaction Count", color="Transaction_count", color_scale="Blues")  
        with col2:
            plot_bar(df_total, "States", "Transaction_count", 
                    f"Transaction Count in {selected_year} Y and {selected_quarter} Q", 
                    color="Transaction_count")
            plot_bar(bottom_state_count, "States", "Transaction_count", "Bottom States - Transaction Count", color="Transaction_count", color_scale="Reds")

    tab_usr, tab_app = st.tabs(["Registered Users", "App Opens"])
    with tab_usr:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.choropleth(
                df_total, geojson=geo_data, locations="States",
                featureidkey="properties.ST_NM", color="RegisteredUser",
                hover_name="States", hover_data={"RegisteredUser": True},
                title=f"Registered Users in {selected_year} Y and {selected_quarter} Q",
                fitbounds="locations", width=800, height=600
            )
            fig.update_geos(visible=False)
            st.plotly_chart(fig, use_container_width=True)
            plot_bar(top_state_user, "States", "RegisteredUser", "Top States - Registered Users", color="RegisteredUser", color_scale="Blues")
        with col2:
            plot_bar(df_total, "States", "RegisteredUser", 
                    f"Registered Users in {selected_year} Y and {selected_quarter} Q", 
                    color="RegisteredUser")
            plot_bar(bottom_state_user, "States", "RegisteredUser", "Bottom States - Registered Users", color="RegisteredUser", color_scale="Reds")
    with tab_app:
        col1, col2 = st.columns(2)
        with col1:
            if df_total["AppOpens"].empty:
                st.warning("no data in year or quarter")
            else:   
                fig = px.choropleth(
                    df_total, geojson=geo_data, locations="States",
                    featureidkey="properties.ST_NM", color="AppOpens",
                    hover_name="States", hover_data={"AppOpens": True},
                    title=f"App Opens in {selected_year} Y and {selected_quarter} Q",
                    fitbounds="locations", width=800, height=600
                )
                fig.update_geos(visible=False)
                st.plotly_chart(fig, use_container_width=True)
                plot_bar(top_state_open, "States", "AppOpens", "Top States - App Opens", color="AppOpens", color_scale="Blues")
        with col2:
            plot_bar(df_total, "States", "AppOpens", 
                    f"App Opens in {selected_year} Y and {selected_quarter} Q", 
                    color="AppOpens")
            plot_bar(bottom_state_open, "States", "AppOpens", "Bottom States - App Opens", color="AppOpens", color_scale="Reds")
   
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    geo_data = json.loads(requests.get(url).content)

    col1, col2 = st.columns(2)

    with col1:
        st.selectbox("Overall", ["ALL"])
        fig = px.choropleth(
            df_total,
            geojson=geo_data,
            locations="States",
            scope="asia",
            featureidkey="properties.ST_NM",
            color="Transaction_count",
            hover_name="States",
            hover_data={
                "Transaction_count": True,
                "RegisteredUser": True
            },
            title="Transaction Count vs Registered Users (All Years)",
            fitbounds="locations",
            width=800,
            height=600
        )
        fig.update_geos(visible=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        years = sorted(df_transaction["Years"].unique().tolist())
        years.insert(0, "All") 
        selected_year = st.selectbox("Select the Year", years)

        if selected_year != "All":
            df_transaction = df_transaction[df_transaction["Years"] == selected_year]
            df_user = df_user[df_user["Years"] == selected_year]

        if selected_year != "All":
            df_year = df_merge[df_merge["Years"] == selected_year]
        else:
            df_year = df_merge.copy()

        df_yearwise = df_year.groupby("States").agg({
            "Transaction_count": "sum",
            "RegisteredUser": "sum"
        }).reset_index()

        fig1 = px.choropleth(
            df_yearwise,
            geojson=geo_data,
            locations="States",
            scope="asia",
            featureidkey="properties.ST_NM",
            color="Transaction_count",
            hover_name="States",
            hover_data={
                "Transaction_count": True,
                "RegisteredUser": True
            },
            title=f"Transaction Count vs Registered Users - {selected_year}",
            fitbounds="locations",
            width=800,
            height=600
        )
        fig1.update_geos(visible=False)
        st.plotly_chart(fig1, use_container_width=True)

    # State Level Penetration 
    state_ins = safe_groupby(df_transaction, ["States", "Years", "Quarter"], {"Transaction_amount": "sum"})
    state_user = safe_groupby(df_user, ["States", "Years", "Quarter"], {"RegisteredUser": "sum","AppOpens" : "sum"})
    state_compare = pd.merge(state_ins, state_user, on=["States", "Years", "Quarter"], how="inner")

    years = ["All"] + sorted(df_transaction["Years"].unique().tolist())
    selected_year = st.selectbox("Select Year for Penetration", years, key="state")

    st.subheader("State Level Penetration")
    st.write("📌 Penetration = Transaction_amount / RegisteredUser")

    # OVERALL (Aggregate Penetration)
    state_filt1 = calc_penetration(state_compare, ["States", "Years"], "Transaction_amount", "RegisteredUser")
    df_state_filt1 = state_filt1.groupby("States").agg({
            "Penetration": "sum",
        }).reset_index()

    col1, col2 = st.columns(2)
    with col1:
        fig = px.choropleth(
            df_state_filt1,
            geojson=geo_data,
            locations="States",
            scope="asia",
            featureidkey="properties.ST_NM",
            color="Penetration",
            hover_name="States",
            hover_data={"Penetration": True},
            title="🌍 Overall Penetration by States",
            fitbounds="locations",
            width=800,
            height=600
        )
        fig.update_geos(visible=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        plot_bar(df_state_filt1, "States", "Penetration",
                "States by Penetration (Overall)", color="Penetration")

    # YEAR-WISE Penetration
    if selected_year == "All":
        state_filt = calc_penetration(state_compare, ["States", "Years"], "Transaction_amount", "RegisteredUser")
    else:
        state_year = state_compare[state_compare["Years"] == selected_year]
        state_filt = calc_penetration(state_year, ["States", "Years"], "Transaction_amount", "RegisteredUser")

    df_state_filt = state_filt.groupby("States").agg({
            "Penetration": "sum",
        }).reset_index()

    col3, col4 = st.columns(2)
    with col3:
        fig1 = px.choropleth(
            df_state_filt,
            geojson=geo_data,
            locations="States",
            scope="asia",
            featureidkey="properties.ST_NM",
            color="Penetration",
            hover_name="States",
            hover_data={"Penetration": True},
            title=f"📅 Penetration by States ({selected_year})",
            fitbounds="locations",
            width=800,
            height=600
        )
        fig1.update_geos(visible=False)
        st.plotly_chart(fig1, use_container_width=True)

    with col4:
        plot_bar(df_state_filt, "States", "Penetration",
                f"States by Penetration ({selected_year})", color="Penetration")

        
    #Top & Bottom States by Penetration 
    top_states = df_state_filt.nlargest(5, "Penetration")
    bottom_states = df_state_filt.nsmallest(5, "Penetration")

    col1, col2 = st.columns(2)
    with col1:
        plot_bar(top_states, "States", "Penetration", f"Top 5 States by Penetration ({selected_year})", color="Penetration", color_scale="Viridis")
    with col2:
        plot_bar(bottom_states, "States", "Penetration", f"Bottom 5 States by Penetration ({selected_year})", color="Penetration", color_scale="Reds")

    # State Level Growth 
    available_years = sorted(state_compare["Years"].unique())
    year_options = ["Overall"] + available_years
    current_year = st.selectbox("Select Current Year", year_options, key="current_year_select_for_growth1")
    compare_year = st.selectbox("Select Comparison Year", year_options, key="selected_year_select_for_growth1")
    state_growth = calculate_year_growth(state_compare, ["States"], current_year, compare_year)
    state_growth1 = calculate_year_growth1(state_compare, ["States"], current_year, compare_year)
    plot_bar(state_growth1, "States", "Growth(%)", f"States by Growth (%)", color="Growth(%)", color_scale="Growth(%)")
    fig = px.pie(
        state_growth,
        names="States",         
        values="Growth(%)",     
        title="States by Growth (%)", 
        hole=0.3,                
        color="Growth(%)",       
    )

    st.plotly_chart(fig, use_container_width=True)
    fig = px.choropleth( 
     state_growth1, 
     geojson=geo_data, 
     locations="States", 
     scope="world", 
     featureidkey="properties.ST_NM", 
     color="Growth(%)", 
     hover_name="States", 
     title=f"Growth(%)", 
     fitbounds="locations", 
     width=800, 
     height=600) 
    fig.update_geos(visible=False) 
    st.plotly_chart(fig, use_container_width=True)

    if not state_growth.empty:
        top5_state = state_growth.nlargest(5, "Growth(%)")
        bottom5_state = state_growth.nsmallest(5, "Growth(%)")

        col1, col2 = st.columns(2)
        with col1:
            plot_bar(top5_state, "States", "Growth(%)", f"Top 5 States by Growth (%) ", color="Growth(%)", color_scale="Plasma")
        with col2:
            plot_bar(bottom5_state, "States", "Growth(%)", f"Bottom 5 States by Growth (%) )", color="Growth(%)", color_scale="Magma")
    def calc_avg_user_usage(df, group_cols, value_col, user_col):
        df = df.copy()
        df["Average Usage"] = df[value_col] / df[user_col]
        df["Average Usage"] = df["Average Usage"].round(0)
        return df
    
    years =sorted(df_transaction["Years"].unique().tolist())
    selected_year = st.selectbox("Select Year for Average usage", years, key="state1")
    df_state_filt_all = calc_avg_user_usage(
            df_total, 
            ["States", "Years"], 
            "AppOpens",             
            "RegisteredUser"              
        )
    state_year = state_compare[state_compare["Years"] == selected_year]
    state_filt = calc_avg_user_usage(
            state_year, 
            ["States", "Years"],
            "AppOpens",        
            "RegisteredUser" 
        )
    df_state_filt = state_filt.groupby(["States", "Years"]).agg({
        "Average Usage": "sum"
    }).reset_index()
    
    col1, col2 = st.columns(2)
        
    with col1:    
        plot_bar(df_state_filt_all,"States","Average Usage",'Average usage by user',color="Average Usage")
        fig = px.choropleth( 
        df_state_filt_all, 
        geojson=geo_data, 
        locations="States", 
        featureidkey="properties.ST_NM", 
        color="Average Usage", 
        hover_name="States", 
        title=f"Average Usage", 
        fitbounds="locations", 
        width=800, 
        height=600) 
        fig.update_geos(visible=False) 
        st.plotly_chart(fig, use_container_width=True)
        st.subheader("Performance based on Average Usage")
        top_state_Average = df_state_filt_all.nlargest(5, "Average Usage")
        bottom_state_Average = df_state_filt_all.nsmallest(5, "Average Usage")
        plot_bar(top_state_Average, "States", "Average Usage", f'Top 5 Average usage by user ', color="Average Usage")
        plot_bar(bottom_state_Average, "States", "Average Usage", f'Bottom 5 Average usage by user ', color="Average Usage")
    with col2:
        if df_state_filt["Average Usage"].sum() == 0:   
            st.warning(f"No data available for {selected_year}", icon="⚠️")
        else:
            plot_bar(df_state_filt, "States", "Average Usage", f'Average usage by user - {selected_year}', color="Average Usage")
            fig = px.choropleth( 
            state_filt, 
            geojson=geo_data, 
            locations="States", 
            featureidkey="properties.ST_NM", 
            color="Average Usage", 
            hover_name="States", 
            title=f"Average Usage- {selected_year}", 
            fitbounds="locations", 
            width=800, 
            height=600) 
            fig.update_geos(visible=False) 
            st.plotly_chart(fig, use_container_width=True)
            st.subheader(f"Performance based on Average Usage - {selected_year}")
            top_state_Average = df_state_filt.nlargest(5, "Average Usage")
            bottom_state_Average = df_state_filt.nsmallest(5, "Average Usage")
            plot_bar(top_state_Average, "States", "Average Usage", f'Top 5 Average usage by user - {selected_year}',color="Average Usage")
            plot_bar(bottom_state_Average, "States", "Average Usage", f'Bottom 5 Average usage by user - {selected_year}',color="Average Usage")
    
def ques5(Aggre_user, Map_user, Top_user, Top_district,Top_transaction):
    st.title("User Engagement & Growth Strategy")
    st.markdown("### Engagement Ratio by State")
    years = ["All"] + sorted(Map_user["Years"].unique().tolist())
    quarters = ["All"] + sorted(Map_user["Quarter"].unique().tolist())

    sel_year = st.selectbox("Select Year", years, index=0)
    sel_quarter = st.selectbox("Select Quarter", quarters, index=0)

    df_filtered = Map_user.copy()
    if sel_year != "All":
        df_filtered = df_filtered[df_filtered["Years"] == sel_year]
    if sel_quarter != "All":
        df_filtered = df_filtered[df_filtered["Quarter"] == sel_quarter]


    total_users = df_filtered["RegisteredUser"].sum()
    total_appopens = df_filtered["AppOpens"].sum()
    engagement_ratio = round(total_appopens / total_users, 2) if total_users > 0 else 0

    st.markdown("### Key Metrics")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Registered Users", f"{total_users:,}")
    col2.metric("Total App Opens", f"{total_appopens:,}")
    col3.metric("Engagement Ratio", engagement_ratio)

  
    # State Engagement
    state_engagement = df_filtered.groupby("States").agg({
        "RegisteredUser": "sum",
        "AppOpens": "sum"
    }).reset_index()

    state_engagement["EngagementRatio"] = state_engagement.apply(
        lambda row: row["AppOpens"] / row["RegisteredUser"] if row["RegisteredUser"] > 0 else 0, axis=1
    )
    state_engagement["EngagementRatio"] = state_engagement["EngagementRatio"].round(2)

    # District Engagement 
    District_engagement = df_filtered.groupby(["States","District"]).agg({
        "RegisteredUser": "sum",
        "AppOpens": "sum"
    }).reset_index()

    District_engagement["EngagementRatio"] = District_engagement.apply(
        lambda row: row["AppOpens"] / row["RegisteredUser"] if row["RegisteredUser"] > 0 else 0, axis=1
    )
    District_engagement["EngagementRatio"] = District_engagement["EngagementRatio"].round(2)
    
    tab1 , tab2 = st.tabs(["🗺️ Map", "📊 Bar chart"])
    with tab2:
        plot_bar(
            state_engagement,
            x="States",
            y="EngagementRatio",
            title=f"State-wise Engagement Ratio ({sel_year}, Q{sel_quarter})" if sel_year!="All" or sel_quarter!="All" else "State-wise Engagement Ratio (Overall)",
            hover_data=["RegisteredUser","AppOpens"]
        )

    with tab1:
        fig = px.choropleth(
            state_engagement,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey="properties.ST_NM",
            locations="States",
            color="EngagementRatio",
            color_continuous_scale="Blues",
            hover_data=["RegisteredUser","AppOpens"],
            title=f"State-wise Engagement Ratio ({sel_year}, Q{sel_quarter})" if sel_year!="All" or sel_quarter!="All" else "State-wise Engagement Ratio (Overall)",
            width=800,
            height=700
        )
        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig, use_container_width=True)

    # Year-wise Bar Charts
    st.markdown("### Year-wise Trends")

    yearly_stats = Map_user.groupby("Years").agg({
        "RegisteredUser": "sum",
        "AppOpens": "sum"
    }).reset_index()
    yearly_stats["EngagementRatio"] = yearly_stats["AppOpens"] / yearly_stats["RegisteredUser"]
    yearly_stats["EngagementRatio"] = yearly_stats["EngagementRatio"].round(2)
    col1, col2, col3 = st.columns(3)

    with col1:
        fig1 = px.bar(yearly_stats, x="Years", y="RegisteredUser", text="RegisteredUser",
                      title="Year-wise Registered Users")
        fig1.update_xaxes(type="category")
        fig1.update_traces(textposition="outside")
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        fig2 = px.bar(yearly_stats, x="Years", y="AppOpens", text="AppOpens",
                      title="Year-wise App Opens")
        fig2.update_traces(textposition="outside")
        fig2.update_xaxes(type="category")
        st.plotly_chart(fig2, use_container_width=True)

    with col3:
        fig3 = px.bar(yearly_stats, x="Years", y="EngagementRatio", text="EngagementRatio",
                      title="Year-wise Engagement Ratio")
        fig3.update_xaxes(type="category")
        fig3.update_traces(textposition="outside")
        st.plotly_chart(fig3, use_container_width=True)

    st.markdown("### Quarter-wise Trends")

    quarter_stats = Map_user.groupby(["Quarter"]).agg({
        "RegisteredUser": "sum",
        "AppOpens": "sum"
    }).reset_index()
    quarter_stats["EngagementRatio"] = (
        quarter_stats["AppOpens"] / quarter_stats["RegisteredUser"]
    ).round(2)

    col1, col2 ,col3 = st.columns(3)

    with col1:
        st.subheader("Registered Users")
        fig_reg = px.bar(
            quarter_stats,
            x="Quarter",
            y="RegisteredUser",
            text="RegisteredUser",
            title="Quarter-wise Registered Users"
        )
        fig_reg.update_traces(textposition="outside")
        st.plotly_chart(fig_reg, use_container_width=True)

    with col2:
        st.subheader("App Opens")
        fig_app = px.bar(
            quarter_stats,
            x="Quarter",
            y="AppOpens",
            text="AppOpens",
            title="Quarter-wise App Opens"
        )
        fig_app.update_traces(textposition="outside")
        st.plotly_chart(fig_app, use_container_width=True)

    with col3:
        st.subheader("Engagement Ratio")
        fig_app = px.bar(
            quarter_stats,
            x="Quarter",
            y="EngagementRatio",
            text="EngagementRatio",    
            title="Quarter-wise Engagement Ratio"
        )
        fig_app.update_traces(textposition="outside")
        st.plotly_chart(fig_app, use_container_width=True)

    quarter_stats1 = Map_user.groupby(["Years","Quarter"]).agg({
        "RegisteredUser": "sum",
        "AppOpens": "sum"
    }).reset_index()      
    quarter_stats1["Period"] = quarter_stats1["Years"].astype(str) + "-Q" + quarter_stats1["Quarter"].astype(str) 
    quarter_stats1["EngagementRatio"] = (
        quarter_stats1["AppOpens"] / quarter_stats1["RegisteredUser"]
    ).round(2)
    
    st.markdown("### Engagement ratio Over Time")
    fig2 = px.line(
        quarter_stats1,
        x="Period",
        y="EngagementRatio",
        markers=True,
        title="Quarter-wise Engagement Ratio",
        hover_data=["RegisteredUser","AppOpens"]
    )
    st.plotly_chart(fig2, use_container_width=True)

    growth = Map_user.groupby(["Years", "Quarter"]).agg({"RegisteredUser":"sum"}).reset_index()
    growth["Period"] = growth["Years"].astype(str) + "-Q" + growth["Quarter"].astype(str)

    st.markdown("### User Growth Over Time")
    fig2 = px.line(
        growth,
        x="Period",
        y="RegisteredUser",
        markers=True,
        title="Quarterly Growth of Registered Users",
        hover_data=["RegisteredUser"]
    )
    st.plotly_chart(fig2, use_container_width=True)
    
    st.markdown("### User loyalty Index (App Opens vs Users)")
    fig3 = px.scatter(
        state_engagement,
        x="RegisteredUser",
        y="AppOpens",
        size="EngagementRatio",
        color="States",
        hover_name="States",
        title=f"State-wise user loyalty Index {sel_year} Year and Quarter {sel_quarter}"
    )
    st.plotly_chart(fig3, use_container_width=True)
    
    top5_states = state_engagement.sort_values(by="EngagementRatio", ascending=False).head(5)
    bottom5_states = state_engagement.sort_values(by="EngagementRatio", ascending=False).tail(5)
    col1,col2 = st.columns(2)
    with col1:
        fig3 = px.bar(
            top5_states,
            x="States",
            y="EngagementRatio",
            color="States",
            title=f"Top 5 States by User loyalty Index (App Opens vs Users) in {sel_year} Year and Quarter {sel_quarter}",
            hover_data=["RegisteredUser","AppOpens"],
            text="EngagementRatio"
            )
        fig3.update_traces(textposition="outside")
        st.plotly_chart(fig3, use_container_width=True)
        
    with col2:
        fig4 = px.bar(
            bottom5_states,
            x="States",
            y="EngagementRatio",
            color="States",
            title=f"Bottom 5 States by User loyalty Index (App Opens vs Users) in {sel_year} Year and Quarter {sel_quarter}",
            hover_data=["RegisteredUser","AppOpens"],
            text="EngagementRatio"
            )
        fig4.update_traces(textposition="outside")
        st.plotly_chart(fig4, use_container_width=True)
        

    st.markdown("### User loyalty Index (App Opens vs Users)")
    fig3 = px.scatter(
        District_engagement,
        x="RegisteredUser",
        y="AppOpens",
        size="EngagementRatio",
        color="District",
        hover_name="District",
        title=f"District-wise user loyalty Index in in {sel_year} Year and Quarter {sel_quarter}"
    )
    st.plotly_chart(fig3, use_container_width=True)
    
    top5_dist = District_engagement.sort_values(by="EngagementRatio", ascending=False).head(5)
    bottom5_dist = District_engagement.sort_values(by="EngagementRatio", ascending=False).tail(5)
    col1,col2 = st.columns(2)
    with col1:
        fig3 = px.bar(
            top5_dist,
            x="District",
            y="EngagementRatio",
            color="District",
            title=f"Top 5 District by User loyalty Index (App Opens vs Users) in {sel_year} Year and Quarter {sel_quarter}",
            hover_data=["States","RegisteredUser","AppOpens"],
            text="EngagementRatio"
            )
        fig3.update_traces(textposition="outside")
        st.plotly_chart(fig3, use_container_width=True)
        
    with col2:
        fig4 = px.bar(
            bottom5_dist,
            x="District",
            y="EngagementRatio",
            color="District",
            title=f"Bottom 5 District by User loyalty Index (App Opens vs Users) in {sel_year} Year and Quarter {sel_quarter}",
            hover_data=["States","RegisteredUser","AppOpens"],
            text="EngagementRatio"
            )
        fig4.update_traces(textposition="outside")
        st.plotly_chart(fig4, use_container_width=True)
   
    # Brand Share (Aggre_user)
    brand_share = Aggre_user.groupby("Brand")["Transaction_count"].sum().reset_index()

    st.markdown("### Brand-wise User Engagement")
    fig4 = px.pie(
        brand_share,
        names="Brand",
        values="Transaction_count",
        title="Brand Contribution to Transactions"
    )
    st.plotly_chart(fig4, use_container_width=True)

    #Top Registered Users (State/District/Pincode)

    state = Map_user.groupby("States")[["RegisteredUser","AppOpens"]].sum().reset_index()
    top_state1 = state.sort_values(by="RegisteredUser", ascending=False).head(5)
    bottom_state1 = state.sort_values(by="RegisteredUser", ascending=True).head(5)
    top_state2 = state.sort_values(by="AppOpens", ascending=False).head(5)
    bottom_state2 = state.sort_values(by="AppOpens", ascending=True).head(5)

    dist = Map_user.groupby(["States","District"])[["RegisteredUser","AppOpens"]].sum().reset_index()
    top_dist1 = dist.sort_values(by="RegisteredUser", ascending=False).head(5)
    bottom_dist1 = dist.sort_values(by="RegisteredUser", ascending=True).head(5)
    top_dist2 = dist.sort_values(by="AppOpens", ascending=False).head(5)
    bottom_dist2 = dist.sort_values(by="AppOpens", ascending=True).head(5)

    pins = Top_user.groupby(["States","Pincodes"])["RegisteredUser"].sum().reset_index()
    top_pins1 = pins.sort_values(by="RegisteredUser", ascending=False).head(5)
    bottom_pins1 = pins.sort_values(by="RegisteredUser", ascending=True).head(5)
    
    view_option = st.selectbox(
        "Select View",
        ["State - wise", "District - wise", "Pincode - wise"]
    )

    if view_option == "State - wise":
        col1,col2 = st.columns(2)
        with col1:
            fig5 = px.bar(
                top_state1,
                x="States",
                y="RegisteredUser",
                text="RegisteredUser",
                title="Top 5 States with Highest Registered Users",
                hover_data=["States","RegisteredUser"]
            )
            fig5.update_xaxes(type="category") 
            st.plotly_chart(fig5, use_container_width=True)
            
            fig6 = px.bar(
                top_state2,
                x="States",
                y="AppOpens",
                text="AppOpens",
                title="Top 5 States with Highest AppOpens",
                hover_data=["States","AppOpens"]
            )
            fig6.update_xaxes(type="category") 
            st.plotly_chart(fig6, use_container_width=True)
            
        with col2:
            fig5 = px.bar(
                bottom_state1,
                x="States",
                y="RegisteredUser",
                text="RegisteredUser",
                title="Bottom 5 States with Highest Registered Users",
                hover_data=["States","RegisteredUser"]
            )
            fig5.update_xaxes(type="category") 
            st.plotly_chart(fig5, use_container_width=True)
            
            fig6 = px.bar(
                bottom_state2,
                x="States",
                y="AppOpens",
                text="AppOpens",
                title="Bottom 5 States with Highest AppOpens",
                hover_data=["States","AppOpens"]
            )
            fig6.update_xaxes(type="category") 
            st.plotly_chart(fig6, use_container_width=True)

    elif view_option == "District - wise":
        col1,col2 = st.columns(2)
        with col1:
            fig5 = px.bar(
                top_dist1,
                x="District",
                y="RegisteredUser",
                text="RegisteredUser",
                title="Top 5 Districts with Highest Registered Users",
                hover_data=["States","RegisteredUser"]
            )
            fig5.update_xaxes(type="category")
            st.plotly_chart(fig5, use_container_width=True)
            
            fig6 = px.bar(
                top_dist2,
                x="District",
                y="AppOpens",
                text="AppOpens",
                title="Top 5 District with Highest AppOpens",
                hover_data=["States","AppOpens"]
            )
            fig6.update_xaxes(type="category") 
            st.plotly_chart(fig6, use_container_width=True)
            
        with col2:
            fig5 = px.bar(
                bottom_dist1,
                x="District",
                y="RegisteredUser",
                text="RegisteredUser",
                title="bottom 5 Districts with Highest Registered Users",
                hover_data=["States","RegisteredUser"]
            )
            fig5.update_xaxes(type="category")
            st.plotly_chart(fig5, use_container_width=True)
            
            fig6 = px.bar(
                bottom_dist2,
                x="District",
                y="AppOpens",
                text="AppOpens",
                title="bottom 5 District with Highest AppOpens",
                hover_data=["States","AppOpens"]
            )
            fig6.update_xaxes(type="category") 
            st.plotly_chart(fig6, use_container_width=True)

    elif view_option == "Pincode - wise":
        col1,col2 = st.columns(2)
        with col1:
            fig5 = px.bar(
                top_pins1,
                x="Pincodes",
                y="RegisteredUser",
                text="RegisteredUser",
                title="Top 5 Pincodes with Highest Registered Users"
            )
            fig5.update_xaxes(type="category")
            st.plotly_chart(fig5, use_container_width=True)
            
            st.warning("App Open data for Pincode is Unavailable")
            
        with col2:
            fig5 = px.bar(
                bottom_pins1,
                x="Pincodes",
                y="RegisteredUser",
                text="RegisteredUser",
                title="Top 5 Pincodes with Highest Registered Users"
            )
            fig5.update_xaxes(type="category")
            st.plotly_chart(fig5, use_container_width=True)
            
            st.warning("App Open data for Pincode is Unavailable")
            
    # Transaction Insights (State / District / Pincode)

    st.markdown("### Top 10 Transaction Amount and Transaction Count")
    view_option = st.selectbox(
        "📊 Select Level of Analysis",
        ["State - wise", "District - wise", "Pincode - wise"]
    )

    # STATE-WISE
    if view_option == "State - wise":
        top_state = Top_district.groupby("States")[["Transaction_count","Transaction_amount"]].sum().reset_index()
        top_state = top_state.sort_values(by="Transaction_amount", ascending=False).head(5)

        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                top_state,
                x="States",
                y="Transaction_amount",
                text="Transaction_amount",
                title="Top 5 States Driving Transactions Amount"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                top_state,
                x="States",
                y="Transaction_count",
                text="Transaction_count",
                title="Top 5 States Driving Transaction Count"
            )
            st.plotly_chart(fig, use_container_width=True)

    # DISTRICT-WISE
    elif view_option == "District - wise":
        top_dist = Top_district.groupby(["States","District"])[["Transaction_count","Transaction_amount"]].sum().reset_index()
        top_dist = top_dist.sort_values(by="Transaction_amount", ascending=False).head(5)

        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                top_dist,
                x="District",
                y="Transaction_amount",
                text="Transaction_amount",
                title="Top 5 Districts Driving Transactions Amount",
                hover_data=["States","District","Transaction_amount"]
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                top_dist,
                x="District",
                y="Transaction_count",
                text="Transaction_count",
                title="Top 5 Districts Driving Transaction Count",
                hover_data=["States","District","Transaction_count"]
            )
            st.plotly_chart(fig, use_container_width=True)

    # PINCODE-WISE
    elif view_option == "Pincode - wise":
        top_pin = Top_transaction.groupby(["States","Pincodes"])[["Transaction_count","Transaction_amount"]].sum().reset_index()
        top_pin = top_pin.sort_values(by="Transaction_amount", ascending=False).head(5)

        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                top_pin,
                x="Pincodes",
                y="Transaction_amount",
                text="Transaction_amount",
                title="Top 5 Pincodes Driving Transactions Amount",
                hover_data=["States","Pincodes","Transaction_amount"]
            )
            fig.update_xaxes(type="category")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                top_pin,
                x="Pincodes",
                y="Transaction_count",
                text="Transaction_count",
                title="Top 5 Pincodes Driving Transaction Count",
                hover_data=["States","Pincodes","Transaction_count"]
            )
            fig.update_xaxes(type="category")
            st.plotly_chart(fig, use_container_width=True)

def map():
    st.title("MAP Visualization ")
    dataframes = {
        "Aggregate insurance": Aggre_insurance,
        "Aggregate transaction": Aggre_transaction,
        "Aggregate user": Aggre_user,
        "Map insurance": Map_insurance,
        "Map transaction": Map_transaction,
        "Map user": Map_user,
        "Top insurance": Top_insurance,
        "Top transaction": Top_transaction,
        "Top user": Top_user,
        "Top district": Top_district
    }

    df_choice = st.sidebar.selectbox("Choose the dataframe:", list(dataframes.keys()))
    df = dataframes[df_choice]


    exclude_cols = ["States", "District", "Pincodes", "Years", "Quarter"]
    columns = [col for col in df.columns if col not in exclude_cols]
    df_choice1 = st.sidebar.selectbox("Choose the column:", columns)

    col1,col2 = st.columns(2)
    with col1:
        years = sorted(df["Years"].unique())
        Year = st.selectbox("Choose Year:", ["All"] + years)
    with col2:
        if Year == "All":
            quarters = ["All"]
        else:
            quarters = sorted(df[df["Years"] == Year]["Quarter"].unique())
            quarters = ["All"] + quarters
        Quarter = st.selectbox("Choose Quarter:", quarters)

    filtered_df = df.copy()
    if Year != "All":
        filtered_df = filtered_df[filtered_df["Years"] == Year]
    if Quarter != "All":
        filtered_df = filtered_df[filtered_df["Quarter"] == Quarter]
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    geo_data = json.loads(requests.get(url).content)
   
    # Case 1: Transaction_type
    if df_choice1 == "Transaction_type":
        map_df = filtered_df.copy()

        most_used = (
            map_df.groupby(["States", "Transaction_type"], as_index=False)["Transaction_amount"].sum()
            .sort_values(["States", "Transaction_amount"], ascending=[True, False])
            .groupby("States").first().reset_index()
        )

        totals = map_df.groupby("States", as_index=False)[["Transaction_amount", "Transaction_count"]].sum()
        total_amt, total_cnt = totals["Transaction_amount"].sum(), totals["Transaction_count"].sum()

        totals["Transaction_Percentage"] = (totals["Transaction_amount"] / total_amt) * 100
        totals["Count_Percentage"] = (totals["Transaction_count"] / total_cnt) * 100
        totals["Transaction_type"] = most_used["Transaction_type"]

        fig = px.choropleth(
            totals,
            geojson=geo_data,
            locations="States",
            featureidkey="properties.ST_NM",
            color="Transaction_Percentage",
            hover_name="States",
            hover_data={
                "Transaction_amount": ":,.0f",
                "Transaction_count": ":,.0f",
                "Transaction_type": True,
                "Transaction_Percentage": ":.2f",
            },
            title=f"Most Used Transaction Type by Amount - {Year} {('Q'+str(Quarter)) if Quarter!='All' else ''}",
            fitbounds="locations",
            width=1200,
            height=900,
            color_continuous_scale="Sunsetdark"
        )
        fig.update_geos(visible=False)
        st.plotly_chart(fig, use_container_width=True)
        
        # Use Transaction_Percentage for ranking
        top5 = totals.nlargest(5, "Transaction_Percentage")
        bottom5 = totals.nsmallest(5, "Transaction_Percentage")

        col1, col2 = st.columns(2)
        with col1:
            plot_bar(   
                top5, 
                "States", 
                "Transaction_Percentage", 
                f"Top 5 States by Transaction Percentage ({Year}, Q{Quarter})"
            )
        with col2:    
            plot_bar(
                bottom5, 
                "States", 
                "Transaction_Percentage", 
                f"Bottom 5 States by Transaction Percentage ({Year}, Q{Quarter})"
            )
            
    # Case 2: Brand
    elif df_choice1 == "Brand":
        if filtered_df.empty:
            st.warning("No data available for the selected filters.")
        else:
            most_used = (
                filtered_df.sort_values(["States", "Transaction_Percentage"], ascending=[True, False])
                .groupby("States").first().reset_index()
            )
            most_used["Transaction_Percentage"] = most_used["Transaction_Percentage"].round(2)

            title_text = "Most Used Device in Each State"
            if Year != "All": title_text += f" - {Year}"
            if Quarter != "All": title_text += f" Q{Quarter}"

            fig = px.choropleth(
                most_used,
                geojson=geo_data,
                locations="States",
                featureidkey="properties.ST_NM",
                color="Brand",
                hover_name="States",
                hover_data={"Transaction_Percentage": True, "Brand": True,"Transaction_count" :True},
                title=title_text,
                fitbounds="locations",
                width=1200,
                height=900
            )
            fig.update_geos(visible=False)
            st.plotly_chart(fig, use_container_width=True)
            top5 = most_used.nlargest(5, "Transaction_Percentage")
            bottom5 = most_used.nsmallest(5, "Transaction_Percentage")

            col1, col2 = st.columns(2)
            with col1:
                plot_bar(   
                    top5, 
                    "States", 
                    "Transaction_Percentage", 
                    f"Top 5 States by Transaction Percentage ({Year}, Q{Quarter})"
                )
            with col2:    
                plot_bar(
                    bottom5, 
                    "States", 
                    "Transaction_Percentage", 
                    f"Bottom 5 States by Transaction Percentage ({Year}, Q{Quarter})"
                )
            df_grouped = filtered_df.groupby("States")[[df_choice1]].sum().reset_index()
    
    elif df_choice1 == "Transaction_Percentage":
        df_grouped = filtered_df.groupby(["Brand","States"])[[df_choice1]].sum().reset_index()

        fig = px.choropleth(
            df_grouped,
            geojson=geo_data,
            locations="States",
            featureidkey="properties.ST_NM",
            color=df_choice1,
            hover_name="States",
            title=f"{df_choice1} across States ({Year}, Q{Quarter})",
            fitbounds="locations",
            width=1200,
            height=900,
            color_continuous_scale="Sunsetdark"
        )
        fig.update_geos(visible=False)
        st.plotly_chart(fig, use_container_width=True)

        top5 = df_grouped.nlargest(5, df_choice1)
        bottom5 = df_grouped.nsmallest(5, df_choice1)
        col1,col2 = st.columns(2)
        with col1:
            plot_bar(top5,"States",f"{df_choice1}",f"{df_choice1} across States ({Year}, Q{Quarter})")
        with col2:    
            plot_bar(bottom5,"States",f"{df_choice1}",f"{df_choice1} across States ({Year}, Q{Quarter})")

    # Case 3: Other numeric columns
    else:
        df_grouped = filtered_df.groupby("States")[[df_choice1]].sum().reset_index()

        fig = px.choropleth(
            df_grouped,
            geojson=geo_data,
            locations="States",
            featureidkey="properties.ST_NM",
            color=df_choice1,
            hover_name="States",
            title=f"{df_choice1} across States ({Year}, Q{Quarter})",
            fitbounds="locations",
            width=1200,
            height=900,
            color_continuous_scale="Sunsetdark"
        )
        fig.update_geos(visible=False)
        st.plotly_chart(fig, use_container_width=True)

        top5 = df_grouped.nlargest(5, df_choice1)
        bottom5 = df_grouped.nsmallest(5, df_choice1)
        col1,col2 = st.columns(2)
        with col1:
            plot_bar(top5,"States",f"{df_choice1}",f"{df_choice1} across States ({Year}, Q{Quarter})")
        with col2:    
            plot_bar(bottom5,"States",f"{df_choice1}",f"{df_choice1} across States ({Year}, Q{Quarter})")

# STREAMLIT APPLICATION
st.set_page_config(page_title="PhonePe Data Visualization", page_icon="📊", layout="wide")

# --- Sidebar customization ---
st.markdown("""
    <style>
        /* Increase sidebar width */
        [data-testid="stSidebar"][aria-expanded="true"] {
            width: 340px;
        }
        [data-testid="stSidebar"][aria-expanded="false"] {
            width: 540px;
            margin-left: -340px;
        }

        /* Style the option menu text */
        .nav-link {
            font-size: 18px !important;
            color: #4B4B4B !important;
            padding: 10px 20px !important;
        }
        .nav-link:hover {
            background-color: #f0f2f6 !important;
            color: #1E88E5 !important;
        }
        .nav-link.active {
            background-color: #1E88E5 !important;
            color: white !important;
        }
    </style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown(
        """
        <div style="padding-bottom: 25px; text-align: center;">
            <img src="data:image/svg+xml;base64,PHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHg9IjAiIHk9IjAiIHZpZXdCb3g9IjAgMCAxMzIgNDgiIHhtbDpzcGFjZT0icHJlc2VydmUiPjxzdHlsZT4uc3Qwe2ZpbGw6IzVmMjU5Zn08L3N0eWxlPjxjaXJjbGUgdHJhbnNmb3JtPSJyb3RhdGUoLTc2LjcxNCAxNy44NyAyNC4wMDEpIiBjbGFzcz0ic3QwIiBjeD0iMTcuOSIgY3k9IjI0IiByPSIxNy45Ii8+PHBhdGggY2xhc3M9InN0MCIgZD0iTTkwLjUgMzQuMnYtNi41YzAtMS42LS42LTIuNC0yLjEtMi40LS42IDAtMS4zLjEtMS43LjJWMzVjMCAuMy0uMy42LS42LjZoLTIuM2MtLjMgMC0uNi0uMy0uNi0uNlYyMy45YzAtLjQuMy0uNy42LS44IDEuNS0uNSAzLS44IDQuNi0uOCAzLjYgMCA1LjYgMS45IDUuNiA1LjR2Ny40YzAgLjMtLjMuNi0uNi42SDkyYy0uOSAwLTEuNS0uNy0xLjUtMS41em05LTMuOWwtLjEuOWMwIDEuMi44IDEuOSAyLjEgMS45IDEgMCAxLjktLjMgMi45LS44LjEgMCAuMi0uMS4zLS4xLjIgMCAuMy4xLjQuMi4xLjEuMy40LjMuNC4yLjMuNC43LjQgMSAwIC41LS4zIDEtLjcgMS4yLTEuMS42LTIuNC45LTMuOC45LTEuNiAwLTIuOS0uNC0zLjktMS4yLTEtLjktMS42LTIuMS0xLjYtMy42di0zLjljMC0zLjEgMi01IDUuNC01IDMuMyAwIDUuMiAxLjggNS4yIDV2Mi40YzAgLjMtLjMuNi0uNi42aC02LjN6bS0uMS0yLjJIMTAzLjJ2LTFjMC0xLjItLjctMi0xLjktMnMtMS45LjctMS45IDJ2MXptMjUuNSAyLjJsLS4xLjljMCAxLjIuOCAxLjkgMi4xIDEuOSAxIDAgMS45LS4zIDIuOS0uOC4xIDAgLjItLjEuMy0uMS4yIDAgLjMuMS40LjIuMS4xLjMuNC4zLjQuMi4zLjQuNy40IDEgMCAuNS0uMyAxLS43IDEuMi0xLjEuNi0yLjQuOS0zLjguOS0xLjYgMC0yLjktLjQtMy45LTEuMi0xLS45LTEuNi0yLjEtMS42LTMuNnYtMy45YzAtMy4xIDItNSA1LjQtNSAzLjMgMCA1LjIgMS44IDUuMiA1djIuNGMwIC4zLS4zLjYtLjYuNmgtNi4zem0tLjEtMi4ySDEyOC42di0xYzAtMS4yLS43LTItMS45LTJzLTEuOS43LTEuOSAydjF6TTY2IDM1LjdoMS40Yy4zIDAgLjYtLjMuNi0uNnYtNy40YzAtMy40LTEuOC01LjQtNC44LTUuNC0uOSAwLTEuOS4yLTIuNS40VjE5YzAtLjgtLjctMS41LTEuNS0xLjVoLTEuNGMtLjMgMC0uNi4zLS42LjZ2MTdjMCAuMy4zLjYuNi42aDIuM2MuMyAwIC42LS4zLjYtLjZ2LTkuNGMuNS0uMiAxLjItLjMgMS43LS4zIDEuNSAwIDIuMS43IDIuMSAyLjR2Ni41Yy4xLjcuNyAxLjQgMS41IDEuNHptMTUuMS04LjRWMzFjMCAzLjEtMi4xIDUtNS42IDUtMy40IDAtNS42LTEuOS01LjYtNXYtMy43YzAtMy4xIDIuMS01IDUuNi01IDMuNSAwIDUuNiAxLjkgNS42IDV6bS0zLjUgMGMwLTEuMi0uNy0yLTItMnMtMiAuNy0yIDJWMzFjMCAxLjIuNyAxLjkgMiAxLjlzMi0uNyAyLTEuOXYtMy43em0tMjIuMy0xLjdjMCAzLjItMi40IDUuNC01LjYgNS40LS44IDAtMS41LS4xLTIuMi0uNHY0LjVjMCAuMy0uMy42LS42LjZoLTIuM2MtLjMgMC0uNi0uMy0uNi0uNlYxOS4yYzAtLjQuMy0uNy42LS44IDEuNS0uNSAzLS44IDQuNi0uOCAzLjYgMCA2LjEgMi4yIDYuMSA1LjZ2Mi40ek01MS43IDIzYzAtMS42LTEuMS0yLjQtMi42LTIuNC0uOSAwLTEuNS4zLTEuNS4zdjYuNmMuNi4zLjkuNCAxLjYuNCAxLjUgMCAyLjYtLjkgMi42LTIuNFYyM3ptNjguMiAyLjZjMCAzLjItMi40IDUuNC01LjYgNS40LS44IDAtMS41LS4xLTIuMi0uNHY0LjVjMCAuMy0uMy42LS42LjZoLTIuM2MtLjMgMC0uNi0uMy0uNi0uNlYxOS4yYzAtLjQuMy0uNy42LS44IDEuNS0uNSAzLS44IDQuNi0uOCAzLjYgMCA2LjEgMi4yIDYuMSA1LjZ2Mi40em0tMy42LTIuNmMwLTEuNi0xLjEtMi40LTIuNi0yLjQtLjkgMC0xLjUuMy0xLjUuM3Y2LjZjLjYuMy45LjQgMS42LjQgMS41IDAgMi42LS45IDIuNi0yLjRWMjN6Ii8+PHBhdGggZD0iTTI2IDE5LjNjMC0uNy0uNi0xLjMtMS4zLTEuM2gtMi40bC01LjUtNi4zYy0uNS0uNi0xLjMtLjgtMi4xLS42bC0xLjkuNmMtLjMuMS0uNC41LS4yLjdsNiA1LjdIOS41Yy0uMyAwLS41LjItLjUuNXYxYzAgLjcuNiAxLjMgMS4zIDEuM2gxLjR2NC44YzAgMy42IDEuOSA1LjcgNS4xIDUuNyAxIDAgMS44LS4xIDIuOC0uNXYzLjJjMCAuOS43IDEuNiAxLjYgMS42aDEuNGMuMyAwIC42LS4zLjYtLjZWMjAuOGgyLjNjLjMgMCAuNS0uMi41LS41di0xem0tNi40IDguNmMtLjYuMy0xLjQuNC0yIC40LTEuNiAwLTIuNC0uOC0yLjQtMi42di00LjhoNC40djd6IiBmaWxsPSIjZmZmIi8+PC9zdmc+",
            width="250">
        </div>
        """,
        unsafe_allow_html=True
    )

    select = option_menu(
        "Main Menu",
        ["Home", "Data Exploration", "Business Cases", "Map"],
        icons=["house", "bar-chart", "pie-chart", "map"],
        default_index=0
    )
    
# HOME PAGE
if select == "Home":
    st.markdown(
    "<h1 style='color:white;'> PHONEPE DATA VISUALIZATION DASHBOARD 📊</h1>",
    unsafe_allow_html=True
)
    states = ["All"] + sorted(Aggre_transaction["States"].unique())
    state_choice = st.sidebar.selectbox("Select State:", states, key="state_choice")

    if state_choice != "All":
        districts = ["All"] + sorted(
            Map_transaction[Map_transaction["States"] == state_choice]["District"].unique()
        )
    else:
        districts = ["All"]

    district_choice = st.sidebar.selectbox(
        "Select District:", districts, key="district_choice", disabled=(state_choice == "All")
    )

    if state_choice != "All" and district_choice != "All":
        pincodes = ["All"] + sorted(
            Map_transaction[(Map_transaction["States"] == state_choice) & 
                            (Map_transaction["District"] == district_choice)]["District"].unique()
        )
    elif state_choice != "All":
        pincodes = ["All"] + sorted(
            Top_transaction[Top_transaction["States"] == state_choice]["Pincodes"].unique()
        )
    else:
        pincodes = ["All"]  

    pincode_choice = st.sidebar.selectbox(
        "Select Pincode:", pincodes, key="pincode_choice", disabled=(state_choice == "All")
    )
    st.DataFrame(Aggre_user)
    def get_filtered_data(state, district, pincode):
        if state == "All" and district == "All" and pincode == "All":
            return Aggre_insurance, Aggre_transaction, Map_user

        elif state != "All" and district == "All" and pincode == "All":
            return (Map_insurance[Map_insurance["States"] == state],
                    Map_transaction[Map_transaction["States"] == state],
                    Map_user[Map_user["States"] == state])
        elif district != "All" and pincode == "All":
            return (Map_insurance[(Map_insurance["States"] == state) & (Map_insurance["District"] == district)],
                    Map_transaction[(Map_transaction["States"] == state) & (Map_transaction["District"] == district)],
                    Map_user[(Map_user["States"] == state) & (Map_user["District"] == district)])
        elif pincode != "All":
            ins = Top_insurance[
                (Top_insurance["States"] == state) & (Top_insurance["Pincodes"] == pincode)
            ]
            txn = Top_transaction[
                (Top_transaction["States"] == state) & (Top_transaction["Pincodes"] == pincode)
            ]
            if "Pincodes" in Top_user.columns:
                user = Top_user[
                    (Top_user["States"] == state) & 
                    (Top_user["Pincodes"].astype(str) == str(pincode))
                ]
            else:
                user = pd.DataFrame()
            return ins, txn, user
        else:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    filtered_insurance, filtered_transaction, filtered_user = get_filtered_data(
        state_choice, district_choice, pincode_choice
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        if filtered_insurance.empty:
            st.warning("⚠️ No data available for Insurance.")
        else:
            st.metric("💰 Insurance Amount", f"₹ {filtered_insurance['Transaction_amount'].sum():,.0f}")
            st.metric("🛡️ Insurance Count", f"{filtered_insurance['Transaction_count'].sum():,}")
    with col2:
        if filtered_transaction.empty:
            st.warning("⚠️ No data available for Transactions.")
        else:
            st.metric("💸 Transaction Amount", f"₹ {filtered_transaction['Transaction_amount'].sum():,.0f}")
            st.metric("📊 Transaction Count", f"{filtered_transaction['Transaction_count'].sum():,}")
    with col3:
        if filtered_user.empty:
            st.warning("⚠️ No user data available.")
        else:   
            if "RegisteredUser" in filtered_user.columns:
                st.metric("👥 Registered Users", f"{filtered_user['RegisteredUser'].sum():,}")
            else:
                st.warning("⚠️ RegisteredUser column missing.")

            if "AppOpens" in filtered_user.columns and filtered_user["AppOpens"].sum() > 0:
                st.metric("📱 App Opens", f"{filtered_user['AppOpens'].sum():,}")
            else:
                st.write("📱 App Opens")
                st.warning("⚠️ No AppOpens data available.")
    
    if state_choice == "All":
        tab1,tab2,tab3=st.columns(3)
        with tab1:
            insurance_state = Aggre_insurance.groupby("States")[["Transaction_count","Transaction_amount"]].sum().reset_index()
            fig = px.bar(insurance_state, x="States", y="Transaction_amount",
                        title="insurance Amount by State", text_auto=True)
            st.plotly_chart(fig, use_container_width=True)
            fig1 = px.bar(insurance_state, x="States", y="Transaction_count",
                    title="insurance count by State", text_auto=True)
            st.plotly_chart(fig1, use_container_width=True)
        with tab2:   
            trans_state = Aggre_transaction.groupby("States")[["Transaction_count","Transaction_amount"]].sum().reset_index()
            fig = px.bar(trans_state, x="States", y="Transaction_amount",
                        title="Transaction Amount by State", text_auto=True)
            st.plotly_chart(fig, use_container_width=True)
            fig1 = px.bar(trans_state, x="States", y="Transaction_count",
                        title="Transaction count by State", text_auto=True)
            st.plotly_chart(fig1, use_container_width=True)
        with tab3:
            user_state = Map_user.groupby("States")[["RegisteredUser","AppOpens"]].sum().reset_index()
            fig = px.bar(user_state, x="States", y="RegisteredUser",
                        title="Transaction Amount by State", text_auto=True)
            st.plotly_chart(fig, use_container_width=True)
            fig1 = px.bar(user_state, x="States", y="AppOpens",
                        title="Transaction count by State", text_auto=True)
            st.plotly_chart(fig1, use_container_width=True)

    elif district_choice == "All" and pincode_choice == "All":
        tab1,tab2,tab3 = st.columns(3)
        with tab1:
            trans_dist = Map_insurance[Map_insurance["States"] == state_choice].groupby("District")[["Transaction_count","Transaction_amount"]].sum().reset_index()
            fig = px.bar(trans_dist, x="District", y="Transaction_amount",
                        title=f"Insurance Amount in {state_choice} by District", text_auto=True)
            st.plotly_chart(fig, use_container_width=True)

            fig = px.bar(trans_dist, x="District", y="Transaction_count",
                        title=f"Insurance Count in {state_choice} by District", text_auto=True)
            st.plotly_chart(fig, use_container_width=True)
            
        with tab2:
            trans_dist = Map_transaction[Map_transaction["States"] == state_choice].groupby("District")[["Transaction_count","Transaction_amount"]].sum().reset_index()
            fig = px.bar(trans_dist, x="District", y="Transaction_amount",
                        title=f"Transaction Amount in {state_choice} by District", text_auto=True)
            st.plotly_chart(fig, use_container_width=True)
                
            fig = px.bar(trans_dist, x="District", y="Transaction_count",
                        title=f"Transaction Count in {state_choice} by District", text_auto=True)
            st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            trans_dist = Map_user[Map_user["States"] == state_choice].groupby("District")[["RegisteredUser","AppOpens"]].sum().reset_index()
            fig = px.bar(trans_dist, x="District", y="RegisteredUser",
                        title=f"Registered User in {state_choice} by District", text_auto=True)
            st.plotly_chart(fig, use_container_width=True)
                
            fig = px.bar(trans_dist, x="District", y="AppOpens",
                        title=f"App Opens in {state_choice} by District", text_auto=True)
            st.plotly_chart(fig, use_container_width=True)
    
    elif pincode_choice == "All" and district_choice != "All":
        tab1,tab2,tab3=st.columns(3)
        with tab1:
            trans_pin = Map_insurance[(Map_insurance["States"] == state_choice) &
                                        (Map_insurance["District"] == district_choice)].groupby("Years")[["Transaction_count","Transaction_amount"]].sum().reset_index()
            fig = px.bar(trans_pin, x="Years", y="Transaction_amount",
                        title=f"Insurance Amount in {district_choice} by Years", text_auto=True)
            fig.update_xaxes(type="category")
            st.plotly_chart(fig, use_container_width=True) 

            fig1 = px.bar(trans_pin, x="Years", y="Transaction_count",
                        title=f"insurance count in {district_choice} by Years", text_auto=True)
            fig1.update_xaxes(type="category")
            st.plotly_chart(fig1, use_container_width=True) 
        with tab2:
            trans_pin = Map_transaction[(Map_transaction["States"] == state_choice) &
                                        (Map_transaction["District"] == district_choice)].groupby("Years")[["Transaction_count","Transaction_amount"]].sum().reset_index()
            fig = px.bar(trans_pin, x="Years", y="Transaction_amount",
                        title=f"Transaction Amount in {district_choice} by Years", text_auto=True)
            fig.update_xaxes(type="category")
            st.plotly_chart(fig, use_container_width=True) 

            fig1 = px.bar(trans_pin, x="Years", y="Transaction_count",
                        title=f"Transaction count in {district_choice} by Years", text_auto=True)
            fig1.update_xaxes(type="category")
            st.plotly_chart(fig1, use_container_width=True) 
        with tab3:
            trans_pin = Map_user[(Map_user["States"] == state_choice) &
                                        (Map_user["District"] == district_choice)].groupby("Years")[["RegisteredUser","AppOpens"]].sum().reset_index()
            fig = px.bar(trans_pin, x="Years", y="RegisteredUser",
                        title=f"RegisteredUser in {district_choice} by Years", text_auto=True)
            fig.update_xaxes(type="category")
            st.plotly_chart(fig, use_container_width=True) 

            fig1 = px.bar(trans_pin, x="Years", y="AppOpens",
                        title=f"AppOpens in {district_choice} by Years", text_auto=True)
            fig1.update_xaxes(type="category")
            st.plotly_chart(fig1, use_container_width=True) 
        
    elif pincode_choice != "All" :
        tab1,tab2,tab3 = st.columns(3)
        with tab1:
            trans_pin = Top_insurance[
                (Top_insurance["States"] == state_choice) &
                (Top_insurance["Pincodes"] == pincode_choice)
            ].groupby("Years")[["Transaction_count", "Transaction_amount"]].sum().reset_index()

            fig = px.bar(trans_pin, x="Years", y="Transaction_amount",
                        title=f"Insurance Amount in {pincode_choice} by Years", text_auto=True)
            fig.update_xaxes(type="category")
            st.plotly_chart(fig, use_container_width=True)

            fig1 = px.bar(trans_pin, x="Years", y="Transaction_count",
                        title=f"Insurance Count in {pincode_choice} by Years", text_auto=True)
            fig1.update_xaxes(type="category")
            st.plotly_chart(fig1, use_container_width=True)

        with tab2:
            trans_pin = Top_transaction[
                (Top_transaction["States"] == state_choice) &
                (Top_transaction["Pincodes"] == pincode_choice)
            ].groupby("Years")[["Transaction_count", "Transaction_amount"]].sum().reset_index()

            fig = px.bar(trans_pin, x="Years", y="Transaction_amount",
                        title=f"Transaction Amount in {pincode_choice} by Years", text_auto=True)
            fig.update_xaxes(type="category")
            st.plotly_chart(fig, use_container_width=True)

            fig1 = px.bar(trans_pin, x="Years", y="Transaction_count",
                        title=f"Transaction Count in {pincode_choice} by Years", text_auto=True)
            fig1.update_xaxes(type="category")
            st.plotly_chart(fig1, use_container_width=True)

        with tab3:
            if "Pincodes" in Top_user.columns:
                user = Top_user[
                    (Top_user["States"] == state_choice) & 
                    (Top_user["Pincodes"].astype(str) == str(pincode_choice))
                ]
            else:
                user = pd.DataFrame()
            if not user.empty:
                user_summary = user.groupby("Years")["RegisteredUser"].sum().reset_index()
                fig = px.bar(
                    user_summary,
                    x="Years",
                    y="RegisteredUser",
                    text_auto=True,
                    title=f"Registered Users in {pincode_choice}, {state_choice} (by Years)"
                )
                fig.update_xaxes(type="category")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning(f"No user data available for {pincode_choice}, {state_choice}")
                
# DATA EXPLORATION PAGE
if select == "Data Exploration":
    st.title("Data Exploration")
    st.sidebar.markdown("### Choose the analysis type")
    analysis_type = st.sidebar.radio("Select Analysis Type",["Aggregated Analysis", "Map Analysis", "Top Analysis"],horizontal=False, label_visibility="collapsed")
    if analysis_type == "Aggregated Analysis":
        analysis_type1 = st.selectbox("Select Analysis Type", ["Aggregated Transaction", "Aggregated Insurance", "Aggregated User"])
        st.write("************************************************************")
        if analysis_type1 == "Aggregated Transaction":
            most_transaction_of_agg_transaction, fig1 = most_transaction(Aggre_transaction)
            tab1, tab2 = st.tabs(["Bar Graph", "Raw Data"])
            with tab1:
                st.plotly_chart(fig1)
            with tab2:
                st.dataframe(most_transaction_of_agg_transaction, hide_index=True)
            Aggre_india_map = Aggre_plot(Aggre_transaction)
            df, fig = plot_transaction_dynamics(Aggre_transaction)
            tab1, tab2 = st.tabs(["Bar Graph", "Raw Data"])
            with tab1:
                st.plotly_chart(fig)
            with tab2:
                st.dataframe(df,hide_index= True)
                      
        elif analysis_type1 == "Aggregated Insurance":            
            df, fig = most_transaction(Aggre_insurance)
            tab1, tab2 = st.tabs(["Bar Graph", "Raw Data"])
            with tab1:
                 st.plotly_chart(fig)
            with tab2:
                 st.dataframe(df, hide_index=True)                 
            Aggre_india_map = Aggre_plot(Aggre_insurance)            
            df, fig = plot_insurance_in_each_quarter(Aggre_insurance)
            tab1, tab2 = st.tabs(["Bar Graph", "Raw Data"])
            with tab1:
                st.plotly_chart(fig)
            with tab2:
                st.dataframe(df,hide_index= True)

        elif analysis_type1 == "Aggregated User":
            df, fig = user_brand_in_each_state(Aggre_user)
            tab1, tab2 = st.tabs(["Bar Graph", "Raw Data"])
            with tab1:
                st.plotly_chart(fig)    
            with tab2:
                st.dataframe(df, hide_index=True)
            st.write("*******************************************************************")            
            most_used_device_in_each_state_in_india_map(Aggre_user)
    
    elif analysis_type == "Map Analysis":
        analysis_type2 = st.selectbox("Select Analysis Type", ["Map Transaction", "Map Insurance", "Map User"])
        st.write("************************************************************")    
        if analysis_type2 == "Map Transaction":
            st.subheader("Transaction Amount and count in each quarter in States and year wise")
            map_bar_for_state_sum_for_each_quarter(Map_transaction)
            st.write("*****************************************************")
            st.subheader("Transaction Amount and count in each districts in States, year and quarter wise")
            map_bar(Map_transaction)
            st.write("*****************************************************")
            st.subheader("Transaction Amount and count in quarter for district, state and year wise")
            map_filter_by_state_and_district(Map_transaction)

        elif analysis_type2 == "Map Insurance":          
            st.subheader("Insurance Amount and count in each quarter in States and year wise")
            map_bar_for_state_sum_for_each_quarter(Map_insurance)
            st.write("*****************************************************")
            st.subheader("Insurance Amount and count in each districts in States, year and quarter wise")
            map_bar(Map_insurance)
            st.write("*****************************************************")
            st.subheader("Insurance Amount and count in quarter for district, state and year wise")
            map_filter_by_state_and_district(Map_insurance)
            
        elif analysis_type2 == "Map User":
            map_user_total_registered_user_and_app_open(Map_user)            
            map_use_registered_user_and_app_open(Map_user)            
            map_user_filter_by_state_and_district(Map_user)
            
    elif analysis_type == "Top Analysis":
        analysis_type3 = st.selectbox("Select Analysis Type", ["Top Transaction", "Top Insurance", "Top User"])
        st.write("************************************************************")
        if analysis_type3 == "Top Transaction":
            Top_count_amount(Top_transaction)
            Top_pie(Top_transaction)
            Top_filter_by_state_and_pincode(Top_transaction)
        
        elif analysis_type3 == "Top Insurance":
            Top_count_amount(Top_insurance)
            Top_pie(Top_insurance)
            Top_filter_by_state_and_pincode(Top_insurance)
            
        elif analysis_type3 == "Top User":
            Top_register_user(Top_user) 
            Top_use_pie(Top_user)
            Top_Registered_by_state_and_pincode(Top_user)


if select =="Business Cases":
    st.title("Business Cases")
    st.sidebar.markdown("### Business Cases 🔍")
    top_chart = st.sidebar.radio("Select Chart Type",["1. Decoding Transaction Dynamics on PhonePe",
                                 "2. Device Dominance and User Engagement Analysis",
                                 "3. Insurance Penetration and Growth Potential Analysis",
                                 "4. Transaction Analysis for Market Expansion",
                                 "5. User Engagement and Growth Strategy"],
                             label_visibility="collapsed"
                             )
    if top_chart =="1. Decoding Transaction Dynamics on PhonePe":
        st.markdown("""### 1. Decoding Transaction Dynamics on PhonePe
                    Purpose: Understand how transactions vary across states, quarters, and categories.  
    Goal: Identify growth trends vs. stagnation to guide region-specific business strategies.""")
        ques1(Aggre_transaction)
    
    elif top_chart =="2. Device Dominance and User Engagement Analysis":
        st.markdown("""### 2. Device Dominance and User Engagement Analysis
                    Purpose: Analyze how users engage with the app across different mobile device brands.  
    Goal: Detect underperforming devices or brands despite high registrations to optimize app performance.""")
        ques2(Aggre_user, Map_user)
    
    elif top_chart =="3. Insurance Penetration and Growth Potential Analysis":
        st.markdown("""### 3. Insurance Penetration and Growth Potential Analysis
                    Purpose: Examine how insurance services are used across states.  
    Goal: Find untapped markets and high-potential states to expand insurance offerings.""")
        ques3(Aggre_insurance, Map_insurance,Top_insurance,Top_user, Map_user)
    
    elif top_chart =="4. Transaction Analysis for Market Expansion":
        st.markdown("""### 4. Transaction Analysis for Market Expansion
                    Purpose: Evaluate state-wise transaction patterns to uncover growth opportunities.  
    Goal: Support strategic expansion and resource allocation in high-performing or emerging regions.""")
        ques4(Aggre_transaction,Map_user)
    
    elif top_chart =="5. User Engagement and Growth Strategy":
        st.markdown("""### 5. User Engagement and Growth Strategy]
                    Purpose: Study app opens and user activity across districts and states.  
    Goal: Enhance engagement strategies and boost adoption where user activity is low.""")
        ques5(Aggre_user, Map_user, Top_user, Top_district,Top_transaction)
        
        
if select == "Map":
    map()









