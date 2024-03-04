import streamlit as st
import pandas as pd
import plotly.express as px
from connections import postgresql_conn
import json

# Cache dataframes to avoid reloading
@st.cache_data
def load_data():
    df = pd.read_csv("Data_files/csv files/players.csv")
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")

    return df


df = load_data()
page = st.sidebar.radio("Page", ["Main", "Dashboard", "Queries"])
# Store unique values in global variables to avoid recalculation


@st.cache_data
def filter_data(df, selected_pos, selected_nation, sort_order, selected_column):
    filtered_df = df[(df['Pos'] == selected_pos) &
                     (df['Nation'] == selected_nation)]

    ascending = sort_order == "asc"
    filtered_df = filtered_df.sort_values(
        by=[selected_column],
        ascending=ascending)

    return filtered_df


def main_page():
    POSITIONS = df['Pos'].unique()
    NATIONS = df['Nation'].unique()
    COLUMNS = df.columns.tolist()

    selected_pos = st.sidebar.selectbox("Position", POSITIONS)
    selected_nation = st.sidebar.selectbox("Nation", NATIONS)
    selected_column = st.sidebar.selectbox("Column", COLUMNS)
    sort_options = ["asc", "desc"]
    sort_order = st.sidebar.selectbox("Sort order", sort_options)
    df_filtered = filter_data(df, selected_pos, selected_nation, sort_order, selected_column)
    st.header("Main Page")
    st.subheader("Raw Data")
    st.write(df.head())
    st.subheader("Filtered Data")
    st.write(df_filtered)

@st.cache_resource
def plot_charts():
    fig1 = px.bar(df, x='Player', y='Performance - Gls', title='Goals per Player (Full Data)')
    fig2 = px.scatter(df, x='Age', y='Playing Time - Min', color='Pos', title='Age vs Playing Time (Full Data)')
    counts = df.groupby("country_full_name").size().reset_index(name="counts")
    fig3 = px.choropleth(
        counts,
        locations="country_full_name",
        locationmode="country names",
        color="counts",
        hover_data=["country_full_name", "counts"]
    )

    return fig1, fig2, fig3


def queries_page():
    QUERY_HISTORY_FILE = "query_history.json"

    def load_query_history():
        try:
            with open(QUERY_HISTORY_FILE, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return []

    def save_query_history(query_history):
        with open(QUERY_HISTORY_FILE, "w") as f:
            json.dump(query_history, f)

    query_history = load_query_history()

    st.header("Queries")

    # Display ERD image
    erd_image = "Data_files/ERD.drawio.png"
    st.image(erd_image, width=800)

    # Query input box
    query = st.text_area("Enter a SQL query", height=200)


    if query:

        query_history.append(query)
        save_query_history(query_history)
        try:
            params = {
                "host": "localhost",
                "dbname": "postgres",
                "user": "postgres",
                "password": "1234"
            }
            db = postgresql_conn(params)
            db.connect()
            results = db.execute_query([query])
        except Exception as e:
            st.error("Query failed: " + str(e))
        else:
            st.write("Results:", results[0])

    if query_history:
        st.subheader("Past Queries:")
        for q in query_history:
            st.code(q)

    if st.button("Clear History"):
        query_history = []
        save_query_history(query_history)


def dashboard_page():
    st.header("Dashboard")
    fig1, fig2, fig3 = plot_charts()
    # st.plotly_chart(fig1)
    # st.plotly_chart(fig2)
    st.subheader("Players by Nation")
    st.plotly_chart(fig3)




if page == "Main":
    main_page()
elif page == "Dashboard":
    dashboard_page()
else:
    queries_page()




