import streamlit as st
import pandas as pd
import plotly.express as px
from connections import postgresql_conn, Elasticsearch_conn
import json
from DataFrameAnalyzer import DataFrameAnalyzer
# Cache dataframes to avoid reloading
@st.cache_data
def load_data():
    df = pd.read_csv("Data_files/csv files/players.csv")
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")

    return df


df = load_data()
page = st.sidebar.radio("Page", ["Main", "Dashboard", "Queries", "Player comparison"])
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
#
# def queries_page():
#     QUERY_HISTORY_FILE = "Data_files/streamlit_app/query_history.json"
#
#     params = {
#         "host": "localhost",
#         "dbname": "postgres",
#         "user": "postgres",
#         "password": "1234"
#     }
#     db = postgresql_conn(params)
#     es = Elasticsearch_conn()
#     def load_query_history():
#         try:
#             with open(QUERY_HISTORY_FILE, "r") as f:
#                 return json.load(f)
#         except FileNotFoundError:
#             return []
#
#     def save_query_history(query_history):
#         with open(QUERY_HISTORY_FILE, "w") as f:
#             json.dump(query_history, f)
#
#
#     query_history = load_query_history()
#     st.header("Queries")
#
#     query_type = st.radio("Query Type", ["PostgreSQL", "Elasticsearch"])
#
#     # Display ERD image
#     erd_image = "Data_files/ERD.drawio.png"
#     st.image(erd_image, width=800)
#
#
#
#     if query_type == "PostgreSQL":
#
#         if st.button("Check PostgreSQL Connection"):
#             connection_status = db.check_connection()
#             st.write(connection_status)
#         try:
#             st.subheader("Enter the Query")
#             query = st.text_area("", height=200)
#             query_history.append(query)
#             save_query_history(query_history)
#             results = db.execute_query([query])
#         except Exception as e:
#             st.error("Query failed: " + str(e))
#         else:
#             st.write(results[0])
#
#     elif query_type == "Elasticsearch":
#
#         if st.button("Check Elasticsearch Connection"):
#             connection_status = es.check_connection()
#             st.write(connection_status)
#
#         if st.button("Start Elasticsearch"):
#             es.start_elasticsearch()
#             st.success("Elasticsearch started successfully!")
#
#         indices_list = es.get_indices()
#         # Display selectbox with indices
#         st.subheader("Index")
#         index = st.selectbox("Select Index", indices_list)
#         # index = st.text_input("Enter index name")
#         st.subheader("Enter the Query")
#         query = st.text_area("", height=200)
#         query = json.loads(query)
#         query_history.append(query)
#         save_query_history(query_history)
#         results = es.query_data(index, query)
#         st.write(results)
#
#     if query_history:
#         st.subheader("Past Queries:")
#         for q in query_history:
#             st.code(q)
#
#     if st.button("Clear History"):
#         query_history = []
#         save_query_history(query_history)

import json

def queries_page():
    QUERY_HISTORY_FILE = "Data_files/streamlit_app/query_history.json"
    params = {
        "host": "localhost",
        "dbname": "postgres",
        "user": "postgres",
        "password": "1234"
    }
    db = postgresql_conn(params)
    es = Elasticsearch_conn()

    def load_query_history():
        with open(QUERY_HISTORY_FILE, "r") as f:
            return json.load(f)

    def save_query_history(query_history):
        with open(QUERY_HISTORY_FILE, "w") as f:
            json.dump(query_history, f)

    query_history = load_query_history()
    st.header("Queries")
    query_type = st.radio("Query Type", ["PostgreSQL", "Elasticsearch"])

    # Display ERD image
    erd_image = "Data_files/ERD.drawio.png"
    st.image(erd_image, width=800)

    if query_type == "PostgreSQL":
        if st.button("Check PostgreSQL Connection"):
            connection_status = db.check_connection()
            st.write(connection_status)
        try:
            st.subheader("Enter the Query")
            query = st.text_area("", height=200)
            query_history.append(query)
            save_query_history(query_history)
            results = db.execute_query([query])
        except Exception as e:
            st.error("Query failed: " + str(e))
        else:
            st.write(results[0])
    elif query_type == "Elasticsearch":
        if st.button("Check Elasticsearch Connection"):
            connection_status = es.check_connection()
            st.write(connection_status)
        if st.button("Start Elasticsearch"):
            es.start_elasticsearch()
            st.success("Elasticsearch started successfully!")
        indices_list = es.get_indices()
        # Display selectbox with indices
        st.subheader("Index")
        index = st.selectbox("Select Index", indices_list)
        # index = st.text_input("Enter index name")
        st.subheader("Enter the Query")
        query = st.text_area("", height=200)
        if query.strip():  # Check if the query string is not empty
            query = json.loads(query)
            query_history.append(query)
            save_query_history(query_history)
            results = es.query_data(index, query)
            st.write(results)
        else:
            st.warning("Query cannot be empty.")

    if query_history:
        st.subheader("Past Queries:")
        for q in query_history:
            st.code(q)

    if st.button("Clear History"):
        query_history = []
        save_query_history(query_history)
def player_comparison_page():

    params = {
        "host": "localhost",
        "dbname": "postgres",
        "user": "postgres",
        "password": "1234"
    }
    db = postgresql_conn(params)
    player_data = db.execute_query(["select id, player from players_info"])

    # Check if the player_data is not empty
    if not player_data or not player_data[0].values.tolist():
        st.write("No players found in the database.")
        return

    # Create a dictionary mapping player names to their IDs
    player_dict = {row['player']: str(row['id']) for _, row in player_data[0].iterrows()}

    # Convert DataFrame to a list of player names
    list_players_name = list(player_dict.keys())

    player_1_name = st.multiselect("Choose player 1", list_players_name)
    player_2_name = st.multiselect("Choose player 2", list_players_name)

    # Retrieve the IDs for the selected player names
    player_1_id = []
    if player_1_name:
        player_1_id = [player_dict[name] for name in player_1_name]
    else:
        st.write("Player 1 is not selected.")
    player_2_id = []
    if player_2_name:
        player_2_id = [player_dict[name] for name in player_2_name]
    else:
        st.write("Player 2 is not selected.")

    if player_1_id and player_2_id:
        params = {
            "host": "localhost",
            "dbname": "postgres",
            "user": "postgres",
            "password": "1234"
        }
        db = postgresql_conn(params)
        df = db.execute_query(['select * from players_info'])[0]
        dfa = DataFrameAnalyzer(df, 'male')
        charts = dfa.create_radar_chart_plotly(player_1_id[0], player_2_id[0])

        # Display the charts one after another
        st.plotly_chart(charts[0], use_container_width=True)
        st.plotly_chart(charts[1], use_container_width=True)
        st.plotly_chart(charts[2], use_container_width=True)
        st.plotly_chart(charts[3], use_container_width=True)

        # # Create a 2x2 grid layout
        # col1, col2 = st.columns(2)
        # col3, col4 = st.columns(2)
        #
        # # Display the figures in the grid cells
        # with col1:
        #     st.plotly_chart(charts[0], use_container_width=True)
        #
        # with col2:
        #     st.plotly_chart(charts[1], use_container_width=True)
        #
        # with col3:
        #     st.plotly_chart(charts[2], use_container_width=True)
        #
        # with col4:
        #     st.plotly_chart(charts[3], use_container_width=True)
    else:
        st.write("Please select both players to display the charts.")


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
elif page == "Queries" :
    queries_page()
elif page == "Player comparison":
    player_comparison_page()




