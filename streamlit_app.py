import streamlit as st
import pandas as pd
import plotly.express as px


# Cache dataframes to avoid reloading
@st.cache_data
def load_data():
    df = pd.read_csv("Data_files/csv files/male.csv")
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")

    return df


df = load_data()
page = st.sidebar.radio("Page", ["Main", "Dashboard"])
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
    counts = df.groupby("Nation").size().reset_index(name="counts")
    fig3 = px.choropleth(
        counts,
        locations="Nation",
        locationmode="country names",
        color="counts",
        hover_data=["Nation", "counts"]
    )

    return fig1, fig2, fig3


def dashboard_page():
    st.header("Dashboard")
    fig1, fig2, fig3 = plot_charts()
    # st.plotly_chart(fig1)
    # st.plotly_chart(fig2)
    st.plotly_chart(fig3)


if page == "Main":
    main_page()
else:
    dashboard_page()




