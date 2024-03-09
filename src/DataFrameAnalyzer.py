import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import shapely.geometry
from sklearn.cluster import KMeans
import plotly.graph_objects as go
import plotly.express as px
from connections import postgresql_conn

class DataFrameAnalyzer:
    """
    A utility class for analyzing and visualizing data in a pandas DataFrame.

    Attributes:
    - dataframe (pandas.DataFrame): The input DataFrame for analysis.
    - gender (str): A gender identifier for the analysis.

    Methods:
    - summary_statistics(): Generate summary statistics for the DataFrame.
    - groupby_sum(column): Perform a groupby operation and calculate sum of groups.
    - plot_histogram(column): Generate a histogram plot for a specified column.
    - plot_bar_chart(x_column, y_column, max_label_length): Generate a bar chart plot with truncated labels.
    - elbow_method(): Perform the Elbow Method for optimal cluster determination.
    """
    def __init__(self, dataframe, gender=''):
        """
        Initialize a DataFrameAnalyzer instance.
        :param dataframe:dataframe (pandas.DataFrame): The input DataFrame for analysis.
        :param gender:gender (str, optional): A gender identifier for the analysis. Default is an empty string.
        """
        self.dataframe = dataframe
        self.gender = gender

    def summary_statistics(self):
        """
        Generate summary statistics for the DataFrame.
        :return:
        pandas.DataFrame: A DataFrame containing summary statistics of the input data.
        """
        return self.dataframe.describe()

    def groupby_sum(self, column):
        """
        Perform a groupby operation on a specified column and calculate the sum of each group.
        :param column: The column name for the groupby operation.
        :return:
        DataFrame showing sum of values for each group.
        """
        return self.dataframe.groupby(column).sum()

    def plot_histogram(self, column):
        """
        Generate a histogram plot for a specified column.
        :param column: The column name for which the histogram will be plotted.
        """
        self.dataframe[column].plot(kind='hist')
        plt.xlabel(column)
        plt.ylabel('Frequency')
        plt.title(f'Histogram of {column}')
        plt.show()



    def plot_bar_chart(self, x_column, y_column, max_label_length=10):
        """
        Generate a bar chart plot with truncated labels.
        :param x_column: The column name for the x-axis of the bar chart.
        :param y_column: The column name for the y-axis of the bar chart.
        :param max_label_length: Maximum length for truncated labels, defaults to 10.
        """
        # Truncate the team names to the first max_label_length characters
        self.dataframe['truncated_team'] = self.dataframe['team'].str[:max_label_length]

        # Create the bar chart using the truncated team names
        self.dataframe.plot(x='truncated_team', y=y_column, kind='bar', rot=0)
        plt.xlabel(x_column)
        plt.ylabel(y_column)
        plt.title(f'Bar Chart: {y_column} vs. Truncated {x_column}')
        plt.show()

        # Drop the temporary column after plotting
        self.dataframe.drop(columns=['truncated_team'], inplace=True)
    def elbow_method(self):
        """
        Perform the Elbow Method for determining the optimal number of clusters using KMeans algorithm.
        Plots the within-cluster sum of squares (CS) for different numbers of clusters.
        """
        cs = []
        for i in range(1, 11):
            kmeans = KMeans(n_clusters=i, init='k-means++', max_iter=300, n_init=10, random_state=0)
            kmeans.fit(self.dataframe)
            cs.append(kmeans.inertia_)
        plt.plot(range(1, 11), cs)
        plt.title('The Elbow Method')
        plt.xlabel('Number of clusters')
        plt.ylabel('CS')
        plt.show()



    @staticmethod
    def pattern_rader_plotly(attributes, values, player1, player2):
        """
        :param attributes: the columns of the dataframe that we want to use for the comperession
        :param values: values is a list that the first value is the list of values of the first player and the
        secound value in the list is for the secund player.
        the function create the radar plot
        """

        fig = go.Figure()

        fig.add_trace(
            go.Scatterpolar(
                r=values[0],
                theta=attributes,
                fill="toself",
                name="Player 1:"+str(player1)+" -",
                line= dict(color='red')
            )
        )
        fig.add_trace(
            go.Scatterpolar(
                r=values[1], theta=attributes, fill="toself", name="Player 2:"+str(player2)+" -"
            )
        )

        fig.update_layout(
            title= str(player1)+'-'+str(player2),
            polar=dict(radialaxis=dict(visible=True, range=[0, 5])),
            # showlegend=False
        )

        # get data back out of figure
        df = pd.concat(
            [
                pd.DataFrame({"r": t.r, "theta": t.theta, "trace": np.full(len(t.r), t.name)})
                for t in fig.data
            ]
        )
        # convert theta to be in radians
        df["theta_n"] = pd.factorize(df["theta"])[0]
        df["theta_radian"] = (df["theta_n"] / (df["theta_n"].max() + 1)) * 2 * np.pi
        # work out x,y co-ordinates
        df["x"] = np.cos(df["theta_radian"]) * df["r"]
        df["y"] = np.sin(df["theta_radian"]) * df["r"]

        # now generate a polygon from co-ordinates using shapely
        # then it's a simple case of getting the area of the polygon
        df_a = df.groupby("trace").apply(
            lambda d: shapely.geometry.MultiPoint(list(zip(d["x"], d["y"]))).convex_hull.area
        )

        # let's use the areas in the name of the traces
        fig.for_each_trace(lambda t: t.update(name=f"{t.name} {df_a.loc[t.name]:.1f}"))

        return fig




    def create_radar_chart_plotly(self, id_player_1, id_player_2):
        """
        :param row_index1: the index of the first player we want to compare
        :param row_index2: the index of the second player we want to compare
        we create two plots, for the performance columns type and for the per 90 columns type
        """
        params = {
            "host": "localhost",
            "dbname": "postgres",
            "user": "postgres",
            "password": "1234"
        }
        db = postgresql_conn(params)
        name_player1 = list(self.dataframe[self.dataframe['id'] == id_player_1]['player'])[0]
        name_player2 = list(self.dataframe[self.dataframe['id'] == id_player_2]['player'])[0]
        dataframes_players = db.execute_query([
            "select * from players_performance",
            "select * from players_per_90_minutes",
            "select * from players_expected ",
            "select * from Players_playing_time"
        ])

        attribute_types = {
            'performance': 'Performance',
            'per_90': 'Per 90',
            'progression': 'Progression',
            'expected': 'Expected'
        }
        figs_list = []
        for df_player in dataframes_players:
            relevant_attributes_player_1 = df_player[df_player['id'] == id_player_1].iloc[:, 3:]
            df_values_player_1 = relevant_attributes_player_1.values[0]
            df_values_player_1 = np.nan_to_num(df_values_player_1, nan=0.0)
            relevant_attributes_player_2 = df_player[df_player['id'] == id_player_2].iloc[:, 3:]
            df_values_player_2 = relevant_attributes_player_2.values[0]
            df_values_player_2 = np.nan_to_num(df_values_player_2, nan=0.0)
            figs_list.append(self.pattern_rader_plotly(relevant_attributes_player_1.columns, [df_values_player_1, df_values_player_2], name_player1, name_player2))

        return figs_list



if __name__ == '__main__':
    params = {
        "host": "localhost",
        "dbname": "postgres",
        "user": "postgres",
        "password": "1234"
    }
    db = postgresql_conn(params)
    df = db.execute_query(['select * from players_info'])[0]
    analyzer_male = DataFrameAnalyzer(df, 'male')
    res = analyzer_male.create_radar_chart_plotly('b7b99c6e07a7fe4c', 'defa5ab54986ba65')
    for fig in res:
        fig.show()

    # data = {'Category': ['A', 'B', 'C', 'D', 'E'],
    #         'Value1': [5, 3, 8, 2, 6],
    #         'Value2': [4, 6, 3, 7, 2]}
    # df = pd.DataFrame(data)
    #
    # # Create a radar chart for the first row
    # row_index = 0
    # DFA = DataFrameAnalyzer(df)
    # fig = DFA.create_radar_chart(row_index)
    #
    # # Display the chart
    # plt.show()
    # female_data = pd.read_csv('Data_files/female.csv')
    # analyzer_female = DataFrameAnalyzer(female_data, 'female')
    # analyzer_female.create_radar_chart_plotly(row_index1=28, row_index2=29)


