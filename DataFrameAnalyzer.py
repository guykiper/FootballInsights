import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import shapely.geometry

import plotly.graph_objects as go

class DataFrameAnalyzer:
    def __init__(self, dataframe, gender):
        self.dataframe = dataframe
        self.gender = gender

    def summary_statistics(self):
        return self.dataframe.describe()

    def groupby_sum(self, column):
        return self.dataframe.groupby(column).sum()

    def plot_histogram(self, column):
        self.dataframe[column].plot(kind='hist')
        plt.xlabel(column)
        plt.ylabel('Frequency')
        plt.title(f'Histogram of {column}')
        plt.show()

    def plot_bar_chart(self, x_column, y_column, max_label_length=10):
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
                r=values[0], theta=attributes, fill="toself", name="Player 1:"+str(player1)+" -"
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

        fig.show()


    def create_radar_chart_plotly(self, row_index1, row_index2):
        """
        :param row_index1: the index of the first player we want to compare
        :param row_index2: the index of the second player we want to compare
        we create two plots, for the performance columns type and for the per 90 columns type
        """
        name_player1 = self.dataframe.loc[row_index1, 'Player']
        name_player2 = self.dataframe.loc[row_index2, 'Player']

        attribute_types = {
            'performance': 'Performance',
            'per_90': 'Per 90',
            'progression': 'Progression',
            'expected': 'Expected'
        }

        for attribute_type, attribute_prefix in attribute_types.items():
            attributes = [col for col in self.dataframe.columns[5:-2] if col.startswith(attribute_prefix)]
            if not attributes:
                continue  # Skip if there are no attributes of this type

            player1_values = list(self.dataframe.loc[row_index1, attributes])
            player2_values = list(self.dataframe.loc[row_index2, attributes])

            self.pattern_rader_plotly(attributes, [player1_values, player2_values], name_player1, name_player2)


#
male_data = pd.read_csv('male.csv')
analyzer_male = DataFrameAnalyzer(male_data, 'male')
analyzer_male.create_radar_chart_plotly(row_index1=8, row_index2=2)
#
# female_data = pd.read_csv('female.csv')
# analyzer_female = DataFrameAnalyzer(female_data, 'female')
# analyzer_female.create_radar_chart_plotly(row_index1=28, row_index2=29)

