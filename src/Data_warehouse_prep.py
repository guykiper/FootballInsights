import pandas as pd
from connections import postgresql_conn, Elasticsearch_conn
from DataProcessor import Dataprocessor_transform
import re
from DataProcessor import Dataprocessor_transform

"""
This create_tables class handles reading in the raw data CSV files 
and transforming them into analytical tables for the football dataset.

Key capabilities:

- Joins the player data with the club data to add club codes
- Subsets columns to create focused analytical tables  
   (performance, playing time etc.)
- Deduplicates nation location data
- Provides helper functions like get_club_code to assist 
  with data transformations

The goal is to take the raw CSV files and output cleaned, 
structured data tables for modeling and analysis.

"""


class create_tables:

    def __init__(self, df_male, df_female, DW_conn='postgresql'):
        """
        Constructor to load all the raw CSV files needed for processing.
        """
        self.table_dict = {}
        self.data_male = Dataprocessor_transform(df_male, 'male')
        self.data_female = Dataprocessor_transform(df_female, 'female')
        self.file_location_info_male = self.data_male.df_location
        self.file_location_info_female = self.data_female.df_location
        self.concat_gender = pd.concat([self.data_male.df, self.data_female.df])
        self.file_teams = pd.read_csv('../Data_files/csv files/backup_new.csv')
        self.club_and_code = self.create_teams_table()[['club_name', 'new_club_name', 'club_code']]
        self.file_players = self.concat_gender.merge(self.club_and_code, how='left', left_on='team_new_name', right_on='new_club_name')
        # self.club_code = self.get_club_code()
        self.DW_conn = DW_conn
        pass

    @staticmethod
    def sanitize_column_name(name):
        # Remove special characters, replace spaces with underscores, and make lowercase
        sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '', name)
        sanitized_name = re.sub(r'([a-z])([A-Z])', r'\1_\2', sanitized_name)
        sanitized_name = sanitized_name.lower()
        return sanitized_name

    @staticmethod
    def club_name_transform(df):
        new_club_name = []
        for gender, club_name in list(zip(df['gender'], df['club_name'])):
            last_value = club_name.split('-')[-1]
            if gender == 'Male':
                new_club_name.append(club_name + '-male')
            else:

                if last_value == 'Women':
                    list_name = club_name.split('-')
                    list_name[-1] = 'female'
                    female_name = '-'.join(list_name)
                    new_club_name.append(female_name)
                else:
                    new_club_name.append(club_name + '-female')
        df['new_club_name'] = new_club_name
        return df

    def get_club_code(self):
        """
        Joins club codes to the player table
        """
        mask = self.club_and_code['new_club_name'].str.startswith('Guadalajara')

        # Delete those rows
        self.club_and_code = self.club_and_code.loc[~mask]

        merged = self.file_players.merge(self.club_and_code, how='left', left_on='team', right_on='new_club_name')
        return list(merged['club_code'])

    def create_teams_table(self):
        """Cleans the Teams table"""
        new_file_teams = self.club_name_transform(self.file_teams)
        teams_table = new_file_teams[['club_code', 'club_name', 'new_club_name', 'gender']]
        teams_table['country'] = [country.split()[0] for country in new_file_teams['country']]
        # teams_table.loc[:, 'country'] = [c.split()[0] for c in new_file_teams['country']]
        self.table_dict['teams'] = teams_table
        teams_table.columns = [self.sanitize_column_name(col) for col in teams_table.columns]
        return teams_table
        # return teams_table

    def create_nation_table(self):
        """Deduplicates nation info"""
        nation_table = pd.concat([self.file_location_info_male, self.file_location_info_female])
        nation_table = nation_table.drop_duplicates(subset=['country_code'])
        nation_table = nation_table[['country_code', 'country_name', 'latitude', 'longitude']]
        nation_table.columns = [self.sanitize_column_name(col) for col in nation_table.columns]
        self.table_dict['nations'] = nation_table
        # return nation_table

    def create_players_performance_table(self):
        """Creates a focused performance table"""
        perf_cols = ['ID', 'season', 'club_code']
        perf_cols += [col for col in self.file_players.columns if col.startswith("Performance")]
        players_performance_table = self.file_players[perf_cols]
        # players_performance_table['club_code'] = self.get_club_code()
        players_performance_table.columns = [self.sanitize_column_name(col) for col in
                                             players_performance_table.columns]
        self.table_dict['players_performance'] = players_performance_table
        # return players_performance_table

    def create_per_90_minutes_table(self):
        """Creates a focused Per 90 stats table"""
        perf_cols = ['ID', 'season', 'club_code']
        perf_cols += [col for col in self.file_players.columns if col.startswith("Per 90")]
        per_90_minutes_table = self.file_players[perf_cols]
        # per_90_minutes_table['club_code'] = self.get_club_code()
        per_90_minutes_table.columns = [self.sanitize_column_name(col) for col in per_90_minutes_table.columns]
        self.table_dict['players_per_90_minutes'] = per_90_minutes_table
        # return per_90_minutes_table

    def create_player_expected_table(self):
        """Creates a focused expected stats table"""
        perf_cols = ['ID', 'season', 'club_code']
        perf_cols += [col for col in self.file_players.columns if col.startswith("Expected")]
        player_expected_table = self.file_players[perf_cols]
        # player_expected_table['club_code'] = self.get_club_code()
        player_expected_table.columns = [self.sanitize_column_name(col) for col in player_expected_table.columns]
        self.table_dict['players_expected'] = player_expected_table
        # return player_expected_table

    def create_player_playing_time_table(self):
        """Creates a focused playing time stats table"""
        perf_cols = ['ID', 'season', 'club_code']
        perf_cols += [col for col in self.file_players.columns if col.startswith("Playing Time")]
        player_playing_time_table = self.file_players[perf_cols]
        # player_playing_time_table['club_code'] = self.get_club_code()
        player_playing_time_table.columns = [self.sanitize_column_name(col) for col in
                                             player_playing_time_table.columns]
        self.table_dict['players_playing_time'] = player_playing_time_table
        # return player_playing_time_table

    def create_player_progression_table(self):
        """Creates a focused progression stats table"""
        perf_cols = ['ID', 'season', 'club_code']
        perf_cols += [col for col in self.file_players.columns if col.startswith("Progression")]
        player_progression_table = self.file_players[perf_cols]
        # player_progression_table['club_code'] = self.get_club_code()
        player_progression_table.columns = [self.sanitize_column_name(col) for col in player_progression_table.columns]
        self.table_dict['players_playing_time'] = player_progression_table
        # return player_playing_time_table

    def create_opponent_total_table(self):
        opponent_total_table = pd.concat([self.data_male.opponent_total, self.data_female.opponent_total])

    def create_player_stat_table(self):
        perf_cols = ['ID', 'season', 'MP', 'club_code']
        player_stat_table = self.file_players[perf_cols]
        # player_stat_table = self.file_players[perf_cols].assign(club_code=self.get_club_code())
        player_stat_table.columns = [self.sanitize_column_name(col) for col in player_stat_table.columns]
        self.table_dict['players_stats'] = player_stat_table
        # return player_stat_table

    def create_player_info_table(self):
        perf_cols = ['ID', 'Player', 'Nation', 'Pos', 'Age', 'Gender', 'season', 'club_code']
        player_info_table = self.file_players[perf_cols]
        player_info_table.columns = [self.sanitize_column_name(col) for col in player_info_table.columns]
        self.table_dict['players_info'] = player_info_table
        # return player_info_table

    def craete_all_tables(self):
        self.create_players_performance_table()
        self.create_teams_table()
        self.create_per_90_minutes_table()
        self.create_nation_table()
        self.create_player_progression_table()
        self.create_player_expected_table()
        self.create_player_info_table()
        self.create_player_playing_time_table()
        self.create_player_stat_table()

    def primary_key_query(self):

        player_table_query_primary = []
        teams_table_query_primary = []
        nations_table_query_primary = []
        all_table_query_primary = []
        player_table = []
        teams_table = []
        nations_table = []
        other_tables = []
        tables_name = list(self.table_dict.keys())

        for table in tables_name:
            if 'players' in table:
                player_table.append(table)
            elif 'teams' in table:
                teams_table.append(table)
            elif 'nations' in table:
                nations_table.append(table)
            else:
                other_tables.append(table)

        for table_name in player_table:
            query = f"""ALTER TABLE {table_name}
                 ADD CONSTRAINT {table_name}_id_pk 
                 PRIMARY KEY (id, season, club_code);"""
            player_table_query_primary.append(query)
            all_table_query_primary.append(query)

        for table_name in teams_table:
            query = f"""ALTER TABLE {table_name}
                 ADD CONSTRAINT club_code_pk 
                 PRIMARY KEY (club_code);"""
            teams_table_query_primary.append(query)
            all_table_query_primary.append(query)

        for table_name in nations_table:
            query = f"""ALTER TABLE {table_name}
                 ADD CONSTRAINT country_code_pk 
                 PRIMARY KEY (country_code);"""
            nations_table_query_primary.append(query)
            all_table_query_primary.append(query)

        dict_query = {'player_PK_queries': player_table_query_primary,
                      'teams_PK_queries': teams_table_query_primary,
                      'nations_PK_queries': nations_table_query_primary}

        return all_table_query_primary

    def load_postgresql(self):
        params = {
            "host": "localhost",
            "dbname": "postgres",
            "user": "postgres",
            "password": "1234"
        }
        db = postgresql_conn(params)
        db.connect()

        self.craete_all_tables()

        for table_name, table in self.table_dict.items():
            cols = ['id', 'season', 'club_code']

            if table_name not in ['teams', 'nations'] and all(c in table.columns for c in cols):
                table.drop_duplicates(subset=cols, inplace=True)
            print(table_name)
            db.load_df(table, table_name)

        db.execute_query(self.primary_key_query())

    print('finish')


if __name__ == "__main__":
    df_male = pd.read_csv('../Data_files/csv files/male.csv')
    df_female = pd.read_csv('../Data_files/csv files/female.csv')
    data = create_tables(df_male, df_female)
    # data.create_nation_table()
    # data.create_players_performance_table()
    # data.create_per_90_minutes_table()
    # data.create_player_expected_table()
    # data.create_teams_table()
    # data.get_club_code()
    data.load_postgresql()