import pandas as pd


class create_tables:

    def __init__(self):
        self.file_locatoin_info_male = pd.read_csv('Data_files/csv files/locaiton_info.csv')
        self.file_location_info_female = pd.read_csv('Data_files/csv files/locaiton_info_female.csv')
        self.file_players = pd.read_csv('Data_files/csv files/players.csv')
        self.file_teams = pd.read_csv('Data_files/csv files/backup_new.csv')
        self.club_and_code = self.create_teams_table()[['club_name','club_code']]
        self.club_code = self.get_club_code()
        pass


    def get_club_code(self):

        merged = self.file_players.merge(self.club_and_code, how='left', left_on='team', right_on='club_name')
        # Drop 'club_name' column now
        # final_df = merged.drop('club_name', axis=1).rename(columns={"Code": "team"})
        final_df = merged.drop_duplicates(subset=['ID'])
        return(list(final_df['club_code']))



    def create_teams_table(self):
        teams_table = self.file_teams[['club_code','club_name','gender']]
        teams_table['country'] = [country.split()[0] for country in self.file_teams['country'] ]
        return teams_table


    def create_nation_table(self):
        nation_table = pd.concat([self.file_locatoin_info_male, self.file_location_info_female])
        nation_table = nation_table.drop_duplicates(subset=['country_code'])
        nation_table = nation_table[['country_code', 'country_name', 'latitude', 'longitude']]



    def create_players_performance_table(self):
        perf_cols = []
        perf_cols.append('ID')
        perf_cols.append('season')
        perf_cols += [col for col in self.file_players.columns if col.startswith("Performance")]
        players_performance_table = self.file_players[perf_cols]
        players_performance_table['club_code'] = self.get_club_code()
        print('hi')

    def create_per_90_minutes_table(self):
        perf_cols = []
        perf_cols.append('ID')
        perf_cols.append('season')
        perf_cols += [col for col in self.file_players.columns if col.startswith("Per 90")]
        per_90_minutes_table = self.file_players[perf_cols]
        per_90_minutes_table['club_code'] = self.get_club_code()
        print('hi')

    def create_player_expected_table(self):
        perf_cols = []
        perf_cols.append('ID')
        perf_cols.append('season')
        perf_cols += [col for col in self.file_players.columns if col.startswith("Expected")]
        player_expected_table = self.file_players[perf_cols]
        player_expected_table['club_code'] = self.get_club_code()
        print('hi')


    def create_player_playing_time_table(self):
        perf_cols = []
        perf_cols.append('ID')
        perf_cols.append('season')
        perf_cols += [col for col in self.file_players.columns if col.startswith("Playing Time")]
        player_playing_time_table = self.file_players[perf_cols]
        player_playing_time_table['club_code'] = self.get_club_code()
        print('hi')

    def create_player_progression_table(self):
        perf_cols = []
        perf_cols.append('ID')
        perf_cols.append('season')
        perf_cols += [col for col in self.file_players.columns if col.startswith("Progression")]
        player_playing_time_table = self.file_players[perf_cols]
        player_playing_time_table['club_code'] = self.get_club_code()
        print('hi')


    def create_player_stat_table(self):
        # perf_cols = []
        # perf_cols.append('ID')
        # perf_cols.append('season')
        # perf_cols.append('MP')
        # player_stat_table = self.file_players[perf_cols]
        # player_stat_table['club_code'] = self.get_club_code()
        perf_cols = ['ID', 'season', 'MP']
        player_stat_table = self.file_players[perf_cols].assign(club_code=self.get_club_code())

    def create_player_info_table(self):
        perf_cols = ['ID', 'Player', 'Nation', 'Pos', 'Age', 'gender']
        player_info_table = self.file_players[perf_cols]


if __name__ == "__main__":

    data = create_tables()
    # data.create_nation_table()
    # data.create_players_performance_table()
    # data.create_per_90_minutes_table()
    # data.create_player_expected_table()
    # data.create_teams_table()
    data.get_club_code()