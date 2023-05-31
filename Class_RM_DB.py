import pandas as pd
from bs4 import BeautifulSoup
import requests
import regex
import lxml

class WebScraping_fbref:

    def __init__(self,team_name):
        self.team_name = team_name
        self.fbref_site = 'https://fbref.com/en/squads/53a2f082/'
        # TODO # if we want to make in more general and not only for Real
        # Madrid we will need to change self.fbref_site_end
        self.fbref_site_end = '/Real-Madrid-Stats'

    def create_url_season(self, year):
        """
        :param year: the year which the season start in
        :return: a dictionary , the key is the year and the value is the url site of the season
        """
        try:
            season_url = (self.fbref_site + str(year) + '-' + str(year + 1) + self.fbref_site_end)
        except:
            season_url = "not found"
        return season_url

    def get_link_report_match(self, url):
        #TODO: currently this function is not working
        """
        :param url: url for the season
        :return: list with link to each game in the season
        """
        counter = 0
        list_links = []
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        links = soup.find_all('a')
        for link in links:
            counter += 1
            if ('href' in link.attrs) and ('matches' in str(link.attrs['href'])) and \
                    ('La-Liga' in str(link.attrs['href'])) and counter % 2 != 0:
                list_links.append('https://fbref.com' + str(link.attrs['href']))
        return pd.Series(list_links)

    def compation_df(self, url, comp='La Liga', conv_json=True):

        """
        :param conv_json: boolean operator for the decision of what the outpur will be
        :param url: get url for a specific season ,example:create_url_all_seasons()['2019-2020']
        :param comp:the compation of real madrid : La Liga,Champions Lg,Copa del Rey,Supercopa de Espana
        :return: return the data frame of the comp that were chosen from the url
        """
        dfs = pd.read_html(url)
        season_df = dfs[1]
        # season_df.drop('Notes', inplace=True, axis=1)# remove column 'Notes'
        # season_df = season_df[season_df['Comp'] == comp]
        season_df = season_df.loc[season_df['Comp'] == comp].copy()
        # edit the column 'Match Report' with the link of the match report
        # season_df['Match Report'] = self.get_link_report_match(url)
        season_df.loc[:, 'Match Report'] = self.get_link_report_match(url)
        if conv_json:
            return season_df.to_json(orient='records')
        else:
            return season_df

    def list_df_player_madrid(self, season):
        """
        :param season: the df of the season, example: compation_df(url_season_2019)
        :return: dataframe of players for each game in the season
        """
        list_home_away = [0 if venue == 'Home' else 1 for venue in list(season['Venue'])]
        list_link_venue = zip(season['Match Report'], list_home_away)
        list_df_players = []
        for tup in list_link_venue:
            link, venue = tup
            df_match_player = pd.read_html(link)
            list_df_players.append(df_match_player[venue])
        return list_df_players



if __name__ == '__main__':
    web_screp = WebScraping_fbref("Real Madrid")
    link = web_screp.create_url_season(2000)
    print(web_screp.compation_df(link))



