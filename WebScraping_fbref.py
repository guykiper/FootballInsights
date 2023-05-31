import pandas as pd
from bs4 import BeautifulSoup
import requests
import regex
import lxml

def create_url_season(year):
    """
    :param year: the year which the season start in
    :return: a dictionary , the key is the year and the value is the url site of the season
    """
    fbref_site = 'https://fbref.com/en/squads/53a2f082/'
    fbref_site_end = '/Real-Madrid-Stats'
    dict_season_url = {}
    try:
        dict_season_url[str(year) + '-' + str(year + 1)] = (fbref_site + str(year) + '-' + str(year + 1) + fbref_site_end)
    except:
        dict_season_url[str(year) + '-' + str(year + 1)] = "not found"
    return dict_season_url


def get_link_report_match(url):
    """
    :param url: url for the season
    :return: list with link to each game in the season
    """
    counter = 0
    list_links = []
    page = requests.get(url)
    soup = BeautifulSoup(page.content,'html.parser')
    links = soup.find_all('a')
    for link in links:
        counter += 1
        if ('href' in link.attrs) and ('matches' in str(link.attrs['href'])) and \
                ('La-Liga' in str(link.attrs['href'])) and counter % 2 != 0:
            list_links.append('https://fbref.com'+str(link.attrs['href']))
    return list_links

def compation_df(url,comp ='La Liga',conv_json = True) :

    """
    :param url: get url for a specific season ,example:create_url_all_seasons()['2019-2020']
    :param comp:the compation of real madrid : La Liga,Champions Lg,Copa del Rey,Supercopa de Espana
    :return: return the data frame of the comp that were chosen from the url
    """
    dfs = pd.read_html(url)
    season_df = dfs[1]
    #season_df.drop('Notes', inplace=True, axis=1)# remove column 'Notes'
    season_df = season_df[season_df['Comp'] == comp]
    # edit the column 'Match Report' with the link of the match report
    season_df['Match Report'] = get_link_report_match(url)
    #season_df.loc[:,'Match Report'] = get_link_report_match(url)
    if conv_json:
        return season_df.to_json(orient='records')
    else:
        return season_df


def list_df_player_madrid(season):
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
    # print(create_url_season(2000))
    url_season_2019 = create_url_season(2019)['2019-2020']
    print(get_link_report_match(url_season_2019))
    # season_2019_2020 = compation_df(url_season_2019)
    # print(list_df_player_madrid(season_2019_2020))
