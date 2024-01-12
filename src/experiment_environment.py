import web_scraping
import pandas as pd

GENDER_MALE = 'Male'
GENDER_FEMALE = 'Female'


def scrape_and_save_data(team_name, team_code, year, gender):
    """
    :param team_name: the team name
    :param team_code: the team code
    :param year: the year of the compatition
    :param gender: the gender of the team
    :return: return the first data frame in the url of the team in that year
    """
    web_scrap = web_scraping.WebScraping_fbref(team_name, team_code, year, conv_json=False)
    link = web_scrap.create_url_season()
    # web_scrap.gender_dataframe(link)

    df_games = web_scrap.all_competition_df(link)
    if web_scrap.gender == gender:
        return df_games[0][1].to_dict(orient='records')
    else:
        return None


def main():
    df_club = pd.read_csv('backup.csv')
    tuple_name_code = list(zip(list(df_club['club_name']), list(df_club['club_code'])))
    list_json_df_male = []
    list_json_df_female = []
    counter = 0
    for team_name, team_code in tuple_name_code:

        print(counter)
        counter = counter +1


        male_data = scrape_and_save_data(team_name, team_code, 2022, 'Male')
        if male_data:
            list_json_df_male.extend(male_data)

        female_data = scrape_and_save_data(team_name, team_code, 2022, 'Female')
        if female_data:
            list_json_df_female.extend(female_data)

        if counter % 10 == 0:
            print('load to csv')
            df_male = pd.DataFrame(list_json_df_male)
            df_female = pd.DataFrame(list_json_df_female)
            df_male.to_csv('male.csv')
            df_female.to_csv('female.csv')

    # with open('res_male.json', "w") as f:
    #     json.dump(list_json_df_male, f)
    #
    # with open('res_female.json', "w") as f:
    #     json.dump(list_json_df_female, f)
#set(chain(*[list(i.keys()) for i in list_json_df_female]))


if __name__ == '__main__':
    # df_club = pd.read_csv('backup.csv')
    # tuple_name_code = list(zip(list(df_club['club_name']), list(df_club['club_code'])))
    # list_json_df_male = []
    # list_json_df_female = []
    # for tup in tuple_name_code[:20]:
    #     team_name = tup[0]
    #     team_code = tup[1]
    #     web_screp = Class_RM_DB.WebScraping_fbref(team_name, team_code, 2022, conv_json=False)
    #     link = web_screp.create_url_season()
    #     web_screp.gender_dataframe(link)
    #
    #     if web_screp.gender == 'Male':
    #         df_games = web_screp.all_competition_df(link)
    #         dict_df_male = df_games[0][1].to_dict(orient='records')
    #         for json_player in dict_df_male:
    #             list_json_df_male.append(json_player)
    #     elif web_screp.gender == 'Female':
    #         df_games = web_screp.all_competition_df(link)
    #         dict_df_female = df_games[0][1].to_dict(orient='records')
    #         for json_player in dict_df_female:
    #             list_json_df_female.append(json_player)
    #     else:
    #         continue
    #
    # with open('res_male.json', "w") as f:
    #     json.dump(list_json_df_male, f)
    # with open('res_female.json', "w") as f:
    #     json.dump(list_json_df_female, f)
    main()
