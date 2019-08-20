import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from time import sleep


def fetch(url):
    """
    Use BeautifulSoup to get the text of a web page.
    """
    r = requests.get(url)
    comm = re.compile("<!--|-->")
    soup = BeautifulSoup(comm.sub("", r.text), "html.parser")
    return soup


def df2numeric(df, avoid):
    """
    Convert data types of some columns to numeric.
    The input is the dataframe.
    The output is the list that you don't wish to convert.
    """
    for col in list(df.columns):
        if col not in avoid:
            df[col] = pd.to_numeric(pd.Series(df[col].values))
    return df


def col_order_adjust(df, desired):
    """
    Adjust column order of a dataframe.
    Desired is a list of columns you wish to stay in front.
    """
    col_list = list(df.columns)
    new_order = desired + [col for col in col_list if col not in desired]
    return df[new_order]


def get_raw_data(html):
    """
    Scrape the box score info from a html table.
    The input is an html table.
    The output is a list of data.
    """
    data_list = []
    for row in html.find_all('tr'):
        row_list = []
        for item in row.find_all('th'):
            row_list.append(item.text)
        for item in row.find_all('td'):
            row_list.append(item.text)
        data_list.append(row_list)
    return data_list


### Schedule Scraper ###


def get_schedule(year):
    """
    Get the schedule dataframe based on year.
    The input is year.
    Output is a dataframe.
    """

    months_dict = {'october': 10, 'november': 11, 'december': 12,
                   'january': 1, 'february': 2, 'march': 3, 'april': 4}
    months = months_dict.keys()
    site_url = 'https://www.basketball-reference.com'
    season_url = f'{site_url}/leagues/NBA_'

    # Get schedule for each month
    schedule_df_list = []
    for month in months:
        month_url = f'{season_url}{year}_games-{month}.html'
        soup = fetch(month_url)
        target_table = soup.findAll('table', id='schedule')

        column_flag = 1
        column_list = []
        data_list = []
        for row in target_table[0].find_all('tr'):
            # Get column names
            if column_flag == 1:
                for item in row.find_all('th'):
                    column_list.append(item.text)
                column_flag = 0
            # Get row records
            else:
                row_list = []
                for item in row.find_all('th'):
                    row_list.append(item.text)
                for item in row.find_all('td'):
                    if item.text != 'Box Score':
                        row_list.append(item.text)
                    else:
                        row_list.append(f'{site_url}{item.a["href"]}')
                data_list.append(row_list)

        # Create pandas dataframe
        column_list[1] = 'Start_ET'
        column_list[2] = 'Visitor'
        column_list[3] = 'Visitor_Pts'
        column_list[4] = 'Home'
        column_list[5] = 'Home_Pts'
        column_list[6] = 'Box_Score_Url'
        column_list[7] = 'is_OT'
        column_list[8] = 'Attendance'
        df = pd.DataFrame(columns=column_list)
        for i in range(len(data_list)):
            if 'Playoffs' in data_list[i]:
                break
            df.loc[i] = data_list[i]
        df['Month'] = months_dict[month]
        schedule_df_list.append(df)

    # Stack the schedule
    schedule_df = pd.concat(schedule_df_list)\
                    .reset_index()\
                    .drop('index', axis=1)

    # Edit dataframe
    schedule_df['Game_No'] = [i + 1 for i in range(len(schedule_df))]
    schedule_df['is_OT'] = schedule_df['is_OT'].apply(lambda x: 0 if x == ''
                                                      else (1 if x == 'OT'
                                                            else int(x[0])))
    schedule_df['DoW'] = schedule_df.Date.apply(lambda x: x.split(',')[0])
    schedule_df['Year'] = schedule_df.Date.apply(lambda x: x.split(',')[2]
                                                            .strip(' '))\
                                          .apply(int)
    schedule_df['Day'] = schedule_df.Date.apply(lambda x: x.split(',')[1]
                                                .strip(' ').split(' ')[1])\
                                         .apply(int)
    schedule_df['Date'] = schedule_df.Year.apply(str) + '-' + \
        schedule_df.Month.apply(str) + '-' + \
        schedule_df.Day.apply(str)
    schedule_df['Visitor_Pts'] = schedule_df['Visitor_Pts'].apply(int)
    schedule_df['Home_Pts'] = schedule_df['Home_Pts'].apply(int)
    schedule_df['Attendance'] = schedule_df['Attendance'].apply(
        lambda x: int(x.replace(',', '')))
    schedule_df['is_Loc_Not_Home'] = schedule_df.Notes\
                                                .apply(lambda x: 0 if x == ''
                                                       else 1)

    # Re-arrange columns
    desired = ['Game_No', 'Date', 'Year', 'Month', 'Day', 'DoW']
    schedule_df = col_order_adjust(schedule_df, desired)

    return schedule_df

### Box Score Scraper ###


def build_inactives_df(s, team_name):
    """
    Build dataframe regarding inactives in a game for a team.
    The inputs are string of info regarding inactives and team's name.
    The outputs are dataframe and team's acronyms.
    """
    team_st = s[:s.find(' ')]
    df = pd.DataFrame()
    df['Player'] = s.replace(team_st, '').strip().split(', ')
    df['Team'] = team_name
    return df, team_st


def get_inactives(soup, visitor, home):
    """
    Build dataframe regarding inactives in a game for a team.
    Generate both teams' acronyms.
    The inputs are the soup of the game's box score page,
    team names of both teams.
    The outputs are the dataframe regarding inactives and teams' acronyms.
    """
    # Find inactives infomation
    divs = soup.find_all('div')
    for div in divs:
        for strong in div.find_all('strong'):
            if strong.text == 'Inactive:':
                target = div.text \
                            .replace('\n', ' ') \
                            .replace('\xa0', ' ') \
                            .replace('Inactive:', '')
                break
    v, h = target[:target.find('Officials')].strip().split('   ')

    # Build inactives dataframes
    v_df, v_st = build_inactives_df(v, visitor)
    h_df, h_st = build_inactives_df(h, home)

    return pd.concat([v_df, h_df]), v_st, h_st


def build_box_score_df(data, team_name, opponent):
    """
    Build box score dataframes for both player and team.
    The inputs are data, team's name and its opponent's name.
    The outputs is player dataframe and team dataframe.
    """
    dnp_list = []  # Create a list of players who did not play

    # Build player dataframe
    player_col = data[0]
    player_col[0] = 'Player'
    player_data = data[1:]
    player_df = pd.DataFrame(columns=player_col)
    for i in range(len(player_data)):
        if 'Reserves' not in player_data[i]:
            if 'Team Totals' not in player_data[i]:
                if len(player_data[i]) != len(player_col):
                    dnp_list.append(player_data[i][0])
                else:
                    player_df.loc[i] = player_data[i]
            else:
                team_data = player_data[i]
    player_df['GS'] = [1] * 5 + [0] * (len(player_df) - 5)
    player_df['Team'] = team_name
    player_df['Opponent'] = opponent

    # Build team dataframe
    team_col = data[0]
    team_col[0] = 'Team'
    team_df = pd.DataFrame(columns=team_col)
    team_data[0] = team_name
    team_df.loc[0] = team_data
    team_df['Opponent'] = opponent

    return player_df, team_df, dnp_list


def get_game_stats(soup, team_name, team_st, opponent):
    """
    Get game stats for a team and its players.
    The inputs are the soup, team's name, its acronym, and its opponent's name.
    The ouputs are two dataframes (One for team, One for players).
    """
    # Basic Box Score
    basic_id = f'box_{team_st.lower()}_basic'
    basic_table = soup.findAll('table', id=basic_id)[0]
    basic_data = get_raw_data(basic_table)[1:]
    # Build player and team dataframes for basic box score
    basic_player_df, basic_team_df, dnp_list = build_box_score_df(basic_data,
                                                                  team_name,
                                                                  opponent)
    basic_team_df = basic_team_df.drop('+/-', axis=1)

    # Advanced Box Score
    adv_id = f'box_{team_st.lower()}_advanced'
    adv_table = soup.findAll('table', id=adv_id)[0]
    adv_data = get_raw_data(adv_table)[1:]
    # Build player and team dataframes for advanced box score
    adv_player_df, adv_team_df, _ = build_box_score_df(adv_data,
                                                       team_name,
                                                       opponent)

    # Merge dataframes
    player_df = basic_player_df.merge(adv_player_df,
                                      on=['Player', 'Team', 'Opponent',
                                          'GS', 'MP'])
    team_df = basic_team_df.merge(adv_team_df,
                                  on=['Team', 'Opponent', 'MP'])

    return player_df, team_df, dnp_list


def MPadjust(s):
    """
    Adjust minutes played to numeric values.
    Example: '12:45' --> 12.75
    """
    s_list = s.split(':')
    return int(s_list[0]) + int(s_list[1]) / 60


def get_game_info(schedule_info):
    """
    Get Player, Team and DNP dataframe.
    The input is a pd.series of info regarding one game.
    The outputs are a player dataframe, a team dataframe and a dnp dataframe.
    """
    visitor = schedule_info['Visitor']
    home = schedule_info['Home']
    game_url = schedule_info['Box_Score_Url']
    game_no = schedule_info['Game_No']
    is_home = schedule_info['is_Loc_Not_Home']

    soup = fetch(game_url)

    # Get inactives information
    inactives_df, v_st, h_st = get_inactives(soup, visitor, home)
    inactives_df['Note'] = 'Inactive'

    # Get stats
    h_player_df, h_team_df, h_dnp = get_game_stats(soup, home, h_st, visitor)
    v_player_df, v_team_df, v_dnp = get_game_stats(soup, visitor, v_st, home)

    # Create Home Tag
    if is_home == 0:
        h_player_df['is_Home'] = 1
        h_team_df['is_Home'] = 1
        v_player_df['is_Home'] = 0
        v_team_df['is_Home'] = 0
    else:
        h_player_df['is_Home'] = 0.5
        h_team_df['is_Home'] = 0.5
        v_player_df['is_Home'] = 0.5
        v_team_df['is_Home'] = 0.5

    # Build DNP dataframe
    h_dnp = [[p, home, 'DNP'] for p in h_dnp] if h_dnp != [] else []
    v_dnp = [[p, visitor, 'DNP'] for p in v_dnp] if v_dnp != [] else []
    dnp_list = h_dnp + v_dnp
    dnp_df = pd.DataFrame(dnp_list, columns=list(inactives_df.columns))
    dnp_df = pd.concat([inactives_df, dnp_df]) \
        .reset_index().drop('index', axis=1) \
        .sort_values(['Team', 'Player'])
    dnp_df['Game_No'] = game_no

    # Build player dataframe
    player_df = pd.concat([h_player_df, v_player_df])\
                  .reset_index().drop('index', axis=1)
    player_df = df2numeric(player_df,
                           ['Player', 'Team', 'Opponent', 'MP'])
    player_df['MP'] = player_df['MP'].apply(lambda x: MPadjust(x))
    player_df['Game_No'] = game_no
    player_df = col_order_adjust(player_df, ['Player', 'Game_No', 'is_Home',
                                             'Team', 'Opponent', 'GS'])

    # Build team dataframe
    team_df = pd.concat([h_team_df, v_team_df])\
                .reset_index().drop('index', axis=1)
    team_df = df2numeric(team_df, ['Team', 'Opponent'])
    team_df['Game_No'] = game_no
    team_df = col_order_adjust(team_df, ['Team', 'Game_No',
                                         'is_Home', 'Opponent'])

    return player_df, team_df, dnp_df

### Fanduel Data Scraper ###

def create_team_dict(year):
    """
    Create team dictionary (Ex. BOS: Boston Celtics).
    """
    site_url = 'https://www.basketball-reference.com'
    teams_url = f'{site_url}/leagues/NBA_{year}.html'
    soup = fetch(teams_url)
    table = soup.find_all('table', id='team-stats-per_game')[0]

    team_dict = {}
    for row in table.find_all('tr'):
        row_list = []
        for item in row.find_all('td'):
            if item.a:
                value = item.text.replace('*', '')
                key = item.a["href"].split('/')[2]
                team_dict[key] = value
    return team_dict


def gameday_fd_data(date):
    """
    Get Fanduel data given a day.
    The input is the date.
    The output is a dataframe.
    """
    year, month, day = date.split('-')
    url_pre = 'http://rotoguru1.com/cgi-bin/hyday.pl?game=fd&'
    url = url_pre + f'mon={month}&day={day}&year={year}'
    # Get data
    soup = fetch(url)
    data_list = get_raw_data(soup)
    pos = ['PG', 'SG', 'SF', 'PF', 'C']
    data_list = [row[:6] for row in data_list if row[0] in pos]

    # Build dataframe
    cols = ['Pos', 'Player', 'FD_Pts', 'Salary',
            'Team', 'Opponent']
    df = pd.DataFrame(data_list, columns=cols)
    df['GS'] = df['Player'].apply(lambda x: 1 if '^' in x else 0)
    df['Date'] = date
    df['Player'] = df['Player'].apply(lambda x: x.replace('^', ''))
    df['Salary'] = df['Salary'].apply(lambda x: x.replace('$', '')
                                      .replace(',', '')).apply(int)
    df['FD_Pts'] = df['FD_Pts'].apply(float)
    df['Team'] = df['Team'].apply(lambda x: x.upper())
    df['Opponent'] = df['Opponent'].apply(lambda x: x.upper()
                                          .replace('@ ', '')
                                          .replace('V ', ''))
    return df


def get_fd_data(gamedays):
    """
    Get Fanduel data for a season.
    The input is a list of gamedays.
    The output is a dataframe.
    """
    df = pd.concat([gameday_fd_data(date) for date in gamedays])
    df = df[['Player', 'Pos', 'Date', 'Team', 'Opponent', 'GS',
             'Salary', 'FD_Pts']]
    df = df.reset_index().drop('index', axis=1)
    return df

### Integration ###


def scrape_nba_data(year):
    """
    Scrape and save the data of a season.
    The input is the year.
    """
    print(year)
    print()

    # Get schedule
    schedule_df = get_schedule(year)
    gameday_dict = {int(x[0]): x[1]
                    for x in schedule_df[['Game_No', 'Date']].values}

    # Scraping
    # Get box score info
    schedule_len, _ = schedule_df.shape
    player_dfs = []
    team_dfs = []
    dnp_dfs = []
    for i in range(schedule_len):
        schedule_info = schedule_df.loc[i]
        p, t, d = get_game_info(schedule_info)
        player_dfs.append(p)
        team_dfs.append(t)
        dnp_dfs.append(d)
        if (i + 1) % 500 == 0:
            print(f'{i + 1} Done')
    print('Box scores scraped.')
    # Get fanduel info
    gamedays = list(set(schedule_df.Date.values))
    fanduel_df = get_fd_data(gamedays)
    team_dict = create_team_dict(year)
    team_dict['BKN'] = team_dict['BRK']
    team_dict['CHA'] = team_dict['CHO']
    team_dict['NOR'] = team_dict['NOP']
    fanduel_df['Team'] = fanduel_df['Team'].apply(lambda x: team_dict[x])
    fanduel_df['Opponent'] = fanduel_df['Opponent'].apply(lambda x:
                                                          team_dict[x])
    print('Fanduel data scraped.')
    print('All the data are scraped.\n')

    player_df = pd.concat(player_dfs)
    player_df['Date'] = player_df.Game_No.apply(lambda x: gameday_dict[x])
    team_df = pd.concat(team_dfs)
    team_df['Date'] = team_df.Game_No.apply(lambda x: gameday_dict[x])
    dnp_df = pd.concat(dnp_dfs)
    dnp_df['Date'] = dnp_df.Game_No.apply(lambda x: gameday_dict[x])
    # Save dataframes
    schedule_df.to_csv(f'data/schedule_{year}.csv', index=False)
    player_df.to_csv(f'data/player_{year}.csv', index=False)
    team_df.to_csv(f'data/team_{year}.csv', index=False)
    dnp_df.to_csv(f'data/dnp_{year}.csv', index=False)
    fanduel_df.to_csv(f'data/fanduel_{year}.csv', index=False)
    print('All data are saved.\n')

