import pandas as pd
import mysql.connector
import random


database_name = 'premier_league_2023_24_stats'

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password = 'root'
)

mycursor = mydb.cursor()
mycursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
mycursor.execute(f"USE {database_name}")

# -------------------------------------------------------------------------------------------------------------------------------- #
# create tables
# team
mycursor.execute("""
CREATE TABLE IF NOT EXISTS team (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    position_league INT,
    country VARCHAR(3),
    city VARCHAR(255),
    region VARCHAR(255),
    matches INT
);
""")

# player
mycursor.execute("""
CREATE TABLE IF NOT EXISTS player (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    country VARCHAR(3),
    strong_foot BOOLEAN,
    matches INT,
    minutes INT
);
""")

# coach
mycursor.execute("""
CREATE TABLE IF NOT EXISTS coach (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT,
    country VARCHAR(3),
    experience_years INT,
    titles INT
);
""")

# team_player
mycursor.execute("""
CREATE TABLE IF NOT EXISTS team_player (
    id INT AUTO_INCREMENT PRIMARY KEY,
    team_id INT,
    player_id INT,
        FOREIGN KEY (team_id) REFERENCES team(id),
        FOREIGN KEY (player_id) REFERENCES player(id)
);
""")

# team_coach
mycursor.execute("""
CREATE TABLE IF NOT EXISTS team_coach (
    id INT AUTO_INCREMENT PRIMARY KEY,
    team_id INT,
    coach_id INT,
        FOREIGN KEY (team_id) REFERENCES team(id),
        FOREIGN KEY (coach_id) REFERENCES coach(id)
);
""")

# team_stats
mycursor.execute("""
CREATE TABLE IF NOT EXISTS team_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    team_id INT,
    cross_success_pct FLOAT,
    accurate_crosses_per_90 FLOAT,
    corners_taken FLOAT,
    team_rating FLOAT,
    touches_in_opp_box FLOAT,
    possession_pct FLOAT,
        FOREIGN KEY (team_id) REFERENCES team(id)
);
""")

# player_stats
mycursor.execute("""
CREATE TABLE IF NOT EXISTS player_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
                 
    -- Player identification            
    player_id INT,
    player_rating FLOAT,
    player_of_match FLOAT,
                 
    -- Goal-related statistics
    goals FLOAT,
    expected_goals FLOAT,
    shot_conversion_rate FLOAT,
    penalties_scored FLOAT,
    
    -- Assist statistics
    assists FLOAT,
    expected_assists FLOAT,
    secondary_assists FLOAT,
    big_chances_created FLOAT,
    big_chances_missed FLOAT,
    
    -- Passing metrics
    pass_success_pct FLOAT,
    accurate_passes_90 FLOAT,
    accurate_long_balls_90 FLOAT,
    successful_long_balls_pct FLOAT,
    
    -- Defensive statistics
    clean_sheets FLOAT,
    goals_conceded FLOAT,
    total_clearances FLOAT,
    total_interceptions FLOAT,
    
    -- Dribbling metrics
    successful_dribbles_90 FLOAT,
    dribble_success_rate FLOAT,
    
    -- Foul statistics
    yellow_cards FLOAT,
    red_cards FLOAT,
    fouls_per_90 FLOAT, 
        FOREIGN KEY (player_id) REFERENCES player(id)
);
""")

# -------------------------------------------------------------------------------------------------------------------------------- #
# INSERT DATA
# Read data from CSV files
# Dataframes for teams, coaches, and players
df_coaches = pd.read_csv("./dataset/coaches.csv").replace("&", "and", regex=True)
df_players = pd.read_csv("./dataset/player_expected_assists.csv")
df_teams = pd.read_csv("./dataset/pl_table_2023_24.csv").replace("&", "and", regex=True)

df_accurate_cross_team = pd.read_csv("./dataset/accurate_cross_team.csv")[
    ["Team", "Cross Success (%)", "Accurate Crosses per Match"]
]
df_corner_taken_team = pd.read_csv("./dataset/corner_taken_team.csv")[
    ["Team", "Corners Taken"]
]
df_team_ratings = pd.read_csv("./dataset/team_ratings.csv")[
    ["Team", "FotMob Team Rating"]
]
df_touches_in_opp_box_team = pd.read_csv("./dataset/touches_in_opp_box_team.csv")[
    ["Team", "Touches in Opposition Box"]
]
df_possession_percentage_team = pd.read_csv("./dataset/possession_percentage_team.csv")[
    ["Team", "Possession (%)"]
]
df_team_stats = pd.read_csv("./dataset/pl_table_2023_24.csv").rename(
    columns={"name": "Team"}
)[["Team"]]
df_team_stats = (
    df_team_stats.merge(df_accurate_cross_team, on="Team", how="left")
    .merge(df_corner_taken_team, on="Team", how="left")
    .merge(df_team_ratings, on="Team", how="left")
    .merge(df_touches_in_opp_box_team, on="Team", how="left")
    .merge(df_possession_percentage_team, on="Team", how="left")
).replace("&", "and", regex=True)


df_player_player_ratings = pd.read_csv("./dataset/player_player_ratings.csv")[
    ["Player", "FotMob Rating", "Player of the Match Awards"]
]
df_player_top_scorers = pd.read_csv("./dataset/player_top_scorers.csv")[
    ["Player", "Goals", "Penalties"]
]
df_player_expected_goals = pd.read_csv("./dataset/player_expected_goals.csv")[
    ["Player", "Expected Goals (xG)"]
]
df_player_big_chances_missed = pd.read_csv("./dataset/player_big_chances_missed.csv")[
    ["Player", "Big Chances Missed", "Shot Conversion Rate (%)"]
]
df_player_top_assists = pd.read_csv("./dataset/player_top_assists.csv")[
    ["Player", "Assists", "Secondary Assists"]
]
df_player_expected_assists = pd.read_csv("./dataset/player_expected_assists.csv")[
    ["Player", "Expected Assists (xA)"]
]
df_player_big_chances_created = pd.read_csv("./dataset/player_big_chances_created.csv")[
    ["Player", "Big Chances Created"]
]
df_player_accurate_passes = pd.read_csv("./dataset/player_accurate_passes.csv")[
    ["Player", "Accurate Passes per 90", "Pass Success (%)"]
]
df_player_accurate_long_balls = pd.read_csv("./dataset/player_accurate_long_balls.csv")[
    ["Player", "Accurate Long Balls per 90", "Successful Long Balls (%)"]
]
df_player_clean_sheets = pd.read_csv("./dataset/player_clean_sheets.csv")[
    ["Player", "Clean Sheets", "Goals Conceded"]
]
df_player_effective_clearances = pd.read_csv(
    "./dataset/player_effective_clearances.csv"
)[["Player", "Total Clearances"]]
df_player_interceptions = pd.read_csv("./dataset/player_interceptions.csv")[
    ["Player", "Total Interceptions"]
]
df_player_contests_won = pd.read_csv("./dataset/player_contests_won.csv")[
    ["Player", "Successful Dribbles per 90", "Dribble Success Rate (%)"]
]
df_player_yellow_cards = pd.read_csv("./dataset/player_yellow_cards.csv")[
    ["Player", "Yellow Cards", "Red Cards"]
]
df_player_fouls_committed = pd.read_csv("./dataset/player_fouls_committed.csv")[
    ["Player", "Fouls Committed per 90"]
]
df_player_stats = (
    df_player_player_ratings.merge(df_player_top_scorers, on="Player", how="left")
    .merge(df_player_expected_goals, on="Player", how="left")
    .merge(df_player_big_chances_missed, on="Player", how="left")
    .merge(df_player_top_assists, on="Player", how="left")
    .merge(df_player_expected_assists, on="Player", how="left")
    .merge(df_player_big_chances_created, on="Player", how="left")
    .merge(df_player_accurate_passes, on="Player", how="left")
    .merge(df_player_accurate_long_balls, on="Player", how="left")
    .merge(df_player_clean_sheets, on="Player", how="left")
    .merge(df_player_effective_clearances, on="Player", how="left")
    .merge(df_player_interceptions, on="Player", how="left")
    .merge(df_player_contests_won, on="Player", how="left")
    .merge(df_player_yellow_cards, on="Player", how="left")
    .merge(df_player_fouls_committed, on="Player", how="left")
).fillna(0)

# COACHES

for i in range(len(df_coaches)):
    coach = df_coaches.iloc[i]
    mycursor.execute(f"""
    INSERT INTO coach (name, age, country, experience_years, titles)
    VALUES ('{coach['Name']}', {coach['Age']}, '{coach['Country']}', {coach['Experience']}, {coach['Titles']});
    """)
    mydb.commit()

print('Coaches inserted')

# TEAMS

for i in range(len(df_teams)):
    team = df_teams.iloc[i]
    mycursor.execute(f"""
    INSERT INTO team (name, country, position_league, matches)
    VALUES ('{team['name']}', 'ENG', {team['idx']}, {team['played']});
    """)
    mycursor.execute(f"""
    INSERT INTO team_stats (team_id) VALUES ((SELECT id FROM team WHERE name = '{team['name']}'));
    """)
    mydb.commit()

update_team_queries = [
    "UPDATE team SET city = 'Manchester', region = 'North' WHERE name = 'Manchester City';",
    "UPDATE team SET city = 'London', region = 'South' WHERE name = 'Arsenal';",
    "UPDATE team SET city = 'Liverpool', region = 'North' WHERE name = 'Liverpool';",
    "UPDATE team SET city = 'Birmingham', region = 'Midlands' WHERE name = 'Aston Villa';",
    "UPDATE team SET city = 'London', region = 'South' WHERE name = 'Tottenham Hotspur';",
    "UPDATE team SET city = 'London', region = 'South' WHERE name = 'Chelsea';",
    "UPDATE team SET city = 'Newcastle', region = 'North' WHERE name = 'Newcastle United';",
    "UPDATE team SET city = 'Manchester', region = 'North' WHERE name = 'Manchester United';",
    "UPDATE team SET city = 'London', region = 'South' WHERE name = 'West Ham United';",
    "UPDATE team SET city = 'London', region = 'South' WHERE name = 'Crystal Palace';",
    "UPDATE team SET city = 'Brighton', region = 'South' WHERE name = 'Brighton and Hove Albion';",
    "UPDATE team SET city = 'Bournemouth', region = 'South' WHERE name = 'Bournemouth';",
    "UPDATE team SET city = 'London', region = 'South' WHERE name = 'Fulham';",
    "UPDATE team SET city = 'Wolverhampton', region = 'Midlands' WHERE name = 'Wolverhampton Wanderers';",
    "UPDATE team SET city = 'Liverpool', region = 'North' WHERE name = 'Everton';",
    "UPDATE team SET city = 'Brentford', region = 'South' WHERE name = 'Brentford';",
    "UPDATE team SET city = 'Nottingham', region = 'Midlands' WHERE name = 'Nottingham Forest';",
    "UPDATE team SET city = 'Luton', region = 'South' WHERE name = 'Luton Town';",
    "UPDATE team SET city = 'Burnley', region = 'North' WHERE name = 'Burnley';",
    "UPDATE team SET city = 'Sheffield', region = 'North' WHERE name = 'Sheffield United';",
]

for query in update_team_queries:
    mycursor.execute(query)

mydb.commit()
	

print('Teams inserted')

# TEAM COACHES

insert_team_coach = """
INSERT INTO team_coach (team_id, coach_id) VALUES 
((SELECT id FROM team WHERE name = 'Manchester City'), (SELECT id FROM coach WHERE name = 'Pep Guardiola')),
((SELECT id FROM team WHERE name = 'Arsenal'), (SELECT id FROM coach WHERE name = 'Mikel Arteta')),
((SELECT id FROM team WHERE name = 'Liverpool'), (SELECT id FROM coach WHERE name = 'Jürgen Klopp')),
((SELECT id FROM team WHERE name = 'Aston Villa'), (SELECT id FROM coach WHERE name = 'Unai Emery')),
((SELECT id FROM team WHERE name = 'Tottenham Hotspur'), (SELECT id FROM coach WHERE name = 'Ange Postecoglou')),
((SELECT id FROM team WHERE name = 'Chelsea'), (SELECT id FROM coach WHERE name = 'Mauricio Pochettino')),
((SELECT id FROM team WHERE name = 'Newcastle United'), (SELECT id FROM coach WHERE name = 'Eddie Howe')),
((SELECT id FROM team WHERE name = 'Manchester United'), (SELECT id FROM coach WHERE name = 'Erik Ten Hag')),
((SELECT id FROM team WHERE name = 'West Ham United'), (SELECT id FROM coach WHERE name = 'David Moyes')),
((SELECT id FROM team WHERE name = 'Crystal Palace'), (SELECT id FROM coach WHERE name = 'Roy Hodgson')),
((SELECT id FROM team WHERE name = 'Crystal Palace'), (SELECT id FROM coach WHERE name = 'Oliver Glasner')),
((SELECT id FROM team WHERE name = 'Brighton and Hove Albion'), (SELECT id FROM coach WHERE name = 'Roberto De Zerbi')),
((SELECT id FROM team WHERE name = 'Bournemouth'), (SELECT id FROM coach WHERE name = 'Andoni Iraola')),
((SELECT id FROM team WHERE name = 'Fulham'), (SELECT id FROM coach WHERE name = 'Marco Silva')),
((SELECT id FROM team WHERE name = 'Wolverhampton Wanderers'), (SELECT id FROM coach WHERE name = 'Julen Lopetegui')),
((SELECT id FROM team WHERE name = 'Wolverhampton Wanderers'), (SELECT id FROM coach WHERE name = 'Gary O’Neil')),
((SELECT id FROM team WHERE name = 'Everton'), (SELECT id FROM coach WHERE name = 'Sean Dyche')),
((SELECT id FROM team WHERE name = 'Brentford'), (SELECT id FROM coach WHERE name = 'Thomas Frank')),
((SELECT id FROM team WHERE name = 'Nottingham Forest'), (SELECT id FROM coach WHERE name = 'Steve Cooper')),
((SELECT id FROM team WHERE name = 'Nottingham Forest'), (SELECT id FROM coach WHERE name = 'Nuno Espírito Santo')),
((SELECT id FROM team WHERE name = 'Luton Town'), (SELECT id FROM coach WHERE name = 'Rob Edwards')),
((SELECT id FROM team WHERE name = 'Burnley'), (SELECT id FROM coach WHERE name = 'Vincent Kompany')),
((SELECT id FROM team WHERE name = 'Sheffield United'), (SELECT id FROM coach WHERE name = 'Paul Heckingbottom')),
((SELECT id FROM team WHERE name = 'Sheffield United'), (SELECT id FROM coach WHERE name = 'Chris Wilder'));
"""

for result in mycursor.execute(insert_team_coach, multi=True):
    if result.with_rows:
        print(result.fetchall())

mydb.commit()

print('Team coaches inserted')

# PLAYERS
# TEAM PLAYERS

escape_single_quotes = lambda x: x.replace("'", "’")

for i in range(len(df_players)):
    strong_foot = random.choice([True, False])
    player = df_players.iloc[i]
    player_name = escape_single_quotes(player['Player']) 
    mycursor.execute(f"""
    INSERT INTO player (name, country, strong_foot, matches, minutes)
    VALUES ('{player_name}', '{player['Country']}', {strong_foot}, {player['Matches']}, {player['Minutes']});
    """)
    mycursor.execute(f"""
    INSERT INTO team_player (team_id, player_id) VALUES 
    ((SELECT id FROM team WHERE name = '{player['Team']}'), (SELECT id FROM player WHERE name = '{player_name}'));
    """)
    mycursor.execute(f"""
    INSERT INTO player_stats (player_id) VALUES
    ((SELECT id FROM player WHERE name = '{player_name}'));
    """)             
    mydb.commit()


print('Players inserted')

# TEAM STATS

queries = []
for i in range(len(df_team_stats)):
    team = df_team_stats.iloc[i]
    params = (
        float(team['Cross Success (%)']),
        float(team['Accurate Crosses per Match']),
        int(team['Corners Taken']),
        float(team['FotMob Team Rating']),
        int(team['Touches in Opposition Box']),
        float(team['Possession (%)']),
        escape_single_quotes(team['Team'])
    )
    query = """
    UPDATE team_stats
    SET cross_success_pct = %s,
        accurate_crosses_per_90 = %s,
        corners_taken = %s,
        team_rating = %s,
        touches_in_opp_box = %s,
        possession_pct = %s
    WHERE team_id = (SELECT id FROM team WHERE name = %s);
    """
    queries.append((query, params))

for query, params in queries:
    try:
        mycursor.execute(query, params)
    except mysql.connector.Error as err:
        print(f"Error executing query with params {params}: {err}")

mydb.commit()

print('Team stats inserted')


# PLAYER STATS
# Prepare a list of queries and parameters

queries = []

for i in range(len(df_player_stats)):
    player = df_player_stats.iloc[i]
    player_name = escape_single_quotes(player['Player'])
    params = (
        float(player['FotMob Rating']),
        int(player['Player of the Match Awards']),
        int(player['Goals']),
        float(player['Expected Goals (xG)']),
        float(player['Shot Conversion Rate (%)']),
        int(player['Penalties']),
        int(player['Assists']),
        float(player['Expected Assists (xA)']),
        int(player['Secondary Assists']),
        int(player['Big Chances Created']),
        int(player['Big Chances Missed']),
        float(player['Pass Success (%)']),
        float(player['Accurate Passes per 90']),
        float(player['Accurate Long Balls per 90']),
        float(player['Successful Long Balls (%)']),
        int(player['Clean Sheets']),
        int(player['Goals Conceded']),
        int(player['Total Clearances']),
        int(player['Total Interceptions']),
        float(player['Successful Dribbles per 90']),
        float(player['Dribble Success Rate (%)']),
        int(player['Yellow Cards']),
        int(player['Red Cards']),
        float(player['Fouls Committed per 90']),
        player_name
    )

    query = """
    UPDATE player_stats
    SET player_rating = %s,
        player_of_match = %s,
        goals = %s,
        expected_goals = %s,
        shot_conversion_rate = %s,
        penalties_scored = %s,
        assists = %s,
        expected_assists = %s,
        secondary_assists = %s,
        big_chances_created = %s,
        big_chances_missed = %s,
        pass_success_pct = %s,
        accurate_passes_90 = %s,
        accurate_long_balls_90 = %s,
        successful_long_balls_pct = %s,
        clean_sheets = %s,
        goals_conceded = %s,
        total_clearances = %s,
        total_interceptions = %s,
        successful_dribbles_90 = %s,
        dribble_success_rate = %s,
        yellow_cards = %s,
        red_cards = %s,
        fouls_per_90 = %s
    WHERE player_id = (SELECT id FROM player WHERE name = %s);
    """
    queries.append((query, params))

for query, params in queries:
    try:
        mycursor.execute(query, params)
    except mysql.connector.Error as err:
        print(f"Error executing query with params {params}: {err}")

mydb.commit()

print('Player stats inserted')
