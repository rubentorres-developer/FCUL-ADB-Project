import pandas as pd
import mysql.connector


database_name = 'premier_league_2023_24_stats'

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password = 'root',
    database = database_name
)

mycursor = mydb.cursor()

def simple_query_1():
    query = """
    SELECT 
        Name, Country, Strong_Foot, Matches, Minutes
    FROM 
        Player
    WHERE 
        Country = 'POR' OR Country = 'ESP';
    """

    mycursor.execute(query)
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)

def simple_query_2():
    query = """
    INSERT INTO Coach (Name, Country, Age, Experience_Years, Titles)
    VALUES ('Ruben Amorim', 'POR', 39, 6, 6);
    """
    mycursor.execute(query)
    mydb.commit()
    print(mycursor.rowcount, "record inserted.")

    query = """
    INSERT INTO Team_Coach (Team_ID, Coach_ID)
    SELECT t.ID, c.ID
    FROM Team t
    JOIN Coach c ON c.Name = 'Ruben Amorim'
    WHERE t.Name = 'Manchester United';
    """
    mycursor.execute(query)
    mydb.commit()
    print(mycursor.rowcount, "record inserted.")


def top_performing_coaches():
    query = """
    SELECT c.name AS coach_name, c.country, c.experience_years, 
           t.name AS team_name, t.position_league,
           ts.team_rating, ts.possession_pct,
           AVG(ps.player_rating) AS avg_player_rating,
           SUM(ps.goals) AS total_team_goals,
           SUM(ps.assists) AS total_team_assists
    FROM coach c
    JOIN team_coach tc ON c.id = tc.coach_id
    JOIN team t ON tc.team_id = t.id
    JOIN team_stats ts ON t.id = ts.team_id
    JOIN team_player tp ON t.id = tp.team_id
    JOIN player_stats ps ON tp.player_id = ps.player_id
    GROUP BY c.id, t.id, c.name, c.country, c.experience_years, t.name, t.position_league, ts.team_rating, ts.possession_pct
    ORDER BY t.position_league ASC, ts.team_rating DESC, total_team_goals DESC
    LIMIT 3
    """
    mycursor.execute(query)
    results = mycursor.fetchall()
    
    for result in results:
        print(f"Coach: {result[0]}, Team: {result[3]}, League Position: {result[4]}, Team Rating: {result[5]}")
        print(f"Average Player Rating: {result[7]:.2f}, Total Goals: {result[8]}, Total Assists: {result[9]}\n")

def top_performing_players():
    query = """
    SELECT 
    P.Name AS Player_Name, 
    SUM(PS.Goals) AS Total_Goals, 
    SUM(PS.Assists) AS Total_Assists, 
    SUM(PS.Goals + PS.Assists) AS Total_Contribution,
    P.Matches AS Total_Matches,
    P.Minutes AS Total_Minutes
    FROM 
        Player_Stats PS
    JOIN 
        Player P ON PS.player_id = P.id
    WHERE 
        P.Matches >= 1 AND P.Matches <= 38 -- Adjust range as needed
    GROUP BY 
        P.id, P.Name
    ORDER BY 
        Total_Contribution DESC
        , Total_Minutes ASC
    LIMIT 5;
    """
    mycursor.execute(query)
    results = mycursor.fetchall()
    
    for result in results:
        print(f"Player: {result[0]}, Total Goals: {result[1]}, Total Assists: {result[2]}, Total Contribution: {result[3]}, Total Matches: {result[4]}, Total Minutes: {result[5]}\n")
    
simple_query_1()
print("\n")
simple_query_2()
print("\n")
top_performing_coaches()
print("\n")
top_performing_players()