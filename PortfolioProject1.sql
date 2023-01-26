/*
European Soccer Database Data Exploration

Skills used: Joins, CTEs, Temp Tables, Window Functions, Aggregate Function, Procedures

*/

-- Use Temp Tables to wrangle data and perform calculations for Wins, Losses, Goals For, Goals Against

-- Create wins per team per season
CREATE TEMPORARY TABLE IF NOT EXISTS wins (
WITH winners AS (
	SELECT
		season
        ,league_id
		,CASE
			WHEN home_team_goal > away_team_goal THEN home_team_api_id
            WHEN away_team_goal > home_team_goal THEN away_team_api_id
		END AS winning_team
	FROM PortfolioProject.match
)
SELECT
	season
	,league_id
    ,winning_team
	,COUNT(*) as wins
FROM winners
WHERE winning_team IS NOT NULL
GROUP BY 1,2,3
);
-- Create losses per team per season
CREATE TEMPORARY TABLE IF NOT EXISTS losses (
WITH losers AS (
	SELECT
		league_id
        ,season
		,CASE
			WHEN away_team_goal > home_team_goal THEN home_team_api_id
            WHEN home_team_goal > away_team_goal THEN away_team_api_id
		END AS losing_team
	FROM PortfolioProject.match
)
SELECT
	season
    ,league_id
    ,losing_team
	,COUNT(*) as losses
FROM losers
WHERE losing_team IS NOT NULL
GROUP BY 1,2,3
);
-- Goals scored table
CREATE TEMPORARY TABLE IF NOT EXISTS goals_scored (
WITH goals_scored AS (
	SELECT 
		season
        ,league_id 
		,home_team_api_id as team_id
		,SUM(home_team_goal) AS goals
        ,MAX(stage) as games_played
	FROM PortfolioProject.match
    GROUP BY 1,2,3
	UNION
	SELECT
		season
        ,league_id
		,away_team_api_id
		,SUM(away_team_goal) AS goals
        ,MAX(stage) as games_played
	FROM PortfolioProject.match
    GROUP BY 1,2,3
)
SELECT 
	season
    ,team_id
    ,league_id
    ,SUM(goals) AS goals_scored
    ,MAX(games_played) as games_played
FROM goals_scored
GROUP BY 1,2,3
);
-- Goals conceded table
CREATE TEMPORARY TABLE IF NOT EXISTS goals_conceded (
WITH goals_conceded AS (
	SELECT 
		season
        ,league_id 
		,home_team_api_id as team_id
		,SUM(away_team_goal) AS goals
        ,MAX(stage) as games_played
	FROM PortfolioProject.match
    GROUP BY 1,2,3
	UNION
	SELECT
		season
        ,league_id
		,away_team_api_id
		,SUM(home_team_goal) AS goals
        ,MAX(stage) AS games_played
	FROM PortfolioProject.match
    GROUP BY 1,2,3
)
SELECT 
	season
    ,league_id
    ,team_id
    ,SUM(goals) AS goals_conceded
    ,MAX(games_played) as games_played
FROM goals_conceded
GROUP BY 1,2,3
);

-- Join the Temp Tables in order to create a league table for every season for every league with goals scored and conceded
-- Use row_number and partition by to assign league table position for every team in each season. Row_number instead of rank in order to avoid two teams being given same position.
-- Calculate points for teams (3 for a win, 1 for a draw) and draws. 
-- Output query result into new table to perform further data analysis
DROP TABLE IF EXISTS league_table;
CREATE TABLE IF NOT EXISTS league_table (
SELECT 
	w.season
    ,ROW_NUMBER() OVER (PARTITION BY league_id,season ORDER BY (w.wins*3) + (gs.games_played - w.wins - l.losses) DESC, gs.goals_scored - gc.goals_conceded DESC) AS position
    ,w.winning_team AS team_id
    ,w.league_id AS league_id
    ,t.team_long_name AS team
    ,gs.games_played AS Pld
    ,w.wins AS W
    ,gs.games_played - w.wins - l.losses AS D
    ,l.losses AS L
    ,(w.wins*3) + (gs.games_played - w.wins - l.losses) AS Pts
    ,gs.goals_scored - gc.goals_conceded AS GD
    ,gs.goals_scored AS GF
    ,gc.goals_conceded AS GA
FROM wins w
JOIN losses l
	ON w.winning_team = l.losing_team AND w.season = l.season
JOIN teams t
	ON w.winning_team = t.team_api_id
JOIN goals_scored gs
	ON w.winning_team = gs.team_id AND w.season = gs.season
JOIN goals_conceded gc
	ON w.winning_team = gc.team_id AND w.season = gc.season
ORDER BY w.season, league_id, ((w.wins*3) + (gs.games_played - w.wins - l.losses)) DESC, gs.goals_scored - gc.goals_conceded DESC
);


-- Premier League Analysis 

-- Winner by season
SELECT *
FROM league_table
WHERE position = 1 AND league_id = 1729;

-- Most league titles
SELECT team,COUNT(*) AS LeagueTitles
FROM league_table
WHERE league_id = 1729 AND position = 1
GROUP BY 1
ORDER BY 2 DESC;

-- Average points of league winner
SELECT AVG(Pts) AS AvgPointsWinner 
FROM league_table
WHERE position = 1 AND league_id = 1729;

-- Average points for 4th place (champions league football) 
SELECT AVG(Pts) AS AvgPointsTop4 
FROM league_table
WHERE position = 4 AND league_id = 1729;

-- Average points for 17th place (not in bottom 3, i.e. not relegated)
SELECT AVG(Pts) AS AvgPointsRelegation
FROM league_table
WHERE position = 17 AND league_id = 1729;

-- Best season (most points, most goals, highest goal difference)
SELECT season,team,Pld,Pts,position
FROM league_table
WHERE league_id = 1729
ORDER BY Pts DESC
LIMIT 5;

SELECT season,team,Pld,GF,position
FROM league_table
WHERE league_id = 1729
ORDER BY GF DESC
LIMIT 5;    

SELECT season,team,Pld,GD,position
FROM league_table
WHERE league_id = 1729
ORDER BY GD DESC
LIMIT 5;

-- Worst season (least points, least goals, lowest goal difference)
SELECT season,team,Pld,Pts,position
FROM league_table
WHERE league_id = 1729
ORDER BY Pts
LIMIT 5;

SELECT season,team,Pld,GF,position
FROM league_table
WHERE league_id = 1729
ORDER BY GF
LIMIT 5;    

SELECT season,team,Pld,GD,position
FROM league_table
WHERE league_id = 1729
ORDER BY GD
LIMIT 5;

-- Biggest wins
-- Create procedure to reuse for top 5 league analysis
// DELIMITER //
DROP PROCEDURE IF EXISTS BigWins;
CREATE PROCEDURE BigWins(IN LeagueID TEXT, NumGames INT)
BEGIN
SELECT
    team1.team_long_name AS HomeTeam
    ,CONCAT(home_team_goal,'-',away_team_goal) AS Score
    ,team2.team_long_name AS AwayTeam
    ,date
FROM PortfolioProject.match
JOIN teams team1
	ON PortfolioProject.match.home_team_api_id = team1.team_api_id
JOIN teams team2
	ON PortfolioProject.match.away_team_api_id = team2.team_api_id
WHERE FIND_IN_SET(league_id, LeagueID) -- Filter by league (id in league table)
ORDER BY ABS(home_team_goal - away_team_goal) DESC -- Sort by goal difference
LIMIT NumGames;
END //
DELIMITER ;

-- Pass in league ID and number of games
CALL BigWins('1729', 10)

-- Players with most appearances
-- Use unions to create a column with player 1-11 for home and away in order to group player id's and sum to get total appearances
-- Use CTEs to perform calculations and combine games played for different teams
-- Create procedure to reuse for top 5 league analysis
-- Dataset only considers players that started the match so therefore it is only total starting appearances

// DELIMITER //
DROP PROCEDURE IF EXISTS appearances;
CREATE PROCEDURE appearances(IN LeagueID TEXT, NumPlayers INT)
BEGIN
	WITH t1 AS (
		SELECT -- Player 1
			home_player_1 AS player_id,COUNT(home_player_1) AS Appearances,home_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT
			away_player_1 AS player_id,COUNT(away_player_1),away_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT -- Player 2
			home_player_2 AS player_id,COUNT(home_player_2) AS Appearances,home_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT
			away_player_2 AS player_id,COUNT(away_player_2),away_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT -- Player 3
			home_player_3 AS player_id,COUNT(home_player_3) AS Appearances,home_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT
			away_player_3 AS player_id,COUNT(away_player_3),away_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT -- Player 4
			home_player_4 AS player_id,COUNT(home_player_4) AS Appearances,home_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT
			away_player_4 AS player_id,COUNT(away_player_4),away_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT -- Player 5
			home_player_5 AS player_id,COUNT(home_player_5) AS Appearances,home_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT
			away_player_5 AS player_id,COUNT(away_player_5),away_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT -- Player 6
			home_player_6 AS player_id,COUNT(home_player_6) AS Appearances,home_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT
			away_player_6 AS player_id,COUNT(away_player_6),away_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT -- Player 7
			home_player_7 AS player_id,COUNT(home_player_7) AS Appearances,home_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT
			away_player_7 AS player_id,COUNT(away_player_7),away_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT -- Player 8
			home_player_8 AS player_id,COUNT(home_player_8) AS Appearances,home_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT
			away_player_8 AS player_id,COUNT(away_player_8),away_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT -- Player 9
			home_player_9 AS player_id,COUNT(home_player_9) AS Appearances,home_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT
			away_player_9 AS player_id,COUNT(away_player_9),away_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT -- Player 10
			home_player_10 AS player_id,COUNT(home_player_10) AS Appearances,home_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT
			away_player_10 AS player_id,COUNT(away_player_10),away_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT -- Player 11
			home_player_11 AS player_id,COUNT(home_player_11) AS Appearances,home_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
		UNION ALL
		SELECT
			away_player_11 AS player_id,COUNT(away_player_11),away_team_api_id AS team_id,league_id
		FROM PortfolioProject.match
		GROUP BY 1,3,4
)
	,t2 AS (
		SELECT
			p.player_name AS Player
			,SUM(t1.Appearances) AS TotalAppearances
			,t1.player_id AS player_id
			,t1.league_id
			,t1.team_id
		FROM t1
		JOIN player p
			ON t1.player_id = p.player_api_id
		WHERE FIND_IN_SET(league_id, LeagueID)
		GROUP BY t1.player_id, p.player_name, t1.league_id, t1.team_id
        )
	SELECT
		Player
        ,SUM(TotalAppearances) AS TotalAppearances
        ,player_id
    FROM t2
    GROUP BY 1,3
	ORDER BY SUM(TotalAppearances) DESC
	LIMIT NumPlayers;
END //
DELIMITER ;

-- Pass league ID and number of players
CALL appearances('1729', 20);

-- Top 5 League Analysis (England Premier League, France Ligue 1, Germany Bundesliga, Italy Serie A, Spain La Liga)

-- Most league titles won
SELECT team,COUNT(*) AS LeagueTitles
FROM league_table
WHERE league_id IN (1729, 4769, 7809, 10257, 21518) AND position = 1
GROUP BY 1
ORDER BY 2 DESC;

-- Average points for league winner in the top 5 leagues
SELECT l.name, AVG(Pts) AS AveragePointsWinner
FROM league_table lt
JOIN league l
	ON lt.league_id = l.id
WHERE league_id IN (1729, 4769, 7809, 10257, 21518) AND position = 1
GROUP BY l.name
ORDER BY 2 DESC;

-- Germany Bundesliga play less games so we take average points per game instead in order to compare

SELECT
	l.name
	,AVG(Pts) / lt.Pld AS AveragePointsPerGame
    ,lt.Pld
FROM league_table lt
JOIN league l
	ON lt.league_id = l.id
WHERE league_id IN (1729, 4769, 7809, 10257, 21518) AND position = 1
GROUP BY 1,3
ORDER BY 2 DESC;

-- Highest win percentage
SELECT
	season, team, W/Pld AS WinPerc
FROM league_table
ORDER BY W/Pld DESC
LIMIT 10;

-- Highest percentage of points won
SELECT
	season, team, Pts / (Pld*3) AS AvailablePointsWon
FROM league_table
WHERE league_id IN (1729, 4769, 7809, 10257, 21518)
ORDER BY 3 DESC
LIMIT 10;

-- Most wins all seasons combined
SELECT
	team, SUM(W) AS TotalWins
FROM league_table
WHERE league_id IN (1729, 4769, 7809, 10257, 21518)
GROUP BY team
ORDER BY SUM(W) DESC
LIMIT 10;

-- Biggest wins top 5 leagues
-- Pass in string of comma seperated league IDs and how many games
CALL BigWins('1729,4769,7809,10257,21518', 10);

-- Most appearances top 5 leagues
-- Pass in string of comma seperated league IDs and how many players
CALL appearances('1729,4769,7809,10257,21518', 50);