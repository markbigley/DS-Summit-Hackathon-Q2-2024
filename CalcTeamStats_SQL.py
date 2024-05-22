from pandasql import sqldf
import pandas as pd
pysqldf = lambda q: sqldf(q, globals())

# Setting Path Variables
RegularSeasonPath = "Data/MRegularSeasonDetailedResults.csv"
TeamsPath = "Data/MTeams.csv"

m_regular_season_detailed_results = pd.read_csv(RegularSeasonPath)
m_teams = pd.read_csv(TeamsPath)

SqlScript = """
  WITH season_stats_prep AS (
    SELECT 'M'          AS Tournament
         , rsd1.Season
         , rsd1.WTeamID AS TeamID
         , ti1.TeamName AS TeamNM
         , CASE WHEN (ti1.LastD1Season >= rsd1.Season AND ti1.FirstD1Season <= rsd1.Season)
                 AND (lti1.LastD1Season >= rsd1.Season AND lti1.FirstD1Season <= rsd1.Season) THEN 1 ELSE 0 END AS both_d1_teams_flg
         , 1            AS WinFlg
         , rsd1.WFGM    AS FieldGoalsMade
         , rsd1.WFGA    AS FieldGoalsAttempted
         , rsd1.WFGM3   AS ThreePointersMade
         , rsd1.WFGA3   AS ThreePointersAttempted
         , rsd1.WFTM    AS FreeThrowsMade
         , rsd1.WFTA    AS FreeThrowsAttempted
         , rsd1.WOR     AS OffensiveRebounds
         , rsd1.WDR     AS DefensiveRebounds
         , rsd1.WAst    AS Assists
         , rsd1.WTO     AS TurnoversCommitted
         , rsd1.WStl    AS Steals
         , rsd1.WBlk    AS Blocks
         , rsd1.WPF     AS PersonalFouls
         , rsd1.LFGM    AS OppFieldGoalsMade
         , rsd1.LFGA    AS OppFieldGoalsAttempted
         , rsd1.LFGM3   AS OppThreePointersMade
         , rsd1.LFGA3   AS OppThreePointersAttempted
         , rsd1.LFTM    AS OppFreeThrowsMade
         , rsd1.LFTA    AS OppFreeThrowsAttempted
         , rsd1.LOR     AS OppOffensiveRebounds
         , rsd1.LDR     AS OppDefensiveRebounds
         , rsd1.LAst    AS OppAssists
         , rsd1.LTO     AS OppTurnoversCommitted
         , rsd1.LStl    AS OppSteals
         , rsd1.LBlk    AS OppBlocks
         , rsd1.LPF     AS OppPersonalFouls
    FROM m_regular_season_detailed_results AS rsd1
    LEFT JOIN m_teams AS ti1
      ON rsd1.WTeamID = ti1.TeamID
    LEFT JOIN m_teams AS lti1
      ON rsd1.LTeamID = lti1.TeamID

    UNION ALL

    SELECT 'M'          AS Tournament
         , rsd2.Season
         , rsd2.LTeamID AS TeamID
         , ti2.TeamName AS TeamNM
         , CASE WHEN (ti2.LastD1Season >= rsd2.Season AND ti2.FirstD1Season <= rsd2.Season)
                 AND (wti2.LastD1Season >= rsd2.Season AND wti2.FirstD1Season <= rsd2.Season) THEN 1 ELSE 0 END AS both_d1_teams_flg
         , 0            AS WinFlg
         , rsd2.LFGM    AS FieldGoalsMade
         , rsd2.LFGA    AS FieldGoalsAttempted
         , rsd2.LFGM3   AS ThreePointersMade
         , rsd2.LFGA3   AS ThreePointersAttempted
         , rsd2.LFTM    AS FreeThrowsMade
         , rsd2.LFTA    AS FreeThrowsAttempted
         , rsd2.LOR     AS OffensiveRebounds
         , rsd2.LDR     AS DefensiveRebounds
         , rsd2.LAst    AS Assists
         , rsd2.LTO     AS TurnoversCommitted
         , rsd2.LStl    AS Steals
         , rsd2.LBlk    AS Blocks
         , rsd2.LPF     AS PersonalFouls
         , rsd2.WFGM    AS OppFieldGoalsMade
         , rsd2.WFGA    AS OppFieldGoalsAttempted
         , rsd2.WFGM3   AS OppThreePointersMade
         , rsd2.WFGA3   AS OppThreePointersAttempted
         , rsd2.WFTM    AS OppFreeThrowsMade
         , rsd2.WFTA    AS OppFreeThrowsAttempted
         , rsd2.WOR     AS OppOffensiveRebounds
         , rsd2.WDR     AS OppDefensiveRebounds
         , rsd2.WAst    AS OppAssists
         , rsd2.WTO     AS OppTurnoversCommitted
         , rsd2.WStl    AS OppSteals
         , rsd2.WBlk    AS OppBlocks
         , rsd2.WPF     AS OppPersonalFouls
    FROM m_regular_season_detailed_results AS rsd2
    LEFT JOIN m_teams AS ti2
      ON rsd2.LTeamID = ti2.TeamID
    LEFT JOIN m_teams AS wti2
      ON rsd2.WTeamID = wti2.TeamID
  )
  SELECT Tournament
       , Season
       , TeamID
       , TeamNM
       , Count(*)                       AS TotalSeasonGames
       , SUM(WinFlg)                    AS TotalSeasonWins
       , COUNT(*)-SUM(WinFlg)           AS TotalSeasonLosses
       , SUM(FieldGoalsMade)            AS TotalSeasonFieldGoalsMade
       , SUM(FieldGoalsAttempted)       AS TotalSeasonFieldGoalsAttempted
       , SUM(ThreePointersMade)         AS TotalSeasonThreePointersMade
       , SUM(ThreePointersAttempted)    AS TotalSeasonThreePointersAttempted
       , SUM(FreeThrowsMade)            AS TotalSeasonFreeThrowsMade
       , SUM(FreeThrowsAttempted)       AS TotalSeasonFreeThrowsAttempted
       , SUM(OffensiveRebounds)         AS TotalSeasonOffensiveRebounds
       , SUM(DefensiveRebounds)         AS TotalSeasonDefensiveRebounds
       , SUM(Assists)                   AS TotalSeasonAssists
       , SUM(TurnoversCommitted)        AS TotalSeasonTurnoversCommitted
       , SUM(Steals)                    AS TotalSeasonSteals
       , SUM(Blocks)                    AS TotalSeasonBlocks
       , SUM(PersonalFouls)             AS TotalSeasonPersonalFouls
       , SUM(OppFieldGoalsMade)         AS TotalSeasonOppFieldGoalsMade
       , SUM(OppFieldGoalsAttempted)    AS TotalSeasonOppFieldGoalsAttempted
       , SUM(OppThreePointersMade)      AS TotalSeasonOppThreePointersMade
       , SUM(OppThreePointersAttempted) AS TotalSeasonOppThreePointersAttempted
       , SUM(OppFreeThrowsMade)         AS TotalSeasonOppFreeThrowsMade
       , SUM(OppFreeThrowsAttempted)    AS TotalSeasonOppFreeThrowsAttempted
       , SUM(OppOffensiveRebounds)      AS TotalSeasonOppOffensiveRebounds
       , SUM(OppDefensiveRebounds)      AS TotalSeasonOppDefensiveRebounds
       , SUM(OppAssists)                AS TotalSeasonOppAssists
       , SUM(OppTurnoversCommitted)     AS TotalSeasonOppTurnoversCommitted
       , SUM(OppSteals)                 AS TotalSeasonOppSteals
       , SUM(OppBlocks)                 AS TotalSeasonOppBlocks
       , SUM(OppPersonalFouls)          AS TotalSeasonOppPersonalFouls
  FROM season_stats_prep
  GROUP BY Tournament
         , Season
         , TeamID
         , TeamNM;
"""

# Run Transformation
hist_df = pysqldf(SqlScript)
# print(hist_df.head())

# Uncomment this to export as CSV
# hist_df.to_csv('TeamStatHist.csv')