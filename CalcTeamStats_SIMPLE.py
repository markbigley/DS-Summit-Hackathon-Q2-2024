
import pandas as pd


AllSeasonsDF = pd.read_csv('MRegularSeasonDetailedResults.csv')

# add in team names
KaggleTeamDF = pd.read_csv('MTeams.csv')
ConfDF = pd.read_csv('MTeamConferences.csv')
ConfDF = ConfDF.merge(KaggleTeamDF[['TeamID','TeamName']],how='left',on='TeamID')

AllSeasonsDF = AllSeasonsDF.merge(KaggleTeamDF[['TeamID','TeamName']],how='left',
                                    left_on='WTeamID',right_on='TeamID')
AllSeasonsDF['WTeamName'] = AllSeasonsDF['TeamName']
AllSeasonsDF.drop(['TeamID','TeamName'],axis=1,inplace=True)

AllSeasonsDF = AllSeasonsDF.merge(KaggleTeamDF[['TeamID','TeamName']],how='left',
                                    left_on='LTeamID',right_on='TeamID')
AllSeasonsDF['LTeamName'] = AllSeasonsDF['TeamName']
AllSeasonsDF.drop(['TeamID','TeamName'],axis=1,inplace=True)

statlist = ['Score','FGM', 'FGA', 'FGM3', 'FGA3', 'FTM', 'FTA', 'OR', 'DR',
            'Ast', 'TO', 'Stl', 'Blk', 'PF']

yearlist = AllSeasonsDF.Season.unique()

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
AllSeasTeamStatsDF = pd.DataFrame()

for season in yearlist:
    print(f'workin on {season} Season...')
    
    SeasonGameDF = AllSeasonsDF[AllSeasonsDF.Season==season]
    
    teamlist = list(set( SeasonGameDF['WTeamName'].values.tolist() + SeasonGameDF['LTeamName'].values.tolist() ))
    
    SeasTeamDF = pd.DataFrame(teamlist,columns=['Team'])
    SeasTeamDF['Season'] = season
        
    # put the team names in the index also
    SeasTeamDF.index = SeasTeamDF.Team
    
    # count wins and losses
    SeasTeamDF['W'] = SeasonGameDF[['WTeamName','WScore']].groupby('WTeamName').count()
    SeasTeamDF['W'].fillna(0,inplace=True)
    SeasTeamDF['L'] = SeasonGameDF[['LTeamName','LScore']].groupby('LTeamName').count()
    SeasTeamDF['L'].fillna(0,inplace=True)
    
    SeasTeamDF['NumGames'] = SeasTeamDF[['W','L'] ].sum(axis=1)
    
    # sum stats
    
    for stat in statlist:
        tempw = pd.DataFrame( SeasonGameDF[['WTeamName','W'+stat]].groupby('WTeamName').sum() )
        templ = pd.DataFrame( SeasonGameDF[['LTeamName','L'+stat]].groupby('LTeamName').sum() )
        tempdf = tempw.merge(templ,how='outer',left_index=True,right_index=True)
        tempdf.fillna(0,inplace=True,axis=1)
        # SeasTeamDF[stat+'_tot'] = tempw['W'+stat] + templ['L'+stat]
        SeasTeamDF[stat+'_tot'] = tempdf.sum(axis=1)
        
    # sum opponenet stats
    for stat in statlist:
        tempw = pd.DataFrame( SeasonGameDF[['WTeamName','L'+stat]].groupby('WTeamName').sum() )
        templ = pd.DataFrame( SeasonGameDF[['LTeamName','W'+stat]].groupby('LTeamName').sum() )
        tempdf = tempw.merge(templ,how='outer',left_index=True,right_index=True)
        tempdf.fillna(0,inplace=True,axis=1)
        # SeasTeamDF['opp_'+stat+'_tot'] =  tempw['L'+stat] + templ['W'+stat]
        SeasTeamDF['opp_'+stat+'_tot'] = tempdf.sum(axis=1)
    
        
    AllSeasTeamStatsDF = pd.concat([AllSeasTeamStatsDF,SeasTeamDF],ignore_index=True)
    
    
    
    