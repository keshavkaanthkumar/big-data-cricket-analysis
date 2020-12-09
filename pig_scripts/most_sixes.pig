raw_data = load '/home/keshav/dataset/Men ODI Player Innings Stats - 20th Century.csv' using PigStorage(',') AS (Innings_Player,Innings_Runs_Scored,Innings_Runs_Scored_Num,Innings_Minutes_Batted,Innings_Batted_Flag,Innings_Not_Out_Flag,Innings_Balls_Faced,Innings_Boundary_Fours,Innings_Boundary_Sixes,Innings_Batting_Strike_Rate,Innings_Number,Opposition,Ground,Innings_Date,Country,fifty,hundreds,Innings_Runs_Scored_Buckets,Innings_Overs_Bowled,Innings_Bowled_Flag,Innings_Maidens_Bowled,Innings_Runs_Conceded,Innings_Wickets_Taken,four_Wickets,five_Wickets,ten_Wickets,Innings_Wickets_Taken_Buckets,Innings_Economy_Rate);


data = FOREACH raw_data GENERATE Innings_Player,REPLACE(Innings_Boundary_Sixes,'-','0') as Innings_Boundary_Sixes:int;

player = GROUP data by Innings_Player;

most_sixes = FOREACH player GENERATE group as Innings_Player, SUM(data.Innings_Boundary_Sixes) as sixes;

order_by_sixes = ORDER most_sixes BY sixes DESC;

store order_by_sixes INTO '/home/keshav/output_final';
