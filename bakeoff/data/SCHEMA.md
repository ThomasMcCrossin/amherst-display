# board.json schema

Produced by `node bakeoff/build_board_data.mjs [--refresh]` (needs `HOCKEYTECH_API_KEY` in the environment; the key is never written to the file).
One self-contained snapshot, about 100 KB. Static pages fetch `bakeoff/data/board.json` and render offline.
Sources and caveats: `docs/board-data-inventory.md`. All values come straight from HockeyTech except where marked *derived*.
Dates are `YYYY-MM-DD`; `start_iso` has the America/Halifax offset. "opp"/"for"/"against" are relative to the Ramblers.

```
generated_at, season:"2026-27"
team:{id,name,abbr,nickname,city,division,logo_url, colors:null}      // colors: not provided by the API
next_game:{                                                           // null if the schedule has no unplayed game
  id,start_iso,home(bool),venue,status,tickets_url,flo_url,
  opponent:{id,name,abbr,logo_url,division,nickname,
    standings:{...standings row...}, record:{overall,home,away,last10,streak} (strings "W-L-OTL-SOL"),
    gf,ga, pp:{power_plays,power_play_goals,percentage}, pk:{times_short_handed,power_play_goals_against,percentage},
    leading_scorers:[{id,name,number,g,a,pts,pim,headshot_url}],
    goalies:[{id,name,number,gp,w,l,otl,sv_pct,gaa,so,starts_last5,headshot_url}],
    goalie_starts_last5:[{date,game_id,id,name,sa,ga}]},               // derived: who started their last 5 games per box score. History, not a prediction.
  ramblers:{record:{...}},
  h2h_this_season:[{id,date,home,score:{for,against},result,ot_so,attendance}],
  h2h_last_season:[...same, no id...],
  h2h_records:{this_season,last_season,last_5_years}:{home_team:{name,w,l,otl,sl,t,otw,sw},visiting_team:{...}},
  opponent_form:{last5:[{date,home,opp,gf,ga,result}] (opponent's perspective), last10, streak},
  official_starters:{home:null|{id,name,number,source}, away:null|{...}},  // null until an official lineup sets the start flag; never inferred
  lineup_note}
upcoming:[{id,start_iso,home,opp:{id,name,abbr,logo_url,division},venue,status}]   // next 7 unplayed
recent:[{id,date,start_iso,home,opp,venue,status,final,score:{for,against},result:"W|L|OTL|SOL",ot_so:null|"OT"|"SO",attendance,
  scorers:[goal...Ramblers only], goals:[{team,mine,period,time,scorer,scorer_id,assists[],pp,sh,en,gwg}],
  three_stars:[{star,name,number,team,id,line}], shots:{for,against}, pp:{for:"G/opps",against}, pim:{for,against},
  starting_goalie:{for:{id,name,sa,ga,sv,toi},against:{...}}}]     // newest first, last 8 finals
standings:{divisions:[{name,rows:[{rank,team,abbr,team_id,logo_url,is_ramblers,gp,w,l,otl,sol,pts,pct,rw,gf,ga,diff,pim,streak,last10,pp_pct,pk_pct,pp,playoff_position}]}],playoff_format,note}
team_stats:{record,record_note:"W-L-OTL-SOL",gp,pts,pct,division_rank,home,away,last10,streak,gf,ga,diff,gf_pg,ga_pg,pp_pct,pk_pct,pp,pim,pim_pg,
  shots_for_pg,shots_against_pg,shots_sample_games, league_ranks:{gf,ga,pp_pct,pk_pct,pim,points_pct,of_teams}, attendance:{home_games,avg_home}}
skaters:[{id,name,number,pos,shoots,age,birthdate,hometown,height,weight,rookie,headshot_url,gp,g,a,pts,ppg(points per game),pim,plus_minus,
  pp_goals,pp_points,sh_goals,gwg,ot_goals, last5:[{game_id,date,home,opp,g,a,pts,pim}] (newest first), career:{gp,g,a,pts}}]   // sorted by points; plus_minus is always 0 (not tracked)
goalies:[{id,name,number,catches,age,birthdate,hometown,height,weight,headshot_url,gp,gs,gs_source,w,l,otl,sol,sv_pct,gaa,so,saves,shots_against,ga,minutes,
  last_start_date, recent_games:[{game_id,date,home,opp,dec,ga,sa,sv,sv_pct,min,started}]}]   // gs derived from box-score start flag
league_leaders:{points:[...],goals:[...],assists:[...] top 10 {id,name,team,pos,number,gp,g,a,pts,ppg,headshot_url},
  goalies:{sv_pct,gaa,wins: top 5 (min 3 GP), min_gp}, ramblers_ranks:{points,goals,assists: top 3 Ramblers with league rank}}
milestones_near:[{id,name,number,stat,current,milestone,needs,scope}]   // derived from MHL career totals; windows: pts<=4, goals<=3, games<=4 away
streaks:{note,goal_streaks[],point_streaks[] (league top 8: {kind,id,name,team,games,g,a,pts,from,to,active,ramblers}),ramblers[],team_streaks[],current_team_streak}
sources[], copyright
```

Notes
* `result` OTL/SOL means the Ramblers lost in overtime/shootout. `otl` and `sol` are separate in standings; points as published.
* `official_starters` fills from, in order: statviewfeed `gameCenterPreview` `lineup.goalies[].starting==1`, statviewfeed `gameSummary` `goalies[].starting==1`, gc `gamesummary` `*_team_lineup.goalies[].start=="1"`. Each entry names its source.
* Attendance for finished games falls back to the schedule value when the box score has none.
* Raw responses are cached in `bakeoff/data/raw/` (gitignored). Finals never refresh; other responses refresh after 20 minutes. `--refresh` ignores the cache.
* Request volume per cold run: about 55 sequential requests with a 350 ms delay.
