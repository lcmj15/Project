---pivot games with infos
select L.GAME_DATE_EST,
L.GAME_ID,
GAME_STATUS_TEXT,
HOME_TEAM_ID as team_id,
VISITOR_TEAM_ID as enemy_team_id,
NICKNAME,
ABBREVIATION,
R.SEASON,
R.FASE,
R2.CONFERENCE,
PTS_HOME as PTS,
FG_PCT_HOME as FG_PCT,
FT_PCT_HOME as FT_PCT,
FG3_PCT_HOME as FG3_PCT,
AST_HOME as AST,
REB_HOME as REB,
HOME_TEAM_WINS as WINS
from {{ source('nba_source', 'games') }} L
inner join {{ source('nba_source', 'nba_seasons') }} R on L.game_date_est >= data_inicio
    and L.game_date_est <= data_fim
inner join {{ source('nba_source', 'team_season_conference') }} R2 on L.HOME_TEAM_ID = R2.TEAM_ID
    and L.game_date_est >= R2.data_inicio
    and L.game_date_est <= R2.data_fim
inner join {{ source('nba_source', 'teams') }} R3 on R3.TEAM_ID = L.HOME_TEAM_ID
--where R.SEASON = '2021-22'
union
---pivot games with infos
select L.GAME_DATE_EST,
L.GAME_ID,
GAME_STATUS_TEXT,
VISITOR_TEAM_ID as team_id,
HOME_TEAM_ID as enemy_team_id,
NICKNAME,
ABBREVIATION,
R.SEASON,
R.FASE,
R2.CONFERENCE,
PTS_AWAY as PTS,
FG_PCT_AWAY as FG_PCT,
FT_PCT_AWAY as FT_PCT,
FG3_PCT_AWAY as FG3_PCT,
AST_AWAY as AST,
REB_AWAY as REB,
case when HOME_TEAM_WINS = 1 then 0 else 1 end as WINS
from {{ source('nba_source', 'games') }} L
inner join {{ source('nba_source', 'nba_seasons') }} R on L.game_date_est >= data_inicio
    and L.game_date_est <= data_fim
inner join {{ source('nba_source', 'team_season_conference') }} R2 on L.HOME_TEAM_ID = R2.TEAM_ID
    and L.game_date_est >= R2.data_inicio
    and L.game_date_est <= R2.data_fim
inner join {{ source('nba_source', 'teams') }} R3 on R3.TEAM_ID = L.HOME_TEAM_ID
--where R.SEASON = '2021-22'