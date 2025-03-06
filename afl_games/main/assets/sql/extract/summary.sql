select
	g.game_id,
	g.game_time,
	g.round,
	g.home_team,
	tf1.team_fantasy_score as home_team_fantasy,
	g.away_team, tf2.team_fantasy_score as away_team_fantasy,
	o.tip,
	o.projected_margin,
	o.confidence
from games g
inner join odds o on g.game_id = o.game_id
inner join team_fantasy tf1 on g.home_team = tf1.team
inner join team_fantasy tf2 on g.away_team = tf2.team
where
	g.game_time::timestamptz > now()
		at time zone substring(g.game_time from '[+-]\d{2}:\d{2}')
order by g.game_id
