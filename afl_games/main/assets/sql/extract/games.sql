{% set config = {
    "extract_type": "full",
    "source_table_name": "games"
} %}

select
    game_id,
    game_data_updated,
    game_time,
    home_team,
    away_team,
    round
from
    {{ config["source_table_name"] }}
{% if optional_where %}
where
    {{optional_where}}
{% endif %}