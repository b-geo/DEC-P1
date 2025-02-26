- get csv of player rankings for last round of 2024 season
- get game results for r24 from csv (https://www.footywire.com/)
- get ladder results from end of r24 from csv (https://www.footywire.com/)

extract from api, csv
minor transformations to column names, unix time to normal time
load three tables
query those three tables using jinja to then produce a data warehouse

get games incomplete?
won't be any incremental extract from db

# ULTIMATELY OFFERING A VIEW OF GAMES FOR THE ROUND

**lil tricks i've done/ what i've learnt**
- _variable_name for internal values
- docstring generator, docstring for top of module
- doing it properly takes a lot longer!
- upsert games and odds since game_id for both. overwrite player rankings (need only active players)
- have made extract full only for postgres extract


assets
    functions for (basically main functions that get used):
        - extract_source, extract_source2
        - load
        - transform
        - logging stuff
[DONE] connectors (classes to connect)
    create class for connecting to api, db etc
        - separate class per connector
data
    static data
        - csv
logs
    folder of log files
pipeline
    - imports functions from assets, classes from connectors
    - runs whole thing with schedule, logging, pass .env for logging
    - .yaml for config


**questions for doug**
- is logging to file and db normal?
- jonathon had mentioned log guru instead of logging
    - do we use that and is metadata logging required?

**holds games for season**
EXTRACT
- games is all of season (slight incremental)
- tips are for games of current round and games not yet complete (incremental)
- player rankings is full cause players could leave

LOAD
- upsert all 2025 games
- tips are for current round so full overwrite
- player rankings are as of last season so full overwrite
- summary table is full overwrite

**broad order**
- code
- docstrings etc
- logging
- aws and 
- documentation
