- get csv of player rankings for last round of 2024 season
- get game results for r24 from csv (https://www.footywire.com/)
- get ladder results from end of r24 from csv (https://www.footywire.com/)

extract from api, csv
minor transformations to column names, unix time to normal time
load three tables
query those three tables using jinja to then produce a data warehouse

**lil tricks i've done/ what i've learnt**
- _variable_name for internal values
- docstring generator, docstring for top of module
- doing it properly takes a lot longer!


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
- when to pass round number, year - get current round?
- if loading multiple csv should they be a connector?
- is there another folder format that is as good?