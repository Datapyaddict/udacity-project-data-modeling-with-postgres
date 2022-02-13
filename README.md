# <u>Data Engineer nanodegree / Udacity project : data modelling with postgre</u>
## Table of Contents
1. [Project info](#project-info)
2. [Repository files info](#repository-files-info)
3. [Prerequisite to scripts run](#pre-requisite)
4. [Database modelling](#database-modelling)
5. [How to run the scripts](#how-to-run-the-scripts)

***

### Project info

Sparkify, a startup, wants its analytics team to analyse the data collected on songs and user activity from its music streaming app. 
The purpose of this project is implement a pipeline in python in order to extract, transform and load the information from JSON files into a postgre database and implement some analytics.
The JSON files consist of :
* log files containing information about the streaming activity of users,
* song files containing meta data of the songs.


***
### Repository files info

* `data/` folder contains the JSON files 
* `sql_queries.py` contains the sql statements to drop , create tables, insert data from the JSON files.
* `create_tables.py` is the script that drops and creates the database `sparkifydb`, connects to it, run the sql statements in `sql_queries.py` to drop and create the tables. 
* `etl.py` is the script that loads the JSON files into tables.
* `etl.ipynb` is the notebook that is used to implement all the steps run by the script `etl.py`.
* `test.ipynb` is the notebook used to check that all the data are correctly integrated into the database.
* `controls_and_analytics.ipynb` is the notebook running a few analytics queries on the database.


***
### Prerequisite to scripts run

* Create a [postgre database](https://www.postgresqltutorial.com/install-postgresql/)
* open Postgre `psql` terminal and run the sql statements included in  `initialization_of_studentdb_and_student_user.sql` in order to initialize studentdb database and student user.

***
## Database modelling

The database ERD is included in the repository.
The database model consists of :
* one fact table : `songplay`,
* dimension tables : `users`, `songs`, `artists` , `time`. 

The data model is a simple star schema.

Mapping rules from JSON files to the tables:

* __songplay__:

| column | source file.field  |
|:--------------|:-------------|
| start_time | *log_file.ts* |
| user_id | *log_file.userid* |
| level | *log_file.level* |
| song_id | *song_file.songid*|
| artist_id | *song_file.artist_id*|
| session_id | *log_file.sessionId*|
| location | *log_file.location*|
| user_agent | *log_file.userAgent*|

* __users__:

| column | source file.field  |
|:--------------|:-------------|
| user_id | *log_file.userid* |
| first_name | *log_file.firstName* |
| last_name | *log_file.lastName* |
| gender | *log_file.gender*|
| level | *log_file.level*|

* __songs__:

| column | source file.field  |
|:--------------|:-------------|
| song_id | *song_file.songid* |
| title | *song_file.title* |
| artist_id | *song_file.artist_id* |
| year | *song_file.year*|
| duration | *song_file.duration*|

* __artists__:

| column | source file.field  |
|:--------------|:-------------|
| artist_id | *song_file.artist_id* |
| name | *song_file.artist_name* |
| location | *song_file.artist_location* |
| latitude | *song_file.artist_latitude*|
| longitude | *song_file.artist_longitude*|

* __time__:

| column | source file.field  |
|:--------------|:-------------|
| start_time | *log_file.ts* |

**<u>Important</u>** : `song_id` and and `artist_id` columns in `songplay` table are derived from the `song_file` JSON file via the query `song_select` in `sql_queries.py`.
***

## How to run the scripts


Once the postgre `studentdb` database and `student` user are created, run the scripts in the following order :
> 1. run `create_tables.py` in the terminal in order to create the database `sparkifydb` if it does not exists, and drop/create the tables. This script is run once.
> 2. run `etl.py` in the terminal in order to load JSON files and insert data into tables.
> 3. run the jupyter notebook `test.ipynb` in order to control the correct data insertion. 
> 4. run `controls_and_analytics.ipynb` to get some analytics on the database.

**<u>Important</u>** : Each time the notebooks `test.ipynb` or `etl.ipynb`are run, remember to restart/shutdown them to close the connection to your database. Otherwise, you won't be able to run your code in create_tables.py, etl.py, since you can't make multiple connections to the same database.


