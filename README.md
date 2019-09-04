# DE-project4-DataLake

#  ABOUT
This project is to support data analytics activity for a startup Sparkify. Sparkify's user base has grown bigger and
company wants to  move data warehouse to a data lake. Metadata about songs and user activity logs have been collected 
in S3 JSON format. This project is build an ETL pipeline, that extracts data from S3, process them using Spark and 
move data back to S3 buckets as fact/dimension tables to support star schema. 

# Log Data
We have two types of log files to process:
- metadata about songs. Located at --  s3://udacity-dend/song_data
- User activity log files located at --s3://udacity-dend/log_data

# Star Schema
After files are processed in spark, they are loaded back to S3 again with a data model of Star Schema.
Below are the tables and their details.

## Fact Table
**songplays**
- table to store log data associated with song played by users.Only teh records associated with page NextSong are stored
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables
**users** 
- table to store user information
- user_id, first_name, last_name, gender, level

**songs** 
- table to store songs information
- song_id, song_title, artist_id, year, duration

**artists**
- table to store srtists information 
- artist_id, name, location, latitude, longitude

**time**
- table to store start time information to do the necessary drill down on date/time
- start_time, hour, day, week, month, year, weekday

# How to run

- Create a EMR cluster on AWS. Required credentials should be specified in dl.cfg
  
- etl.py is the main python module. This will read the json log files. 
  Does any additional data wrangling required and writes the data back to S3 as per STAR schema.
  Data is written to destination S3 bucket in parquet format.

- To tun the module, login to cluster and run the below command:
  Spark-submit  --master yarn etl.py
  
