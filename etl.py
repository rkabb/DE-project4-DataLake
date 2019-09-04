#import configparser
from datetime import datetime
#import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col ,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format ,from_unixtime


#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function will create a spark session
    :return:
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function will take three parameters:
    spark: spark session
    input_data : S3 bucket where log files are stored
    output_data: S3 bucket where analyzed data would be stored

    This will process logs in Song_Data folder.
    Creates two dimension tables: SONGS and ARTISTS.
    Files are written in parquet format
    """

    # get filepath to song data files
    song_data = input_data + "/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").format("parquet").mode('overwrite').save( output_data + "songs")

    # extract columns to create artists table
    artist_table = df.select ("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artist_table.write.format("parquet").mode('overwrite').save(output_data + "artists")



def process_log_data(spark, input_data, output_data):
    """
    This function will take three parameters:
    spark: spark session
    input_data : S3 bucket where log files are stored
    output_data: S3 bucket where analyzed data would be stored

    This will process logs in Log_Data folder.
    Creates two dimension tables: TIME and USERS. and one fact table SONGPLAYS
    Files are written in parquet format
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data files
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userId","firstName" , "lastName" , "gender" ,"level" ).dropDuplicates(["userId"])
    
    # write users table to parquet files
    users_table.write.format("parquet").mode('overwrite').save(output_data + "users")

    # create timestamp column from original timestamp column
    df = df.select("ts").dropDuplicates(["ts"]).withColumn('ts1', from_unixtime ( df.ts/1000,'yyyy-MM-dd HH:mm:ss' ))

    # extract columns to create time table
    time_table = df.select(df.ts.alias("start_time") ,
           hour(df.ts1).alias("hour")  ,
           dayofmonth(df.ts1).alias("day") ,
           weekofyear(df.ts1).alias("week"),
           month(df.ts1).alias("month"),
           year(df.ts1).alias("year")
                    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").format("parquet").mode('overwrite').save(output_data + "time")


    # read in song data to use for songplays table. Read this data from the S3 bucket( Dimension table)
    songs_df = spark.read.format("parquet").load("s3n://sparkify-rk/songs").select("song_id","artist_id","title")

    # add year and month to the log_data that is in data frame DF above
    log_df = df.withColumn("year", year(from_unixtime(df.ts / 1000, 'yyyy-MM-dd HH:mm:ss'))) \
        .withColumn("month", month(from_unixtime(df.ts / 1000, 'yyyy-MM-dd HH:mm:ss'))) \
        .withColumn("songplay_id", monotonically_increasing_id()) \
        .select("songplay_id", df.ts.alias("start_time"), df.userId.alias("user_id"), "level", "song", \
                df.sessionId.alias("session_id"), "location", df.userAgent.alias("user_agent"), "year", "month")


    # extract columns from joined   log datasets to create songplays table
    songplays_table = log_df.join(songs_df, log_df.song == songs_df.title, 'left_outer') \
        .select("songplay_id", "start_time", "user_id", "level", "song_id", "artist_id", \
                "session_id", "location", "user_agent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").format("parquet").mode('overwrite').save(output_data + "songplays")



def main():
    """
     calls module to establish spark session.
     Source log files are located at bucket : s3a://udacity-dend/
     Processed parquet files are located at : s3a://sparkify-rk/
     Calls functions process_song_data and process_log_data
    """
    # create spark session
    spark = create_spark_session()

    # Source S3  bucket  where log files are located. This is provided by udacity
    input_data = "s3a://udacity-dend/"

    # target S3 bucket  where processed files are saved. This is hosted under my account
    output_data = "s3a://sparkify-rk/"

    process_song_data(spark, input_data, output_data)
    #process_log_data(spark, input_data, output_data)
    spark.stop()

if __name__ == "__main__":
    main()
