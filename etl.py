import os
import configparser

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """
    Create the spark session with the passed configs.
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    Perform ETL on song_data to create the songs and artists dimensional tables: 
    - Process a single song data file and upload to database. 
    - Extract song data to insert records into table.
    - Extract artist data to insert records into table.
    
    Parameters:
    - spark: spark session
    - input_data : path to input files
    - output_data : path to output files
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration'].dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')
    print("--- songs.parquet completed ---")

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'].dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    print("--- artists.parquet completed ---")
    print("*** process_song_data completed ***\n\n")


def process_log_data(spark, input_data, output_data):
    
    """
     Perform ETL on log_data to create the time and users dimensional tables, 
    as well as the songplays fact table:
    - Process a single log file and load a single record into each table.
    - Extract time data to insert records for the timestamps into table.
    - Extract user data to insert records into table.
    - Extract and inserts data for songplays table from different tables.
    
    Parameters:
    - spark: spark session
    - input_data : path to input files
    - output_data : path to output files
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/2018/11/*.json")


    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    #df = 

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level'].dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    print("--- users.parquet completed ---")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType())
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)))
    get_hour = udf(lambda x: x.hour, T.IntegerType()) 
    get_day = udf(lambda x: x.day, T.IntegerType()) 
    get_week = udf(lambda x: x.isocalendar()[1], T.IntegerType()) 
    get_month = udf(lambda x: x.month, T.IntegerType()) 
    get_year = udf(lambda x: x.year, T.IntegerType()) 
    get_weekday = udf(lambda x: x.weekday(), T.IntegerType()) 

    df = df.withColumn("timestamp", get_timestamp(df.ts))
    df = df.withColumn('start_time', get_datetime(df.ts))
    df = df.withColumn("hour", get_hour(df.timestamp))
    df = df.withColumn("day", get_day(df.timestamp))
    df = df.withColumn("week", get_week(df.timestamp))
    df = df.withColumn("month", get_month(df.timestamp))
    df = df.withColumn("year", get_year(df.timestamp))
    df = df.withColumn("weekday", get_weekday(df.timestamp))
    
    
    # extract columns to create time table
    time_columns = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'] 
    
    # write time table to parquet files partitioned by year and month
    time_table = df[time_columns]
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    print("--- time.parquet completed ---")
    
    # read in song data to use for songplays table
    df_songs = spark.read.parquet(os.path.join(output_data, 'songs.parquet'))
    
    df_songplays = df_songs.join(df, (df_songs.title == df.song)).where(df.page == 'NextSong').orderBy(df.timestamp)
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_songplays['timestamp', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()

    # write songplays table to parquet files partitioned by year and month
    songplays_table\
    .withColumn("year", get_year(songplays_table.timestamp))\
    .withColumn("month", get_month(df.timestamp))\
    .write\
    .partitionBy('year', 'month')\
    .parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    
    print("--- songplays.parquet completed ---")
    print("*** process_log_data completed ***\n\nEND")


def main():
    
    """
    Build ETL Pipeline for Sparkify song play data:
    
    Call the function to create a spark session;
    Instantiate the input and output paths;
    Call the process functions.
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://fpmacedo/tables/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
