import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import boto3
import pyspark.sql.functions as F


def create_spark_session():

    """Creates a Spark session"""

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):

    """Processes the song data and creates a song table and an artist table"""

    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    df = spark.read.json(song_data)

    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()

    songs_table.write.parquet(os.path.join(output_data, 'songs.parquet'), mode='overwrite', partitionBy=['year', 'artist_id'])

    print('songs_table is completed')

    artists_table = df.select('artist_id',
                        F.col('artist_name').alias('name'),
                        F.col('artist_location').alias('location'),
                        F.col('artist_latitude').alias('latitude'),
                        F.col('artist_longitude').alias('longitude')).dropDuplicates()

    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), mode='overwrite')

    print('artists_table is completed')


def process_log_data(spark, input_data, output_data):

    """Processes the log data and creates a users table, a times table, and the songplays table"""

    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    df = spark.read.json(log_data)

    df = df.filter(df.page=='NextSong')

    users_table = df.select(
            F.col('userId').alias('user_id'),
            F.col('firstName').alias('first_name'),
            F.col('lastName').alias('last_name'),
            'gender',
            'level').dropDuplicates()

    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), mode='overwrite')

    print('users_table is completed')

    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('start_time', get_datetime(df.ts))

    time_table = df.select(
            'start_time',
            F.hour('start_time').alias('hour'),
            F.dayofmonth('start_time').alias('day'),
            F.weekofyear('start_time').alias('week'),
            F.month('start_time').alias('month'),
            F.year('start_time').alias('year'),
            F.dayofweek('start_time').alias('weekday')).dropDuplicates()

    time_table.write.parquet(os.path.join(output_data, 'time_table.parquet'), mode='overwrite', partitionBy=['year', 'month'])

    print('time_table is completed')

    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))

    df = df.join(song_df, song_df.title == df.song)

    songplays_table = df.select(
                    'start_time',
                    F.col('userId').alias('user_id'),
                    'level',
                    'song_id',
                    'artist_id',
                        F.col('sessionId').alias('session_id'),
                    F.col('artist_location').alias('location'),
                    F.col('userAgent').alias('user_agent'),
                    F.month('start_time').alias('month'),
                    F.year('start_time').alias('year')
                    )

    songplays_table = songplays_table.withColumn('songplay_id', F.monotonically_increasing_id())

    songplays_table.write.parquet(os.path.join(output_data, 'songplays.parquet'), mode='overwrite', partitionBy=['year', 'month'])

    print('songplays_table is completed')

def main():

    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config['DEFAULT']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['DEFAULT']['AWS_SECRET_ACCESS_KEY']
    os.environ['DATA_LAKE'] = config['DEFAULT']['DATA_LAKE']

    input_data = config['DEFAULT']['INPUT_DATA']
    output_data = config['DEFAULT']['OUTPUT_DATA']

    spark = create_spark_session()

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
