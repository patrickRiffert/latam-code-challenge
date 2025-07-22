from typing import List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    spark = SparkSession.builder.appName("Users más influyentes").getOrCreate()
    sc = spark.sparkContext

    df = spark.read.json(file_path)
    # explode df making each row a user mention. After counts number of rows per username
    mentioned_users = df.select(explode('mentionedUsers.username').alias('username')).select('username')
    top_10_mentioned = (mentioned_users.groupBy('username')
                                        .agg(count('*').alias('mentions_count'))
                                        .orderBy(col("mentions_count").desc())
                                        .limit(10))

    result = [(row['username'], row['mentions_count']) for row in top_10_mentioned.collect()]                                        
    return result



if __name__ == "__main__":
    file_path = '../data/farmers-protest-tweets-2021-2-4.json'
    from memory_profiler import profile
    q3_time = profile(q3_time)
    q3_time(file_path)
