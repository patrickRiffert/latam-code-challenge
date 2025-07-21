from typing import List, Tuple
from datetime import datetime

import duckdb


def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    con = duckdb.connect(database=':memory:')
    con.execute(f"""
        CREATE TABLE tweets AS 
        SELECT * FROM read_ndjson('{file_path}')
    """)
    con.execute("""
        CREATE TABLE tweets_with_date AS
        SELECT 
            CAST(strptime(tweets['date'], '%Y-%m-%dT%H:%M:%S%z') AT TIME ZONE 'UTC' AS DATE) AS date_only,
            tweets['user']['username'] AS username
        FROM tweets
    """)
    # top 10 dates
    top_dates = con.execute("""
        SELECT date_only, COUNT(*) as tweet_count
        FROM tweets_with_date
        GROUP BY date_only
        ORDER BY tweet_count DESC
        LIMIT 10
    """).fetchall()
    result = []
    for date_only, _ in top_dates:
        top_user = con.execute("""
            SELECT username
            FROM tweets_with_date
            WHERE date_only = ?
            GROUP BY username
            ORDER BY COUNT(*) DESC
            LIMIT 1
        """, [date_only]).fetchone()[0]
        result.append((date_only, top_user))
    con.close()
    return result


if __name__ == '__main__':
    file_path = '../data/farmers-protest-tweets-2021-2-4.json'

    from memory_profiler import profile
    q1_time = profile(q1_time)
    q1_time(file_path)
