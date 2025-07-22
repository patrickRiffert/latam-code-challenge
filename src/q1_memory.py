from collections import Counter, defaultdict
from typing import List, Tuple
from datetime import datetime

from functions import read_json_lines_generator


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:

    nested_dict = defaultdict(Counter)
    for line in read_json_lines_generator(file_path):
        date_part = datetime.fromisoformat(line['date']).date()
        nested_dict[date_part][line['user']['username']] += 1
    
    # total tweet counts per date
    date_counts = {date: sum(user_counts.values()) for date, user_counts in nested_dict.items()}
    # top 10
    top_dates = sorted(date_counts, key=date_counts.get, reverse=True)[:10]
    result = []
    for date in top_dates:
        top_user = nested_dict[date].most_common(1)[0][0] # get userName
        result.append((date, top_user))
    return result


if __name__ == '__main__':
    file_path = '../data/farmers-protest-tweets-2021-2-4.json'

    from memory_profiler import profile
    q1_memory = profile(q1_memory)
    q1_memory(file_path)