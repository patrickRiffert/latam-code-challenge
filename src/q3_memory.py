import ujson
from typing import List, Tuple
from collections import Counter

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    counter = Counter()
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            tweet = ujson.loads(line)
            for user in (tweet.get('mentionedUsers') or []):
                counter[user['username']] += 1
    return counter.most_common(10)



if __name__ == "__main__":
    file_path = '../data/farmers-protest-tweets-2021-2-4.json'
    from memory_profiler import profile
    q3_memory = profile(q3_memory)
    q3_memory(file_path)
