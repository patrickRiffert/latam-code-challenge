from typing import List, Tuple
from collections import Counter

import emoji

from functions import read_json_lines_generator


def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    emoji_counter = Counter()
    for line in read_json_lines_generator(file_path):
        tweet = line['content']
        for emoji_dict in emoji.emoji_list(tweet):
            emoji_counter[emoji_dict['emoji']] += 1

    return emoji_counter.most_common(10)



if __name__ == "__main__":
    file_path = '../data/farmers-protest-tweets-2021-2-4.json'
    from memory_profiler import profile
    q2_memory = profile(q2_memory)
    q2_memory(file_path)