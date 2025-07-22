import json
from typing import Generator


def read_json_lines_generator(file_path: str) -> Generator[dict]:
    with open(file_path, 'r') as f:
        for line in f:
            yield json.loads(line)