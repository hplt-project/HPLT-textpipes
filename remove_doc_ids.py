#! python3

import sys
import json

for line in sys.stdin:
    content = json.loads(line.strip())
    content.pop("id", None)
    print(json.dumps(content, ensure_ascii=False, separators=(",", ":")))

