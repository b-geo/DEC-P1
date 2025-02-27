import time
from datetime import datetime, timezone
print(int(time.time()))

print(datetime.now())

s= "2025-02-27T10:53:32.761107+0800 | INFO | Transforming player fantasy data to group by team."
res = len(s.encode('utf-8'))
print(str(res))