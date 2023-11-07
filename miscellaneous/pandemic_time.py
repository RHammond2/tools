"""
The current date in March since the pandemic started and time stopped feeling
meaningful. Based on this blog post, but made for Python:
https://pluralistic.net/2020/08/26/destroy-surveillance-capitalism/#blursday

Python
------
Any

Dependencies
------------
None
"""

import datetime

def main():
    pandemic = datetime.datetime(2020, 3, 1)
    current = datetime.datetime.now()
    diff = current - pandemic
    print(f"Current time: March {diff.days}, 2020 {current.strftime('%H:%M')}")


if __name__ == "__main__":
    main()