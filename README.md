# crawler


Recursively crawl links from a given webpage in a breadth first (BFS) approach.

An UPPER_LIMIT can be set on the number of links to be crawled.

## USAGE:
Run Link crawler using:

python3 crawler.py URL UPPER_LIMIT

EXAMPLE:

python3 crawler.py https://google.com 100

NOTE:

Currently implemented in python3 only.

If no UPPER_LIMIT is specified the program will run indefinitely untill stopped manually.

The output will be saved in a file output.txt. It's path will be displayed upon script execution.
