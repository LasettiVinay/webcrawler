# webcrawler

A webcrawl application to crawl given urls and search for given text.

This application currently returns the matching text web page url links as output
## Setup environemnt

1. Setup python virtual environment (Python3)
  * Create virtual environment (first time activity)
    `python3 -m venv <give a name for virtual env>`
  * Activate python viratual environment (for unix/OS X)
    `source <path-to-venv-folder>/bin/activate`
  * Install dependencies along with upgrade pip
    `pip install --upgrade pip; pip install -r requirements.txt`

2. Run webcrawl application:
  * Activate python virtual env through step 01
  * Check usage help by running `python main.py -h`
  * Example usage to webcrawl and search some text
    `python main.py -u "<web-url>" --text "<search-text>"`
