# -*- coding: utf-8 -*-
"""
@author: Kapil Chandorikar
"""

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time


def get_random_user_agent():
    r"""Return a random user agent from the file."""
    try:
        with open('user_agents.txt') as file:
            lines = file.readlines()
            if len(lines) > 0:
                ua = np.random.choice(lines).strip()
    except Exception as ex:
        print('Exception in get_random_user_agent()')
        print(str(ex))
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
    finally:
        return ua


def random_delay():
    r"""Delays the execution of the code by a random number of seconds."""
    delays = [0, 1, 2, 3, 4, 5]
    delay = np.random.choice(delays)
    print("Delaying by {} seconds.".format(delay))
    time.sleep(delay)


# Scraping
def process_page():
    r"""Fetches the page by sending a request to the server, parses the data and stores them in pandas DataFrame structure."""
    global year
    global players_df

    year_df = pd.DataFrame(columns=["Player", "Country", "{}".format(year)])

    page = requests.get(
        'http://www.howstat.com/cricket/Statistics/Batting/BattingMostRunsYear_ODI.asp',
        params={
            "Year": "{}".format(year),
            "SortOrder": "Player"
        },
        headers={
            "Host":
            "www.howstat.com",
            "User-Agent":
            get_random_user_agent(),
            "Accept":
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
            "Referer":
            "http://www.howstat.com/cricket/Statistics/Batting/BattingMenu.asp",
            "Accept-Encoding":
            "gzip, deflate",
            "Accept-Language":
            "en-US,en"
        })

    soup = BeautifulSoup(page.text, 'html.parser')

    table = soup.find(
        'table',
        attrs={
            'class': 'TableLined',
            'cellspacing': '0',
            'cellpadding': '4',
            'width': '600'
        })

    rows = table.findAll('tr')
    for row in rows[1:]:
        cols = row.findAll('td')
        player = cols[1].text.replace("*", "").strip()
        country = cols[2].text.strip()
        runs = cols[6].text.replace("*", "").replace("-", "0")
        temp = pd.Series([player, country, runs], index=year_df.columns)
        year_df = year_df.append(temp, ignore_index=True)

    print("{} records".format(year_df.shape[0]))

    year_df.fillna(0, inplace=True)
    year_df[["{}".format(year)]] = year_df[["{}".format(year)]].apply(
        pd.to_numeric)
    players_df = pd.merge(players_df, year_df, how='outer', sort=False)
    players_df = players_df.groupby(["Player", "Country"]).sum().reset_index()


players_df = pd.DataFrame(columns=["Player", "Country"])
for year in range(1971, 2020):
    print("\nYear: {}".format(year))
    random_delay()
    process_page()

# saving the collected data as a CSV file
# players_df.to_csv('fetched_data_howstat.csv')

players_df.set_index("Player", inplace=True)
cs_df = players_df.iloc[:, 1:].cumsum(axis=1)
cs_df.columns = [
    "CS1971", "CS1972", "CS1973", "CS1974", "CS1975", "CS1976", "CS1977",
    "CS1978", "CS1979", "CS1980", "CS1981", "CS1982", "CS1983", "CS1984",
    "CS1985", "CS1986", "CS1987", "CS1988", "CS1989", "CS1990", "CS1991",
    "CS1992", "CS1993", "CS1994", "CS1995", "CS1996", "CS1997", "CS1998",
    "CS1999", "CS2000", "CS2001", "CS2002", "CS2003", "CS2004", "CS2005",
    "CS2006", "CS2007", "CS2008", "CS2009", "CS2010", "CS2011", "CS2012",
    "CS2013", "CS2014", "CS2015", "CS2016", "CS2017", "CS2018", "CS2019"
]
players_df = pd.merge(
    players_df,
    cs_df,
    how='outer',
    sort=False,
    left_index=True,
    right_index=True)
print(players_df)

# saving the processed data as a CSV file
players_df.to_csv('players_data_howstat.csv')