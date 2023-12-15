import mechanicalsoup
import pandas as pd
import sqlite3
import redis

def crawler(url, r):
    browser = mechanicalsoup.StatefulBrowser()
    browser.open(url)

    # Parse the page for links
    links = browser.page.find_all("a", href=True)
    for link in links:
        # Add found links to Redis Queue
        r.lpush('link_queue', link['href'])

    th = browser.page.find_all("th", attrs={"class": "table-rh"})
    distribution = [value.text.replace("\n", "") for value in th]
    distribution = distribution[:98]

    td = browser.page.find_all("td")
    columns = [value.text.replace("\n", "") for value in td]
    columns = columns[6:1084]

    column_names = [
        "Founder", "Maintainer", "Initial_Release_Year", "Current_Stable_Version",
        "Security_Updates", "Release_Date", "System_Distribution_Commitment",
        "Forked_From", "Target_Audience", "Cost", "Status"
    ]

    dictionary = {"Distribution": distribution}

    for idx, key in enumerate(column_names):
        dictionary[key] = columns[idx:][::11]

    df = pd.DataFrame(data=dictionary)

    connection = sqlite3.connect("distro.db")
    cursor = connection.cursor()
    cursor.execute("create table if not exists linux (Distribution, " + ",".join(column_names) + ")")
    for i in range(len(df)):
        cursor.execute("insert into linux values (?,?,?,?,?,?,?,?,?,?,?,?)", df.iloc[i])

    connection.commit()
    connection.close()

    next_link = r.rpop('link_queue')
    if next_link:
        crawler(next_link, r)

# Starting URL for the recursive crawl
starting_url = "https://en.wikipedia.org/wiki/Comparison_of_Linux_distributions"
r = redis.StrictRedis(host='localhost', port=6379, db=0)
crawler(starting_url, r)
