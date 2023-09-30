import mechanicalsoup       # <3
import pandas as pd
import sqlite3

url = "https://en.wikipedia.org/wiki/List_of_Alexander_McQueen_collections"
browser = mechanicalsoup.StatefulBrowser()
browser.open(url)

th = browser.page.find_all("th", attrs={"class": "table-rh"})
distribution = [value.text.replace("\n", "") for value in th]
# print(distribution.index("Zorin OS"))
distribution = distribution[:36]

# print(distribution)

td = browser.page.find_all("td")
columns = [value.text.replace("\n", "") for value in td]
# print(columns)
# print(columns.index("AlmaLinux Foundation"))
# print(columns.index("Binary blobs"))

columns = columns[6:20]
print(columns)

column_names = ["Collection",
                "Season",
                "Show_date",
                "Show_location", 
                "Themes_and_inspiration", 
                "Notes"
                ]

# column[0:][::11]
# column[1:][::11]
# column[2:][::11]

dictionary = {"Distribution": distribution}

for idx, key in enumerate(column_names):
    dictionary[key] = columns[idx:][::6]


df = pd.DataFrame(data = dictionary)        
#^ Extracts information about different Linux distributions and stores it in a Pandas DataFrame.
print(df.head())
print(df.tail())

connection = sqlite3.connect("distro.db")
#^ Creates a SQLite database named "distro.db" and stores the data in a table named "linux."
cursor = connection.cursor()
cursor.execute("create table McQueen (Distribution, " + ",".join(column_names) + ")")
for i in range(len(df)):
    cursor.execute("insert into McQueen values (?,?,?,?,?,?)", df.iloc[i])

connection.commit()     #Commit changes to database

connection.close()      #Closes the connection with the server (database connection)

