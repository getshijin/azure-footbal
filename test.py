import requests
from bs4 import BeautifulSoup

url = "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)

if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all("table", {"class": "wikitable sortable sticky-header"})
    if tables:
        table = tables[0]
        print((table))
    else:
        print("No matching table found")
else:
    print("Failed to fetch page:", response.status_code)
