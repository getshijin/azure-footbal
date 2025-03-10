
def get_wikipedia_page(url):
    import requests

    print("Getting the wikipedia page..",url)

    try:
        response =requests.get(url,timeout=10)
        response.raise_for_status() #check if the request is succesfull

        return response.text
    except requests.RequestException as e:
        print(f"An error occured: {e}")

def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all("table",{"class": "wikitable sortable sticky-header"})[0]

    table_rows = table.find_all('tr')
    return table_rows

def extract_wikipedia_data(**kwargs):
    import pandas as pd
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    data = []

    for i in range(1,len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images':'https://'+ tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else 'NO-IMAGE',
            'home_team': clean_text(tds[6].text)
        }
        data.append(values)
    
    df = pd.DataFrame(data)
    df.to_csv("data/output.csv",index=False)
    return data

    def clean_text():
        text =str(text).strip()
        text = text.replace('&nbsp','')
        if text.find(" ♦"): text = text.split(' ♦')[0]
        if text.find('[') != -1: text = text.split('[')[0]
        if text.find( ' (formerly)') != -1: text = text.split(' (formerly)')[0]
        return  text.replace('\n','')