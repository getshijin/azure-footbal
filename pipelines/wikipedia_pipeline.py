
def get_wikipedia_page(url):
    import requests

    print("Getting the wikipedia page..",url)

    try:
        response =requests.get(url,timeout=10)
        response.raise_for_status() #check if the request is succesfull

        return response.text
    except requests.RequestException as e:
        print(f"An error occured: {e}")