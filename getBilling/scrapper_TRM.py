import requests
from bs4 import BeautifulSoup

def get_TRM():

    URL = 'https://www.dolar-colombia.com/en'
    response = requests.get(URL)
    soup = BeautifulSoup(response.text, 'html.parser')  
    TRM_scrapped = soup.find_all(attrs={'class': 'exchange-rate'})[0]
    
    return  float(TRM_scrapped.text.replace(',', ''))

    

if __name__ == '__main__':
    print(get_TRM())