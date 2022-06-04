from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
import requests
import re
import time
import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override() # <== that's all it takes :-)

def extract(d,path):
    soup = BeautifulSoup(d,'html.parser')
    groups = soup.find_all('accordion-group')

    if len(groups) != 12:
        raise Exception('The length of groups is not correct')
    records = []

    for group in groups:
        title = group.find_all('div',class_='accordion-toggle')[0].text.strip()
        table = group.find_all('table')[0]
        sector = re.findall(r'[-]{1} ([a-zA-Z0-9&\s]+)',title)[0]

        if sector == 'Healthcare':
            sector = 'Health Care'
        if sector == 'Conglomerates':
            continue

        rows = table.find_all('tr')

        for r in rows:
            code,name = r.find_all('td')

            if code.text.strip() == 'Code':
                continue

            code,name = code.text.strip(),name.text.strip()
            
            if 4-len(code) != 0:
                code = '0'*(4-len(code)) + code + '.HK'
            else:
                code = code + '.HK'
            records.append([code,name,sector])

    col_names =['Code','Name','Sector']
    df_hsci = pd.DataFrame(records,columns=col_names)
    # print('\n',df_hsci)
    df_info = pdr.get_quote_yahoo(df_hsci['Code'])
    
    df_hsci['Exchange'] = df_info['exchange'].values
    df_hsci['CCY'] = df_info['currency'].values
    df_hsci['CurrentPrice'] = df_info['price'].values
    df_hsci['MarketCap(Billions)'] = df_info['marketCap'].fillna(0).values/1000000000

    df_hsci.to_csv(path,index=False)
    print('\n Done')


if __name__=="__main__":
    source = 'https://www.hsi.com.hk/eng/indexes/all-indexes/industry'
    csv_file = 'data/hsci_companies_info.csv'
    driver = webdriver.Chrome()
    driver.get(source)
    time.sleep(7)
    c = driver.page_source
    driver.quit()
    extract(c,csv_file)

