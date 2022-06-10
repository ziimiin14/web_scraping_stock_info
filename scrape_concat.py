from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
import requests
import re
import os
import time
import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override() # <== that's all it takes :-)


def extract_sti():
    source = 'https://sginvestors.io/market/sgx-share-price-performance/straits-times-index-constituents'
    source_mc = 'https://sginvestors.io/analysts/sti-straits-times-index-constituents-target-price'
    d = requests.get(source)
    d_mc = requests.get(source_mc)

    soup = BeautifulSoup(d.content,'html.parser')
    soup_mc = BeautifulSoup(d_mc.content,'html.parser')

    table = soup.find('div',{'itemtype':'https://schema.org/ItemList'})
    table_mc = soup_mc.find('table',{'id':'stock-list-analyst-target-price'})

    header = table.findAll('th')[1]
    header_mc = table_mc.findAll('th')[1]

    if header.text.strip() != 'Straits Times Index (STI) Constituent':
        raise Exception("Can't parse SGX Market Info")
    if header_mc.text.strip() != 'Straits Times Index STI Constituent':
        raise Exception("Can't get MarketCap data")

    records = []
    records_mc = []
    rows = table.findAll('tr',{'class':'price-performance'})
    rows_mc = table_mc.findAll('tr')[1:31]

    for row in rows:
        fields = row.findAll('td')
        if fields:
            f1 = fields[1].text.strip() # names with symbol
            name,symbol = re.findall(r'([a-zA-Z0-9.\s&-]+) \([a-zA-Z0-9]+:([a-zA-Z0-9]+)\)',f1)[0]
            symbol += '.SI'
            f2 = fields[2].text.strip() # curreny symbol with share price
            ccy,price = re.findall(r'(SGD|USD) ([0-9.]+)',f2)[0]
            sector = fields[10].text.strip() # sector
            records.append([symbol,name,sector,'SGX',ccy,price])
    
    for row_mc in rows_mc:
        fields_mc = row_mc.findAll('td')
        
        if fields_mc:
            f1_mc = fields_mc[1].text.strip()
            symbol_mc = re.findall(r'[a-zA-Z0-9.\s-]+\([a-zA-Z0-9]+:([a-zA-Z0-9]+)\)',f1_mc)[0]
            symbol_mc += '.SI'
            f3_mc = fields_mc[3].text.strip()
            marketCap = float(re.findall(r'([0-9.]+)B',f3_mc)[0])
            records_mc.append([symbol_mc,marketCap])

    df = pd.DataFrame(records,columns=['Code','Name','Sector','Exchange','CCY','CurrentPrice'])
    df_mc = pd.DataFrame(records_mc,columns=['Code','MarketCap(Billions)'])
    df_sti = pd.merge(df,df_mc,how='inner',on='Code')

    print('\n STI done')
    return df_sti
 

def extract_hsci():
    source = 'https://www.hsi.com.hk/eng/indexes/all-indexes/industry'
    driver = webdriver.Chrome()
    driver.get(source)
    time.sleep(7)
    d = driver.page_source
    driver.quit()

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
    df_info = pdr.get_quote_yahoo(df_hsci['Code'])
    
    df_hsci['Exchange'] = df_info['exchange'].values
    df_hsci['CCY'] = df_info['currency'].values
    df_hsci['CurrentPrice'] = df_info['price'].values
    df_hsci['MarketCap(Billions)'] = df_info['marketCap'].fillna(0).values/1000000000

    print('\n HSCI Done')
    return df_hsci

def extract_spy():
    source = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    d = requests.get(source)

    soup = BeautifulSoup(d.content,"html.parser")
    table = soup.find("table",{"class":"wikitable sortable"})

    header = table.findAll('th')
    if header[0].text.rstrip() != "Symbol" or header[1].string != "Security":
        raise Exception("Can't parse Wikipedia's table!")

    # Retrieve the values in the table
    records = []
    symbols = []
    rows = table.findAll("tr")
    for row in rows:
        fields = row.findAll("td")
        if fields:
            symbol = fields[0].text.rstrip()
            symbol = symbol.replace(".","-")
            # fix as now they have links to the companies on WP
            name = fields[1].text.replace(",", "")
            sector = fields[3].text.rstrip()
            records.append([symbol, name, sector])
            symbols.append(symbol + "\n")
    header = ["Code", "Name", "Sector"]
    df_spy = pd.DataFrame(records,columns=header)
    df_info = pdr.get_quote_yahoo(df_spy['Code'])

    df_spy['Exchange'] = df_info['exchange'].values
    df_spy['CCY'] = df_info['currency'].values
    df_spy['CurrentPrice'] = df_info['price'].values
    df_spy['MarketCap(Billions)'] = df_info['marketCap'].fillna(0).values/1000000000

    print('\n Spy Done')
    return df_spy

if __name__=='__main__':
    save_path = './data/US_HK_SG_companies_basic_info.csv'

    df_sti = extract_sti()
    df_hsci = extract_hsci()
    df_spy = extract_spy()

    df = pd.concat([df_spy,df_hsci,df_sti],axis=0)
    df['TargetPrice'] = None
    df['As_of_Date'] = pd.to_datetime('2022-06-03')

    df.to_csv(save_path,index=False)
