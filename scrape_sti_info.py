import requests
import os
import re
from bs4 import BeautifulSoup
import pandas as pd

def extract(d,d_mc,path):
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
    df_final = pd.merge(df,df_mc,how='inner',on='Code')
    df_final.to_csv(path,index=False)
    # print('\n',df_final)
    

if __name__=="__main__":
    source = 'https://sginvestors.io/market/sgx-share-price-performance/straits-times-index-constituents'
    source_mc = 'https://sginvestors.io/analysts/sti-straits-times-index-constituents-target-price'
    csv_file = 'data/sti_companies_info.csv'
    c = requests.get(source)
    c_mc = requests.get(source_mc)
    save_path = os.path.join(os.path.curdir,csv_file)
    extract(c,c_mc,save_path)

