import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override() # <== that's all it takes :-)

def extract(d,path):
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
    # print('\n',records)
    df_spy = pd.DataFrame(records,columns=header)
    # df_spy.to_csv(path,index=False)
    df_info = pdr.get_quote_yahoo(df_spy['Code'])

    df_spy['Exchange'] = df_info['exchange'].values
    df_spy['CCY'] = df_info['currency'].values
    df_spy['CurrentPrice'] = df_info['price'].values
    df_spy['MarketCap(Billions)'] = df_info['marketCap'].fillna(0).values/1000000000

    df_spy.to_csv(path,index=False)
    
if __name__ == "__main__":
    source = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    csv_file = 'data/spy_500_companies_info.csv'
    c = requests.get(source)
    save_path = os.path.join(os.path.curdir,csv_file)
    extract(c,save_path)


   
