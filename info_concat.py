import pandas as pd

def execute():
    save_path = './data/US_HK_SG_companies_basic_info.csv'
    path_spy = './data/spy_500_companies_info.csv'
    path_hsci = './data/hsci_companies_info.csv'
    path_sti = './data/sti_companies_info.csv'
    df_spy = pd.read_csv(path_spy)
    df_hsci = pd.read_csv(path_hsci)
    df_sti = pd.read_csv(path_sti)

    df = pd.concat([df_spy,df_hsci,df_sti],axis=0)
    df['TargetPrice'] = None
    df['As_of_Date'] = pd.to_datetime('2022-06-03')
    print('\n',df)

    df.to_csv(save_path,index=False)

if __name__=="__main__":
    execute()
