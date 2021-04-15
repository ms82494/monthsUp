import pandas as pd
import pickle
from logconfig import *

# Alphavantage.co allows access to the following API without entering a valid API Key
VALIDTICKERURL = 'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=demo'
# SEC-maintained cross-reference table for stock tickers and CIK numbers
CIKURL = 'https://www.sec.gov/include/ticker.txt'

TICKERFILE = './stocks.csv'

logger = logging.getLogger("tickerSource")

# load the tables into dataframes
tickers = pd.read_csv(VALIDTICKERURL)
logger.info(f"Number of tickers from Alphavantage: {len(tickers)}")

ciks = pd.read_csv(CIKURL,
    sep='\t',
    header=None,
    names=['symbol', 'CIK'],
    converters={'symbol': lambda x: x.upper()},
    index_col='symbol',
    dtype={'CIK': str})
logger.info(f"Number of CIKs from SEC: {len(ciks)}")

# fix some missing CIKs
ciks.loc['GTN'] = ciks.loc['GTN-A', 'CIK']
ciks.loc['POW'] = ['1829427']
ciks.loc['NLSP'] = ['1783036']
ciks.loc['HOL'] = ['1814329']
ciks.loc['WSG'] = ['1771279']
ciks.loc['SVVC'] = ['1495584']
ciks.loc['LEXX'] = ciks.loc['LXX', 'CIK']
ciks.loc['BF-B'] = ciks.loc['BFA', 'CIK']
ciks.loc['GRP-U'] = ciks.loc['GRP-UN', 'CIK']

# merge the SEC CIK numbers to the Alphavantage tickers
df = pd.merge(tickers,
    ciks,
    on='symbol',
    how='left')
logger.info(f"Number of tickers with CIKs: {len(df)}")

# drop all ETFs
dropETFs = df[df['assetType'] == 'ETF'].index
df.drop(dropETFs, inplace=True)

# drop all stocks for which Alphavantage does not have a name
dropNoName = df[df['name'].isnull()].index
df.drop(dropNoName, inplace=True)

# drop SPACs & Units
dropSPACs1 = df[df['name'].str.contains('Acquisition')].index
dropSPACs2 = df[df['name'].str.contains('SPAC')].index
dropSPACs3 = df[df['name'].str.contains(' Units ')].index

dropSPACs = dropSPACs1.append([dropSPACs2, dropSPACs3])
df.drop(dropSPACs, inplace=True)

# drop mistakes in the AlphaVantage list (duplicates, and such)
mistakeNames = ["CHINA SECURITY & SURVEILLANCE TECHNOLOGY INC.",
    "Cogentix Medical Inc",
    "CNX Coal Resources LP",
    "The Dolan Company",
    "XL Group plc",
    "Elmira Savings Bank",
    "Angel Oak Dynamic Financial Strategies Income Term Trust",
    "PIMCO Dynamic Income Opportunities Fund",
    "First Trust High Yield Opportunities 2027 Term Fund"]
dropMistakes = df[df['name'].isin(mistakeNames)].index
df.drop(dropMistakes, inplace=True)

logger.info(f"Number of tickers with CIKs after dropping unwanted/mistaken ones: {len(df)}")


# drop all tickers for which Mergent Online does not have a Mergent ID 
with open('mergentid_by_ticker.pkl', 'rb') as picklefile:
        company_data = pickle.load(picklefile)

validtickers = company_data.keys()
dropNoMergentID = df[~(df.symbol.isin(validtickers))].index
df.drop(dropNoMergentID, inplace=True)
logger.info(f"Number of tickers with CIKs after dropping ones without Mergent ID: {len(df)}")

# add Mergent ID and SIC code from the pickle file
def getMergentID(i):
    return company_data[i][2]

def getSIC(i):
    return company_data[i][3]

df['MergentID'] = df.symbol.map(getMergentID)
df['SIC'] = df.symbol.map(getSIC)

dropIPOmissing = df[df['ipoDate'].isnull()].index
df.drop(dropIPOmissing, inplace=True)

dropyoung = df[df['ipoDate'] > '2011-02-28'].index
df.drop(dropyoung, inplace=True)

df.drop(['assetType', 'delistingDate', 'status'], axis=1, inplace=True)
logger.info(f"Number of tickers with IPO date 10+ years ago: {len(df)}")

df.to_csv(TICKERFILE, index=False)
