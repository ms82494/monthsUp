import pandas as pd 
import os
import csv
from logconfig import *
import matplotlib.pyplot as plt

logger = logging.getLogger("stats")
STOCKDIR = './stocks'
STATSFILE = './stats.csv'

files = os.listdir(STOCKDIR)
with open(STATSFILE, 'w', newline='') as csv_file:
    writer = csv.writer(csv_file, delimiter=',')
    header = ['ticker', 'positive returns']
    writer.writerow(header)
    for f in files:
        ticker = f.split('.')[0]
        df = pd.read_csv(os.path.join(STOCKDIR, f), index_col = 'date')
        # logger.error(f"can't handle {ticker}")
        droprows = df[df.index.isin(['2011-02-28','2011-03-31','2021-04-30'])].index
        df.drop(droprows, inplace=True)
        df['return'] = df.adjClose.pct_change()
        posmonths = df.loc[df['return']>0,'return'].count()
        writer.writerow([ticker, posmonths])

