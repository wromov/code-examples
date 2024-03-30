import json
import requests
import pandas as pd
import numpy as np

import psycopg2
import psycopg2.extras
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData

import time
import datetime

# Establish a connection to the database by creating a cursor object

conn = psycopg2.connect(
            host = "######",
            database="######",
            user = "######",
            password = "######")

# Create new_cursor allowing us to write Python to execute PSQL:
cur = conn.cursor()

conn.autocommit = True  # (TRUE) Automatically commits entries to psql db

today = datetime.date.today()

# create a dataframe variable "tickers" of equity ticker symbols from csv containing those symbols

tickers = pd.read_csv('544_tickers.csv')

# assign columns of TDA's option chain json response to variable "c"

c = ['putCall','symbol','description','exchangeName','bid','ask','last','mark','bidSize','askSize','bidAskSize','lastSize',
     'highPrice','lowPrice','openPrice','closePrice','totalVolume','tradeDate','tradeTimeInLong','quoteTimeInLong','netChange',
     'volatility','delta','gamma','theta','vega','rho','openInterest','timeValue','theoreticalOptionValue',
     'theoreticalVolatility','optionDeliverablesList','strikePrice','expirationDate','daysToExpiration','expirationType',
     'lastTradingDay','multiplier','settlementType','deliverableNote','isIndexOption','percentChange','markChange',
     'markPercentChange','inTheMoney','mini', 'nonStandard','Today']

# the chain_request function makes the request to TDA's API and returns json response to variable "data"

def chain_request(symbol):
    response = requests.get(f"https://api.tdameritrade.com/v1/marketdata/chains?apikey=######&symbol={symbol}&strikeCount=100&includeQuotes=TRUE&optionType=S", verify=False)
    data = response.json()
    return data

start = time.time()
# Iterate over each symbol in the tickers dataframe and requests the associated options chain data from TDA's API
for i in range(0, len(tickers.index)):
	symbol = tickers['Symbol'].iloc[i]
	print(symbol)
	data = chain_request(symbol)
	# Seperate the calls and puts into seperate objects
	calls = data['callExpDateMap']
	puts = data['putExpDateMap']

	# Create a list of all call expirations
	callindex  = []
	for x in calls:
	    callindex.append(x)

	# Create a list of all put expirations
	putindex = []
	for x in puts:
		putindex.append(x)

	# Iterate over each expiration date in the callindex and define the "tablename" variable based on the current expiration being iterated over (expiration dates are the psql table names)
	for i in range(0,len(callindex)):
	    expiration = callindex[i]
	    tablename = expiration[:10]
	    
		# Create a list of all strikes for the current expiration being iterated over and creates dataframe object "arr" with the list of strikes as its index
	    strikes = []
	    for x in calls[callindex[i]]:
	        strikes.append(x)

	    arr = pd.DataFrame(data=np.array(strikes))

		# Create a new dataframe with columns defined by variable "c" created earlier and iterate through each strike
	    df=pd.DataFrame(data=None,columns=c)
		# Pull the values corresponding to "c" for the current expiration and each strike being iterated over and append those values to the dataframe
	    for i in range(0, len(arr)):
	        strike = arr[0].iloc[i]
	        values = calls[expiration][f'{strike}']
	        df = df.append(pd.DataFrame(data=values,columns=c,index=[symbol]))
			#8a. Append today's date to the final column 'Today' and move on to the next strike
	        df['Today'] = today

	# 	Once all strikes are iterated over, the dataframe takes "tablename" and either creates a psql table if it doesn't already exist or appends the existing table if it does exist
	# and buckets the dataframe into the psql table that matches the expiration date being iterated over from step 4
			
	    sqlengine = create_engine('postgresql+psycopg2://######:######@######:######/######')
	    dbConnection = sqlengine.connect()
	    frame = df.to_sql(tablename, dbConnection, if_exists='append');
	    dbConnection.close()

	# Iterate over each expiration date in the putindex and define the "tablename" variable based on the current expiration being iterated over (expiration dates are the psql table names)
	for i in range(0,len(putindex)):
	    expiration = putindex[i]
	    tablename = expiration[:10]
	    
		# Create a list of all strikes for the current expiration being iterated over and creates dataframe object "arr" with the list of strikes as its index
	    strikes = []
	    for x in puts[putindex[i]]:
	        strikes.append(x)

	    arr = pd.DataFrame(data=np.array(strikes))

		# Create a new dataframe with columns defined by variable "c" created earlier and iterate through each strike
	    df=pd.DataFrame(data=None,columns=c)
		# Pull the values corresponding to "c" for the current expiration and each strike being iterated over and append those values to the dataframe
	    for i in range(0, len(arr)):
	        strike = arr[0].iloc[i]
	        values = puts[expiration][f'{strike}']
	        df = df.append(pd.DataFrame(data=values,columns=c,index=[symbol]))
			# Append today's date to the final column 'Today' and move on to the next strike
	        df['Today'] = today

		# 	Once all strikes are iterated over, the dataframe takes "tablename" and either creates a psql table if it doesn't already exist or appends the existing table if it does exist
	# and buckets the dataframe into the psql table that matches the expiration date being iterated over

	    sqlengine = create_engine('postgresql+psycopg2://######:######@######:######/######')
	    dbConnection = sqlengine.connect()
	    frame = df.to_sql(tablename, dbConnection, if_exists='append');
	    dbConnection.close()	        
end = time.time()
print("Time to fetch data: ", end-start)