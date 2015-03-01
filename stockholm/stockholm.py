import requests
import json
import timeit
import time
import io
import os
import csv
from pymongo import MongoClient
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial

class Static:
    all_quotes_url = 'http://money.finance.sina.com.cn/d/api/openapi_proxy.php'
    yql_url = 'http://query.yahooapis.com/v1/public/yql'
    export_folder = './export'

    def get_columns(self, quote):
        columns = []
        if(quote is not None):
            for key in quote.keys():
                if(key == 'Data'):
                    for data_key in quote['Data'][0]:
                        columns.append("data." + data_key)
                else:
                    columns.append(key)
            columns.sort()
        return columns

def load_all_quote_symbol():
    print("load_all_quote_symbol start..." + "\n")
    static = Static()
    start = timeit.default_timer()

    all_quotes = []
    try:
        count = 1
        while (count < 100):
            para_val = '[["hq","hs_a","",0,' + str(count) + ',500]]'
            r_params = {'__s': para_val}
            r = requests.get(static.all_quotes_url, params=r_params)
            if(len(r.json()[0]['items']) == 0):
                break
            for item in r.json()[0]['items']:
                quote = {}
                code = item[0]
                name = item[2]
                ## convert quote code
                if(code.find('sh') > -1):
                    code = code[2:] + '.SS'
                elif(code.find('sz') > -1):
                    code = code[2:] + '.SZ'
                ## convert quote code end
                quote['Symbol'] = code
                quote['Name'] = name
                all_quotes.append(quote)
            count += 1
    except Exception as e:
        print("Error: Failed to load all stock symbol..." + "\n")
        print(e)
    
    print("load_all_quote_symbol end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
    return all_quotes

def load_quote_info(quote, is_retry):
    print("load_quote_info start..." + "\n")
    static = Static()
    start = timeit.default_timer()

    if(quote is not None and quote['Symbol'] is not None):
        yquery = 'select * from yahoo.finance.quotes where symbol = "' + quote['Symbol'].lower() + '"'
        r_params = {'q': yquery, 'format': 'json', 'env': 'http://datatables.org/alltables.env'}
        r = requests.get(static.yql_url, params=r_params)
        ## print(r.url)
        ## print(r.text)
        rjson = r.json()
        try:
            quote_info = rjson['query']['results']['quote']
            quote['LastTradeDate'] = quote_info['LastTradeDate']
            quote['LastTradePrice'] = quote_info['LastTradePriceOnly']
            quote['PreviousClose'] = quote_info['PreviousClose']
            quote['Open'] = quote_info['Open']
            quote['DaysLow'] = quote_info['DaysLow']
            quote['DaysHigh'] = quote_info['DaysHigh']
            quote['Change'] = quote_info['Change']
            quote['ChangeinPercent'] = quote_info['ChangeinPercent']
            quote['Volume'] = quote_info['Volume']
            quote['MarketCap'] = quote_info['MarketCapitalization']
            quote['StockExchange'] = quote_info['StockExchange']
            
        except Exception as e:
            print("Error: Failed to load stock info... " + quote['Symbol'] + "/" + quote['Name'] + "\n")
            print(e + "\n")
            if(not is_retry):
                time.sleep(1)
                load_quote_info(quote, True) ## retry once for network issue
        
    print(quote)
    print("load_quote_info end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
    return quote

def load_all_quote_info(all_quotes):
    print("load_all_quote_info start...")
    static = Static()
    start = timeit.default_timer()
    for idx, quote in enumerate(all_quotes):
        print("#" + str(idx + 1))
        load_quote_info(quote, False)

    print("load_all_quote_info end... time cost: " + str(round(timeit.default_timer() - start)) + "s")
    return all_quotes

def load_quote_data(quote, start_date, end_date, is_retry, counter):
    ## print("load_quote_data start..." + "\n")
    static = Static()
    start = timeit.default_timer()

    if(quote is not None and quote['Symbol'] is not None):        
        yquery = 'select * from yahoo.finance.historicaldata where symbol = "' + quote['Symbol'].upper() + '" and startDate = "' + start_date + '" and endDate = "' + end_date + '"'
        r_params = {'q': yquery, 'format': 'json', 'env': 'http://datatables.org/alltables.env'}
        r = requests.get(static.yql_url, params=r_params)
        ## print(r.url)
        ## print(r.text)
        rjson = r.json()
        try:
            quote_data = rjson['query']['results']['quote']
            quote['Data'] = quote_data
            if(not is_retry):
                counter.append(1)          
            
        except:
            print("Error: Failed to load stock data... " + quote['Symbol'] + "/" + quote['Name'] + "\n")
            if(not is_retry):
                time.sleep(1)
                load_quote_data(quote, start_date, end_date, True, counter) ## retry once for network issue
    
        print("load_quote_data " + quote['Symbol'] + "/" + quote['Name'] + " end..." + "\n")
        ## print("time cost: " + str(round(timeit.default_timer() - start)) + "s." + "\n")
        ## print("total count: " + str(len(counter)) + "\n")
    return quote

def load_all_quote_data(all_quotes, start_date, end_date):
    print("load_all_quote_data start..." + "\n")
    static = Static()
    start = timeit.default_timer()

    counter = []
    mapfunc = partial(load_quote_data, start_date=start_date, end_date=end_date, is_retry=False, counter=counter)
    pool = ThreadPool(9)
    pool.map(mapfunc, all_quotes) ## multi-threads executing
    pool.close() 
    pool.join()

    print("load_all_quote_data end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
    return all_quotes

def data_export(all_quotes, export_type):
    static = Static()
    start = timeit.default_timer()
    directory = static.export_folder
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    if(export_type == 'json'):
        print("start export to JSON file...")
        f = io.open(directory + '/stockholm_export.json', 'w', encoding='utf8')
        json.dump(all_quotes, f, ensure_ascii=False)
        
    elif(export_type == 'csv'):
        print("start export to CSV file...")
        columns = static.get_columns(all_quotes[0])
        writer = csv.writer(open(directory + '/stockholm_export.csv', 'w', encoding='utf8'))
        writer.writerow(columns)

        for quote in all_quotes:
            if('Data' in quote):
                for quote_data in quote['Data']:
                    try:
                        line = []
                        for column in columns:
                            if(column.find('data.') > -1):
                                line.append(quote_data[column[5:]])
                            elif(column == 'Name'):
                                line.append("'" + quote[column] + "'")
                            else:
                                line.append(quote[column])
                        writer.writerow(line)
                    except Exception as e:
                        print(e)
                        print("write csv error: " + quote)
        
    elif(export_type == 'mongo'):
        print("start export to MongoDB...")
        
    print("export is complete... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")

if __name__ == '__main__':
    all_quotes = load_all_quote_symbol()
    print("total " + str(len(all_quotes)) + " quotes are loaded..." + "\n")
    ##load_all_quote_info(all_quotes)
    load_all_quote_data(all_quotes, "2015-02-01", "2015-03-01")
    data_export(all_quotes, 'csv')
