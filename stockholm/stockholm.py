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

class Static():
    all_quotes_url = 'http://money.finance.sina.com.cn/d/api/openapi_proxy.php'
    yql_url = 'http://query.yahooapis.com/v1/public/yql'
    export_folder = './export'

    def get_columns(self, quote):
        columns = []
        if(quote is not None):
            for key in quote.keys():
                if(key == 'Data'):
                    for data_key in quote['Data'][-1]:
                        columns.append("data." + data_key)
                else:
                    columns.append(key)
            columns.sort()
        return columns

class KDJ():
    def _avg(self, a):
        length = len(a)
        return sum(a)/length
    
    def _getMA(self, values, window):
        array = []
        x = window
        while x <= len(values):
            curmb = 50
            if(x-window == 0):
                curmb = self._avg(values[x-window:x])
            else:
                curmb = (array[-1]*2+values[x-1])/3
            array.append(round(curmb,3))
            x += 1
        return array
    
    def _getRSV(self, arrays):
        rsv = []
        x = 9
        while x <= len(arrays):
            high = max(map(lambda x: x['High'], arrays[x-9:x]))
            low = min(map(lambda x: x['Low'], arrays[x-9:x]))
            close = arrays[x-1]['Close']
            rsv.append((close-low)/(high-low)*100)
            t = arrays[x-1]['Date']
            x += 1
        return rsv
    
    def getKDJ(self, quote_data):
        if(len(quote_data) > 12):
            rsv = self._getRSV(quote_data)
            k = self._getMA(rsv,3)
            d = self._getMA(k,3)
            j = list(map(lambda x: round(3*x[0]-2*x[1],3), zip(k[2:], d)))
            
            for idx, data in enumerate(quote_data[0:12]):
                data['KDJ_K'] = None
                data['KDJ_D'] = None
                data['KDJ_J'] = None
            for idx, data in enumerate(quote_data[12:]):
                data['KDJ_K'] = k[2:][idx]
                data['KDJ_D'] = d[idx]
                if(j[idx] > 100):
                    data['KDJ_J'] = 100
                elif(j[idx] < 0):
                    data['KDJ_J'] = 0
                else:
                    data['KDJ_J'] = j[idx]
            
        return quote_data

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
            quote_data.reverse()
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

def data_process(all_quotes):
    print("data_process start..." + "\n")
    kdj = KDJ()
    start = timeit.default_timer()
    
    for quote in all_quotes:
        if('Data' in quote):
            try:
                temp_data = []
                for quote_data in quote['Data']:
                    if(quote_data['Volume'] != '000'):
                        d = {}
                        d['Open'] = float(quote_data['Open'])
                        d['Adj_Close'] = float(quote_data['Adj_Close'])
                        d['Close'] = float(quote_data['Close'])
                        d['High'] = float(quote_data['High'])
                        d['Low'] = float(quote_data['Low'])
                        d['Volume'] = int(quote_data['Volume'])
                        d['Date'] = quote_data['Date']
                        temp_data.append(d)
                quote['Data'] = temp_data
            except KeyError as e:
                print(e + "\n")
                print(quote)

    ## calculate Change
    for quote in all_quotes:
        if('Data' in quote):
            for i, quote_data in enumerate(quote['Data']):
                if(i > 0):
                    quote_data['Change'] = round((quote_data['Close']-quote['Data'][i-1]['Close'])/quote['Data'][i-1]['Close'], 3)

    ## calculate KDJ
    for quote in all_quotes:
        if('Data' in quote):
            kdj.getKDJ(quote['Data'])

    print("data_process end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")

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
                                if(column[5:] in quote_data):
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
    load_all_quote_data(all_quotes, "2015-01-01", "2015-03-01")
    data_process(all_quotes)
    data_export(all_quotes, 'csv')
