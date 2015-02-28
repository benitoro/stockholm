import requests
import timeit
import time
import json

class Static:
    all_quotes_url = 'http://money.finance.sina.com.cn/d/api/openapi_proxy.php'
    yql_url = 'http://query.yahooapis.com/v1/public/yql'

def load_all_quote_symbol():
    print("load_all_quote_symbol start...")
    static = Static()
    start = timeit.default_timer()

    all_quotes = []
    try:
        count = 1
        while (count < 100):
            para_val = '[["hq","hs_a","",0,' + str(count) + ',500]]'
            r_params = {'__s': para_val}
            r = requests.get(static.all_quotes_url, params=r_params)
            if(len(json.loads(r.text)[0]['items']) == 0):
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
    except:
        print("Error: Failed to load all stock symbol...")
    
    print("load_all_quote_symbol end... time cost: " + str(round(timeit.default_timer() - start)) + "s")
    return all_quotes

def load_quote_info(quote, is_retry):
    print("load_quote_info start...")
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
            
        except(KeyError, ValueError, TypeError):
            print("Error: Failed to load stock info... " + quote['Symbol'] + "/" + quote['Name'])
            if(not is_retry):
                time.sleep(1)
                load_quote_info(quote, True) ## retry once for network issue
        
    print(quote)
    print("load_quote_info end... time cost: " + str(round(timeit.default_timer() - start)) + "s")
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

def load_quote_data(quote, start_date, end_date, is_retry):
    print("load_quote_data start...")
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
            
        except(KeyError, ValueError, TypeError):
            print("Error: Failed to load stock data... " + quote['Symbol'] + "/" + quote['Name'])
            if(not is_retry):
                time.sleep(1)
                load_quote_data(quote, start_date, end_date, True) ## retry once for network issue
    
    print("load_quote_data end... time cost: " + str(round(timeit.default_timer() - start)) + "s")
    return quote

def load_all_quote_data(all_quotes, start_date, end_date):
    print("load_all_quote_data start...")
    static = Static()
    start = timeit.default_timer()
    for idx, quote in enumerate(all_quotes):
        print("#" + str(idx + 1))
        load_quote_data(quote, start_date, end_date, False)

    print("load_all_quote_data end... time cost: " + str(round(timeit.default_timer() - start)) + "s")
    return all_quotes

if __name__ == '__main__':
    all_quotes = load_all_quote_symbol()
    print(len(all_quotes))
    ##load_all_quote_info(all_quotes)
    load_all_quote_data(all_quotes, "2015-01-01", "2015-02-01")
