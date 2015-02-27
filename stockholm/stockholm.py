import requests
import timeit
import json

class Static:
    all_quotes_url = 'http://money.finance.sina.com.cn/d/api/openapi_proxy.php'
    yql_url = 'http://query.yahooapis.com/v1/public/yql'

def load_all_quotes():
    print("load_all_quotes start...")
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
            for item in json.loads(r.text)[0]['items']:
                quote = {}
                code = item[0]
                name = item[2]
                ## convert quote code
                if(code.find('sh') > -1):
                    code = code[2:] + '.SS'
                elif(code.find('sz') > -1):
                    code = code[2:] + '.SZ'
                ## convert quote code end
                quote['code'] = code
                quote['name'] = name
                all_quotes.append(quote)
            count += 1
    except:
        print("Error: Failed to load all stock...")
    
    print("load_all_quotes end... time cost: " + str(round(timeit.default_timer() - start)) + 's')
    return all_quotes

def load_quote_info(quote):
    print("load_quote_info start...")
    static = Static()
    start = timeit.default_timer()

    if(quote is not None and quote['code'] is not None):
        yquery = 'select * from yahoo.finance.quotes where symbol = "' + quote['code'].lower() + '"'
        r_params = {'q': yquery, 'format': 'json', 'env': 'http://datatables.org/alltables.env'}
        r = requests.get(static.yql_url, params=r_params)
        ## print(r.url)
        rjson = r.json()
        try:
            quote_info = rjson['query']['results']['quote']
            print(quote_info)
        except(KeyError):
            print("Error: Failed to load stock info... " + quote['code'] + "/" + quote['name'])
        
    print(quote)
    print("load_quote_info end... time cost: " + str(round(timeit.default_timer() - start)) + 's')
    return quote
    

if __name__ == '__main__':
    all_quotes = load_all_quotes()
    print(len(all_quotes))
    load_quote_info(all_quotes[0])
