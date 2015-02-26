import requests
import timeit
import json

class Static:
    all_quotes_url = 'http://money.finance.sina.com.cn/d/api/openapi_proxy.php'

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
        print("Error: Failed to load stock info...")
    
    print("load_all_quotes end... time cost: " + str(round(timeit.default_timer() - start)) + 's')
    return all_quotes

if __name__ == '__main__':
    all_quotes = load_all_quotes()
    print(all_quotes[0])
    print(len(all_quotes))
