Stockholm
=======

#### 一个股票数据（沪深）爬虫和选股策略测试框架，数据基于雅虎YQL和新浪财经。
* 根据选定的日期范围抓取所有沪深两市股票的基本数据。
* 根据指定的选股策略和指定的日期进行选股测试。
* 计算选股测试实际结果（包括与沪深300指数比较）。
* 保存数据到JSON文件、CSV文件或MongoDB（未完成）。
* 支持使用表达式定义选股策略（未完成）。
* 支持多线程处理。

环境
-------------
Python 3.4以上<br \>
[Requests](http://www.python-requests.org/en/latest/)<br \>
```shell
pip install requests
```

使用
-------------
```shell
python main.py [-h] [--reload {Y,N}] [--portfolio {Y,N}] 
               [--output {json,csv}] [--storepath PATH] [--thread NUM] 
               [--startdate YYYY-mm-DD] [--enddate YYYY-mm-DD] 
               [--targetdate YYYY-mm-DD] [--testrange NUM]
```

可选参数
-------------
```shell
  -h, --help                  查看帮助并退出
  --reload {Y,N}              是否重新抓取股票数据，默认值：Y
  --portfolio {Y,N}           是否生成选股测试结果，默认值：N
  --output {json,csv}         输出文件格式，默认值：json
  --storepath PATH            输出文件路径，默认值：~/tmp/stockholm_export
  --thread NUM                线程数，默认值：10
  --startdate YYYY-mm-DD      抓取数据的开始日期，默认值：当前系统日期-100天（例如2015-01-01）
  --enddate YYYY-mm-DD        抓取数据的结束日期，默认值：当前系统日期
  --targetdate YYYY-mm-DD     测试选股策略的目标日期，默认值：当前系统日期
  --testrange NUM             测试日期范围天数，默认值：50
```
