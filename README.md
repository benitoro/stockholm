Stockholm
=======

#### 一个股票数据（沪深）爬虫和选股策略测试框架，数据基于雅虎YQL和新浪财经。
* 根据选定的日期范围抓取所有沪深两市股票的行情数据。
* 根据指定的选股策略和指定的日期进行选股测试。
* 计算选股测试实际结果（包括与沪深300指数比较）。
* 保存数据到JSON文件、CSV文件。
* 支持使用表达式定义选股策略。
* 支持多线程处理。

能干什么
-------------
如果你想基于沪深股市行情数据进行一些工作，它可以帮助你导出指定时间范围内所有沪深A股的行情数据和一些技术指标，包括代码、名称、开盘、收盘、最高、最低、成交量、均线、KDJ等。<br>
如果你对于技术分析有兴趣，它可以帮你根据你自定义的策略在所有沪深A股的范围内进行选股，并回测选股策略的收益情况（详细说明后面会有）。这样就能够非常方便快捷地测试和调整基于技术分析的选股策略。<br>

还有些什么问题
-------------
行情数据目前来源于雅虎YQL，每日数据的更新时间不太稳定（一般在中国时间午夜左右）。<br>
目前支持的技术指标还不多，还有一些指标如MACD和BOLL后续会增加。<br>
在回测中，如果有在选定时间内发生过除权的股票，收益计算会有问题。<br>
导出格式目前只支持CSV和JSON文本。MongoDB和MySQL会考虑后续加入。<br>

环境
-------------
Python 3.4以上<br>
[Requests](http://www.python-requests.org/en/latest/)<br>
[PyMongo](http://api.mongodb.org/python/current/installation.html)<br>
OSX和CentOS已测。Windows尚未测试，输出路径可能有问题。<br>

```shell
pip install requests
pip install pymongo
```

使用
-------------
```shell
python main.py [-h] [--reload {Y,N}] [--portfolio {Y,N}] 
               [--output {json,csv,all}] [--storepath PATH] [--thread NUM] 
               [--startdate yyyy-MM-dd] [--enddate yyyy-MM-dd] 
               [--targetdate yyyy-MM-dd] [--testrange NUM] [--testfile PATH]
```

可选参数
-------------
```shell
  -h, --help                  查看帮助并退出
  --reload {Y,N}              是否重新抓取股票数据，默认值：Y
  --portfolio {Y,N}           是否生成选股测试结果，默认值：N
  --output {json,csv,all}     输出文件格式，默认值：json
  --charset {utf-8,gbk}       输出文件编码，默认值：utf-8
  --storepath PATH            输出文件路径，默认值：~/tmp/stockholm_export
  --thread NUM                线程数，默认值：10
  --startdate yyyy-MM-dd      抓取数据的开始日期，默认值：当前系统日期-100天（例如2015-01-01）
  --enddate yyyy-MM-dd        抓取数据的结束日期，默认值：当前系统日期
  --targetdate yyyy-MM-dd     测试选股策略的目标日期，默认值：当前系统日期
  --testrange NUM             测试日期范围天数，默认值：50
  --testfile PATH             测试文件路径，默认值：./portfolio_test.txt
```

可用数据/格式
-------------
### 行情数据:
```shell
[
	{"Symbol": "600000.SS", 
	 "Name": "浦发银行"，
	 "Data": [
				 {"Vol_Change": null, "MA_10": null, "Date": "2015-03-26", "High": 15.58, "Open": 15.15, "Volume": 282340700, "Close": 15.36, "Change": null, "Low": 15.04}, 
				 {"Vol_Change": -0.22726, "MA_10": null, "Date": "2015-03-27", "High": 15.55, "Open": 15.32, "Volume": 218174900, "Close": 15.36, "Change": 0.0, "Low": 15.17}
			 ]
	}
]
```
Date(日期); Open(开盘价); Close(收盘价); High(当日最高); Low(当日最低); Change(价格变化%); Volume(成交量); Vol_Change(成交量较前日变化); MA_5(5日均线); MA_10(10日均线); MA_20(20日均线); MA_30(30日均线); KDJ_K(KDJ指标K); KDJ_D(KDJ指标D); KDJ_J(KDJ指标J); <br>
以上数据都可以用于制定选股策略，后面会介绍具体方法。<br>

### 选股策略测试数据:
```shell
[
	{
		"Symbol": "600000.SS", 
		"Name": "浦发银行", 
		"Close": 14.51, 
		"Change": 0.06456,
		"Vol_Change": 2.39592, 
		"MA_10": 14.171, 
		"KDJ_K": 37.65, 
		"KDJ_D": 33.427, 
		"KDJ_J": 46.096, 
		"Data": [
					{"Day_5_Differ": 0.01869, "Day_9_Profit": 0.08546, "Day_1_Profit": -0.02826, "Day_1_INDEX_Change": -0.00484, "Day_3_INDEX_Change": 0.01557, "Day_5_INDEX_Change": 0.04747, "Day_3_Differ": 0.02647, "Day_9_INDEX_Change": 0.1003, "Day_5_Profit": 0.06616, "Day_3_Profit": 0.04204, "Day_1_Differ": -0.02342, "Day_9_Differ": -0.014840000000000006}
				]
	}
]
```
Close(收盘价); Change(价格变化%); Vol_Change(成交量较前日变化); MA_10(十天均价); KDJ_K(KDJ指标K); KDJ_D(KDJ指标D); KDJ_J(KDJ指标J); Day_1_Profit(后一天利润率%); Day_1_INDEX_Change(后一天沪深300变化率%); Day_1_Differ(后一天相对利润率%——即利润率-沪深300变化率); Day_n_Profit(后n天利润率%); Day_n_INDEX_Change(后n天沪深300变化率%); Day_n_Differ(后n天相对利润率%——即利润率-沪深300变化率);

行情数据抓取范例
-------------
获取从当前日期倒推100天(不是100个交易日)的所有沪深股票行情数据。<br>
执行完成后，数据在当前用户文件夹下./tmp/stockholm_export/stockholm_export.json<br>
```shell
python main.py
```
如果想导出csv文件
```shell
python main.py --output=csv
```

选股策略测试范例
-------------
选股策略范例文件内容如下(包括在源码中)<br>
选股策略"method 1"是:前前个交易日的KDJ指标的J值小于20+前个交易日的KDJ指标J值小于20+当前交易日的KDJ指标J值比上个交易日大40+当前交易日成交量变化大于100%<br />
```shell
## Portfolio selection methodology sample file

[method 1]:day(-2).{KDJ_J}<20 and day(-1).{KDJ_J}<20 and day(0).{KDJ_J}-day(-1).{KDJ_J}>=40 and day(0).{Vol_Change}>=1
```
以当前系统日期为目标日期进行倒推60天得选股策略测试。<br>
不重新抓取行情数据并执行测试命令。<br />
执行完毕后，会将测试结果按照每天一个文件的方式保存在./tmp/stockholm_export/。<br>
文件名格式为result_yyyy-MM-dd.json(例如result_2015-03-24.json)。<br>
```shell
python main.py --reload=N --portfolio=Y
```
通过更改测试文件中的选股策略公式，可以随意测试指定时间范围内的选股效果。<br>
