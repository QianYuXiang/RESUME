#计算骑手时间成本

####项目背景:
支持配送策略机器学习相关算法，为<font color="red">配送人员区域调配</font>，外卖配送成本<font color="red">动态定价策略</font>做数据基础设施建设。

####项目现状:
数据源 骑手配送数据

对于骑手A，取到POI1的餐后，又去取POI2的餐。

| 订单ID        | 骑手ID           | POI ID|取餐时间|完成时间|
| ------------- |:-------------:|:-------------:|:-------------:| -----:|
| Order1 | A | POI1 | take_time_A1 | finish_time_A1|
| Order2 | A | POI2 | take_time_A2 | finish_time_A2|

则对于Order1订单来说，假设骑手A是取到Order1订单后取到Order2订单，并且Order1优先送达后Order2送达，他的Order1订单的配送时间成本是
(take_time_A2-take_time_A1) + (finish_time_A1 - take_time_A2)/2，Order2订单的配送时间成本是(finish_time_A1 - take_time_A2)/2 + (finish_time_A2 - finish_time_A1)。

####实现思路:
对于每个骑手，收集当天的所有配送时间点，按时间点划分成每个时间段，连接出每个时间段内有哪些运单，计算每个运单的各时间段时间成本的占比，然后聚合出每个运单的时间成本，最后再连接处其他想要的信息。

####实现代码: 
https://github.com/QianYuXiang/RESUME/blob/master/projects/wmridertimecost.scala

#计算区域天气状况

####项目需求:
抓取平台会定期每天抓取各个城市天气状况。比如城市哈尔滨在20160120当天抓取了8次天气数据。这些数据要处理成对于每个10分钟时间段该城市天气的天气状况。则对于每个10分钟，都取距离当前时间段起点最近的那个时间点的天气状况。

####项目代码: 
https://github.com/QianYuXiang/RESUME/blob/master/projects/wmpeisongpaweather.scala