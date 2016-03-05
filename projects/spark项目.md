#计算骑手时间成本

项目背景: 支持配送策略机器学习相关算法，为<font color="red">配送人员区域调配</font>，外卖配送成本<font color="red">动态定价策略</font>做数据基础设施建设。

数据源 骑手配送数据

对于骑手A，取到POI1的餐后，又去取POI2的餐。

| 订单ID        | 骑手ID           | POI ID|取餐时间|完成时间|
| ------------- |:-------------:|:-------------:|:-------------:| -----:|
| Order1 | A | POI1 | take_time_A1 | finish_time_A1|
| Order2 | A | POI2 | take_time_A2 | finish_time_A2|

则对于Order1订单来说，假设骑手A是取到Order1订单后取到Order2订单，并且Order1优先送达后Order2送达，他的Order1订单的配送时间成本是
(take_time_A2-take_time_A1) + (finish_time_A1 - take_time_A2)/2，Order2订单的配送时间成本是(finish_time_A1 - take_time_A2)/2 + (finish_time_A2 - finish_time_A1)。

`项目代码: https://github.com/QianYuXiang/RESUME/blob/master/projects/wmridertimecost.scala`

#计算区域天气状况

项目背景: 从原先业务库

`项目代码: https://github.com/QianYuXiang/RESUME/blob/master/projects/wmpeisongpaweather.scala`