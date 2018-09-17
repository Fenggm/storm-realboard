##实时看板案例

###项目架构模型
支付系统+kafka+storm集群+redis集群<br>
1、支付系统发送mq到kafka集群中，编写storm程序消费kafka的数据并计算实时的订单数量、订单数量<br>
2、将计算的实时结果保存在redis中<br>
3、外部程序访问redis的数据实时展示结果<br>

这里采用PayMentInfoProducer模拟数据生产,调用random()方法,产生随机数据进行发送,并未连接真正的数据.<br>
通过JedisUtil工具类,获取jedis连接池以及jedis的连接.<br>
通过RealBoardBolt,将获取的kafkaSpout发送过来的数据,进行格式转换后,发送到redis.