package com.fgm.storm.realboard.kafkaToStorm;


import com.alibaba.fastjson.JSON;
import com.fgm.storm.realboard.domain.PaymentInfo;
import com.fgm.storm.realboard.util.JedisUtil;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

public class RealBoardBolt extends BaseBasicBolt {


    /**
     * 这个方法会被反复的调用
     * @param input
     * @param collector
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //得到kafkaSpout发送过来的数据
        String string = input.getString(4);
        //将我们json格式的数据，转换过来
        PaymentInfo paymentInfo = JSON.parseObject(string, PaymentInfo.class);

        //得到商品的价格
        long payPrice = paymentInfo.getPayPrice();

        Jedis jedis = JedisUtil.getConn();
        //累加平台销售的额度
        jedis.incrBy("fgm:order:total:price:date",payPrice);
        //平台今天下单人数
        jedis.incr("fgm:order:total:user:date");
        //平台商品销售数量
        jedis.incr("fgm:order:total:num:date");


        //商品维度统计
        jedis.incrBy("fgm:order:"+paymentInfo.getProductId()+":price:date",payPrice);
        //每个商品的购买人数
        jedis.incr("fgm:order:"+paymentInfo.getProductId()+":user:date");
        //每个商品的销售数量
        jedis.incr("fgm:order:"+paymentInfo.getProductId()+":num:date");


        //店铺维度统计指标
        //统计店铺销售额度
        jedis.incrBy("fgm:order:"+paymentInfo.getShopId()+":price:date",payPrice);
        // 每个店铺的购买人数
        jedis.incr("fgm:order:"+paymentInfo.getShopId()+":user:date");
        //店铺销售的数量
        jedis.incr("fgm:order:"+paymentInfo.getShopId()+":num:date");
        jedis.close();


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
