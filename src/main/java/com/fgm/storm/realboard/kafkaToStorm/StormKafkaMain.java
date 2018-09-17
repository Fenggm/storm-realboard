package com.fgm.storm.realboard.kafkaToStorm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 整合kafka与storm，将数据发送给下游的bolt
 */
public class StormKafkaMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        KafkaSpoutConfig.Builder<String, String> kafkaSpoutConfigBuilder = KafkaSpoutConfig.builder("node01:9092,node02:9092,node03:9092", "fgmOrder");

        //设置kafka的消费组
        kafkaSpoutConfigBuilder.setGroupId("kafkaStormRealBoard");

        //设置kafka的消费策略
        kafkaSpoutConfigBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST);

        KafkaSpoutConfig<String, String> build = kafkaSpoutConfigBuilder.build();

        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<String, String>(build);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafakSpout", kafkaSpout, 3);
        builder.setBolt("realBoardBolt", new RealBoardBolt(), 3).localOrShuffleGrouping("kafakSpout");

        Config config = new Config();
        if (null != args && args.length > 0) {
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("realBoard", config, builder.createTopology());

        }

    }

}
