package com.legend.heron.quickstart;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.api.GlobalMetrics;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * Created by allen on 8/5/16.
 */
public class CuxExclamationTopology {
    private CuxExclamationTopology() {
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new CuxWordSpout(), 1);
        builder.setBolt("exclaim1", new CuxExclamationBolt(), 1)
                .shuffleGrouping("word");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxSpoutPending(10);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
        conf.setComponentRam("word", 512L * 1024 * 1024);
        conf.setComponentRam("exclaim1", 512L * 1024 * 1024);
        conf.setContainerDiskRequested(1024L * 1024 * 1024);
        conf.setContainerCpuRequested(1);

        if (args != null && args.length > 0) {
            conf.setNumStmgrs(1);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            System.out.println("Toplogy name not provided as an argument, running in simulator mode.");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    public static class CuxExclamationBolt extends BaseRichBolt {
        private static final long serialVersionUID = -8589315935186423725L;
        private long nItems;
        private long startTime;

        public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
            nItems = 0;
            startTime = System.currentTimeMillis();
        }

        public void execute(Tuple tuple) {
            if (++nItems % 1000 == 0) {
                long latency = System.currentTimeMillis() - this.startTime;
                System.out.println(tuple.getString(0) + "!!!");
                System.out.println("Bolt processed " + nItems + " tuples in " + latency + " ms");
                GlobalMetrics.incr("selected_items");
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }
}
