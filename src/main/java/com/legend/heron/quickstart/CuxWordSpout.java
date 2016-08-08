package com.legend.heron.quickstart;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by allen on 8/5/16.
 *
 * Note: BaseRichSpout only do some default implementation for IRichSpout
 */
public class CuxWordSpout extends BaseRichSpout {

    private static final long serialVersionUID = 6645846407969919829L;
    private SpoutOutputCollector spoutOutputCollector;
    private String[] words;
    private Random random;

    public void open(Map<String, Object> conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        words = new String[] {"mike", "allen", "jackson", "hello", "well"};
        random = new Random();
    }

    public void nextTuple() {
        final String word = this.words[random.nextInt(this.words.length)];
        this.spoutOutputCollector.equals(new Values(word));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
