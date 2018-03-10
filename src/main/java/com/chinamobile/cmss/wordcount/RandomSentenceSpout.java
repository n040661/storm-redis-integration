package com.chinamobile.cmss.wordcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RandomSentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = -6946545133539398885L;
	
	private SpoutOutputCollector collector;

	private Random random;
	
	private ConcurrentHashMap<UUID, Values> pending;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.random = new Random();
		this.pending = new ConcurrentHashMap<UUID, Values>();
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		
		String[] sentences = new String[] { "the cow jumped over the moon", 
											"an apple a day keeps the doctor away",
											"four score and seven years ago", 
											"snow white and the seven dwarfs", 
											"i am at two with nature"
										  };
		String sentence = sentences[random.nextInt(sentences.length)];
		
		Values values = new Values("saying", sentence);
		
		UUID uuId = UUID.randomUUID();
		this.pending.put(uuId, values);
		
		collector.emit(values, uuId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key","message"));
	}
	
	@Override
	public void ack(Object msgId) {
		this.pending.remove(msgId);
	}

	@Override
	public void fail(Object msgId) {
		this.collector.emit(this.pending.get(msgId), msgId);
	}
}