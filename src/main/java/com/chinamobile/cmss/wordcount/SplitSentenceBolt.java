package com.chinamobile.cmss.wordcount;

import java.util.Map;
import java.util.StringTokenizer;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {

	private static final long serialVersionUID = -4530399241137293745L;

	private OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("message");
		StringTokenizer stringTokenizer = new StringTokenizer(sentence);
		while(stringTokenizer.hasMoreTokens()) {
			String word = stringTokenizer.nextToken();
			
			// anchoring(锚定传入的tuple)
			this.collector.emit(tuple, new Values(word));
		}
		
		this.collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}