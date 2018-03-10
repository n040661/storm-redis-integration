package com.chinamobile.cmss.wordcount;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;

public class WordCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = -9150804182007856278L;
	
	private static final Logger logger = LogManager.getLogger(RandomSentenceSpout.class);
	
	private OutputCollector collector;

	private Map<String, Long> counts;
	
	private Integer emitFrequency;

	public WordCountBolt() {
		emitFrequency = 10; // Default to 60 seconds
	}

	public WordCountBolt(Integer frequency) {
		emitFrequency = frequency;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.counts = new HashMap<String, Long>();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
		return conf;
	}

	@Override
	public void execute(Tuple tuple) {
		// split and default, __system and __tick
		// tuple.getSourceComponent(): Gets the id of the component that created this tuple
		// tuple.getSourceStreamId(): Gets the id of the stream that this tuple was emitted to
		if (isTickTuple(tuple)) {
			for (String word : counts.keySet()) {
				long count = counts.get(word);
				logger.info("***********" + word + " : " + count + "***********");
			}
		} else {
			String word = tuple.getStringByField("word");
			Long count = counts.get(word);
			if (count == null)
				count = 0L;
			// Increment the count and store it
			count++;
			this.counts.put(word, count);
			
			// anchoring
			this.collector.emit(tuple, new Values(word, count));
		}
		
		this.collector.ack(tuple);
	}

	// Declare that we will emit a tuple containing two fields; word and count
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
	
	protected boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) 
				&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
}
