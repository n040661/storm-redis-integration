package com.chinamobile.cmss.wordcount;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt{

	private static final long serialVersionUID = 1662610089285119652L;
	
	private Map<String,Long> counts;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counts = new HashMap<String,Long>();		
	}
	
	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = input.getLongByField("count");
		this.counts.put(word, count);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//
	}
	
	// 引用Javadoc：The cleanup method is called when a Bolt is being shutdown and should cleanup any resources that were opened.
	// There's no guarantee that this method will be called on the cluster: for example, 
	@Override
	public void cleanup() {
		System.out.println("---***--- FINAL COUNTS START ---***---");
		counts.forEach((word, count) -> System.out.println(word + " appears " + count + " times···"));
		System.out.println("---***--- FINAL COUNTS END ---***---");
	}
}
