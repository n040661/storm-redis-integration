package com.chinamobile.cmss.wordcount;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription.RedisDataType;
import org.apache.storm.redis.common.mapper.RedisFilterMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;

public class WordCountFilterMapper implements RedisFilterMapper {

	private static final long serialVersionUID = -6896345952772344115L;

	private RedisDataTypeDescription description;
	
	private String setKey = "blacklist";
	
	public WordCountFilterMapper() {
		this.description = new RedisDataTypeDescription(RedisDataType.SET, setKey);
	}
	
	@Override
	public String getKeyFromTuple(ITuple tuple) {
		// TODO Auto-generated method stub
		return tuple.getStringByField("word");
	}

	@Override
	public String getValueFromTuple(ITuple tuple) {
		// TODO Auto-generated method stub
		return tuple.getStringByField("count");
	}

	@Override
	public RedisDataTypeDescription getDataTypeDescription() {
		// TODO Auto-generated method stub
		return this.description;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declare) {
		// TODO Auto-generated method stub
		declare.declare(new Fields("word","count"));
	}
}
