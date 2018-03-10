package com.chinamobile.cmss.wordcount;

import java.util.List;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import com.google.common.collect.Lists;

class WordCountRedisLookupMapper implements RedisLookupMapper {
  
	private static final long serialVersionUID = -1331292090514112548L;
	private RedisDataTypeDescription description;
    private final String hashKey = "fwordcounts";	//additional key

    public WordCountRedisLookupMapper() {
        this.description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    @Override
    public List<Values> toTuple(ITuple input, Object value) {
    	List<Values> values = Lists.newArrayList();
    	
        String word = getKeyFromTuple(input);
        
        String valueFromRedis = (String)value;
        
        if("".equals(valueFromRedis) || null == value) {
        	values.add(new Values(word, 0L));
        }else {
        	long count = Long.valueOf(valueFromRedis);
        	values.add(new Values(word, count));
        }
    
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField("word");
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return String.valueOf(tuple.getLongByField("count"));
    }
}
