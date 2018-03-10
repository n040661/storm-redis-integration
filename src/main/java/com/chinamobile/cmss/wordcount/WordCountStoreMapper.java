package com.chinamobile.cmss.wordcount;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

public class WordCountStoreMapper implements RedisStoreMapper {

	private static final long serialVersionUID = -5386846703024414422L;
	
	 private RedisDataTypeDescription description;
	 
	 private String hashKey = "twordcounts";
	
	public WordCountStoreMapper() {
        this.description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

	@Override
	public String getKeyFromTuple(ITuple tuple) {
		return tuple.getStringByField("word");
	}

	@Override
	public String getValueFromTuple(ITuple tuple) {
		return String.valueOf(tuple.getLongByField("count"));
	}

	@Override
	public RedisDataTypeDescription getDataTypeDescription() {
		return this.description;
	}
}