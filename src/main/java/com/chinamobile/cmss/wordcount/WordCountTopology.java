package com.chinamobile.cmss.wordcount;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.redis.bolt.RedisLookupBolt;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class WordCountTopology {

	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME1 = "from-wordcount-topology";
	private static final String TOPOLOGY_NAME2 = "to-wordcount-topology";
	
	private static final String LOOKUP_BOLT_ID = "lookup-bolt";
	private static final String STORE_BOLT_ID = "store-bolt";
	
	private static final String REDIS_HOST = "10.254.10.56";
	private static final int REDIS_PORT = 6379;
	private static final String REDIS_PASSWORD = "ShLsEs3j";

	public static void main(String[] args) throws Exception {
		//fromRedisTopology(args);
		
		toRedisTopology(args);
	}

	/*
	 * RedisLooupBolt的源码可以看出，该Bolt可以发送Tuple，而RedisStoreBolt不可以发送Tuple
	 * 之前想法很愚蠢，认为RedisLookupBolt是单纯的从Redis读取数据并作为数据源，这不扯淡吗？这可是一个Bolt哎
	 * RedisLookupBolt的工作流程大致如下::
	 * 		① 获取上一级Bolt发送来的Tuple元组数据
	 * 		② 取出该Tuple元组的key，在Redis中查找是否存在该key
	 * 		③ 若存在，封装Tuple并发送给ReportBolt，即collector.emit(new Values(word,count))
	 * 		④ 若不存在，封装Tuple并发送给ReportBolt，即collector.emit(new Values(word,0L)
	 * 
	 * ---***--- FINAL COUNTS START ---***---
	 * away appears 0 times···
	 * ago appears 0 times···
     * jumped appears 0 times···
     * seven appears 0 times···
     * cow appears 0 times···
     * two appears 56 times···
     * dwarfs appears 0 times···
     * years appears 13 times···
     * score appears 0 times···
     * apple appears 0 times···
     * white appears 0 times···
     * and appears 0 times···
     * four appears 0 times···
     * day appears 1 times···
     * keeps appears 63 times···
     * over appears 0 times···
     * a appears 0 times···
     * nature appears 0 times···
     * i appears 3 times···
     * am appears 0 times···
     * an appears 0 times···
     * the appears 0 times···
     * doctor appears 0 times···
     * with appears 0 times···
     * moon appears 0 times···
     * at appears 3 times···
     * snow appears 0 times···
     * ---***--- FINAL COUNTS END ---***---
     * 
     * 我感觉RedisLookupBolt特别适合基于Redis做过滤逻辑···
	 * 
	 */
	public static void fromRedisTopology(String... args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, NotAliveException {
		TopologyBuilder builder = new TopologyBuilder();
		
		JedisPoolConfig poolConfig = new JedisPoolConfig
				.Builder()
		        .setHost(REDIS_HOST)
		        .setPort(REDIS_PORT)
		        .setPassword(REDIS_PASSWORD)
		        .build();
		
		RedisLookupMapper lookupMapper = new WordCountRedisLookupMapper();
		RedisLookupBolt lookupBolt = new RedisLookupBolt(poolConfig, lookupMapper);
		
		builder.setSpout(SENTENCE_SPOUT_ID, new RandomSentenceSpout(), 1);
		builder.setBolt(SPLIT_BOLT_ID, new SplitSentenceBolt(), 1).shuffleGrouping(SENTENCE_SPOUT_ID);
		builder.setBolt(COUNT_BOLT_ID, new WordCountBolt(), 1).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
		builder.setBolt(LOOKUP_BOLT_ID, lookupBolt, 1).globalGrouping(COUNT_BOLT_ID);
		builder.setBolt(REPORT_BOLT_ID, new ReportBolt(), 1).globalGrouping(LOOKUP_BOLT_ID);
		
		Config conf = new Config();
		conf.setNumAckers(3);
		conf.setMessageTimeoutSecs(100);
		conf.setDebug(true);
		
		runningMode(args, TOPOLOGY_NAME1, conf, builder);
	}
	
	/*
	 * 继承RedisStoreBolt类实现Tuple的再分发，但是好像没法再分发，只能单纯地写入Redis
	 */
	public static void toRedisTopology(String... args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, NotAliveException {
		TopologyBuilder builder = new TopologyBuilder();
		
		JedisPoolConfig poolConfig = new JedisPoolConfig
				.Builder()
		        .setHost(REDIS_HOST)
		        .setPort(REDIS_PORT)
		        .setPassword(REDIS_PASSWORD)
		        .build();

		RedisStoreMapper storeMapper = new WordCountStoreMapper();
		RedisStoreBolt wordcountStoreBolt = new RedisStoreBolt(poolConfig, storeMapper);
		
		builder.setSpout(SENTENCE_SPOUT_ID, new RandomSentenceSpout(), 1);
		builder.setBolt(SPLIT_BOLT_ID, new SplitSentenceBolt(), 3).shuffleGrouping(SENTENCE_SPOUT_ID);
		builder.setBolt(COUNT_BOLT_ID, new WordCountBolt(), 9).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
		builder.setBolt(STORE_BOLT_ID, wordcountStoreBolt, 9).shuffleGrouping(COUNT_BOLT_ID);
		
		Config conf = new Config();
		conf.setNumAckers(3);
		conf.setMessageTimeoutSecs(100);
		conf.setDebug(true);
		
		runningMode(args, TOPOLOGY_NAME2, conf, builder);
	}

	public static void runningMode(String[] args, String topologyName, Config conf, TopologyBuilder topologyBuilder)
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, NotAliveException {
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());
		}

		else {
			conf.setMaxTaskParallelism(3);

			ILocalCluster cluster = new LocalCluster();

			cluster.submitTopology(topologyName, conf, topologyBuilder.createTopology());

			Utils.sleep(10000);
			
			cluster.killTopology(topologyName);

			cluster.shutdown();
		}
	}
}