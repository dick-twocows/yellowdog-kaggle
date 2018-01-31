package org.twocows.yellowdog.kaggle;

import org.apache.kafka.common.serialization.LongSerializer;

import static org.junit.Assert.*;

import org.junit.Test;
import org.twocows.yellowdog.kafka.KafkaFactory;
import org.twocows.yellowdog.kafka.SimpleKafkaProducer;

public class TestKaggleLineProducer {
	
	/*
	 * This is the underlying data but unable to access due to permissions. Otherwise we could specify a https:// URL.
	 * "https://storage.googleapis.com/kaggle-datasets/4614/7040/"; 
	 */
	public static final String KAGGLE_URL = "file:////home/dick/CV/Yellow Dog/commodity_trade_statistics_data.csv"; 
	/*
	 * This is uncompressed at 1.15GB so best to use GZIP.
	 * However could not get Java GZIP to read the file.!?
	 * "https://www.kaggle.com/unitednations/global-commodity-trade-statistics/downloads/commodity_trade_statistics_data.csv";
	 */
	
	@Test
	public void test() {
		System.out.println("KaggleLineProducer");
		final KaggleLineProducer kaggleLineProducer = new KaggleLineProducer();
		kaggleLineProducer
			.setProperties(
				KaggleLineProducer.append(
					SimpleKafkaProducer.createProperties(KafkaFactory.BOOTSTRAP_SERVERS_LOCAL_HOST_9092, TestKaggleLineProducer.class.getSimpleName(), LongSerializer.class, KaggleLineSerializer.class),
					KAGGLE_URL, 
					false
				)
			);
		kaggleLineProducer.getProperties().setProperty(KaggleLineProducer.MAX_COUNT, "10");
		System.out.println(kaggleLineProducer.produce());
		System.out.println(kaggleLineProducer);
	}
}
