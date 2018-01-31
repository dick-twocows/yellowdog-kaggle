package org.twocows.yellowdog.kaggle;

import static org.junit.Assert.*;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.junit.Test;
import org.twocows.yellowdog.kafka.KafkaFactory;
import org.twocows.yellowdog.kafka.KafkaJsonDeserializer;
import org.twocows.yellowdog.kafka.SimpleKafkaConsumer;

public class TestKaggleAggregatesConsumer {

	@Test
	public void test() {
		final KaggleAggregatesConsumer kaggleAggregatesConsumer = new KaggleAggregatesConsumer();
		kaggleAggregatesConsumer
			.setProperties(
				SimpleKafkaConsumer
					.createConsumerProperties(
						KafkaFactory.BOOTSTRAP_SERVERS_LOCAL_HOST_9092, 
						TestKaggleAggregatesConsumer.class.getSimpleName(), 
						LongDeserializer.class, 
						KaggleAggregatesDeserializer.class
					)
			);
		kaggleAggregatesConsumer.getProperties().setProperty(SimpleKafkaConsumer.SUBSCRIBE_TOPICS, KaggleAggregatesProducer.TOPIC_DEFAULT);
		System.out.println(kaggleAggregatesConsumer.consume());
		System.out.println(kaggleAggregatesConsumer.getAggregates());
	}

}
