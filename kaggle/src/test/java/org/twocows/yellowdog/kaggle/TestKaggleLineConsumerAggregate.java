package org.twocows.yellowdog.kaggle;

import static org.junit.Assert.*;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.junit.Test;

public class TestKaggleLineConsumerAggregate {

	@Test
	public void test() {
		final KaggleLineConsumerAggregate kaggleLineConsumerAggregate = new KaggleLineConsumerAggregate();
		kaggleLineConsumerAggregate
			.setProperties(
				SimpleKafkaConsumer.createConsumerProperties(KafkaFactory.BOOTSTRAP_SERVERS_LOCAL_HOST_9092, TestKaggleLineConsumerAggregate.class.getSimpleName(), LongDeserializer.class, KaggleLineDeserializer.class));
		kaggleLineConsumerAggregate.getProperties().setProperty(SimpleKafkaConsumer.SUBSCRIBE_TOPICS, KaggleLineProducer.TOPIC_DEFAULT);
		System.out.println(kaggleLineConsumerAggregate.consume());
		System.out.println(kaggleLineConsumerAggregate.getAggregates());
	}

}
