package org.twocows.yellowdog.kaggle;

import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.Test;
import org.twocows.yellowdog.kafka.KafkaFactory;
import org.twocows.yellowdog.kafka.SimpleKafkaProducer;

public class TestKaggleAggregatesProducer {
	
	@Test
	public void producer() {
		final KaggleAggregatesProducer kaggleAggregatesProducer = new KaggleAggregatesProducer();
		kaggleAggregatesProducer
			.setProperties(
				SimpleKafkaProducer
					.createProperties(
						KafkaFactory.BOOTSTRAP_SERVERS_LOCAL_HOST_9092, 
						TestKaggleAggregatesProducer.class.getSimpleName(), 
						LongSerializer.class, 
						KaggleAggregatesSerializer.class
					)
			);
		kaggleAggregatesProducer.setGroup("test");
		final KaggleLineFlowAggregates kaggleLineFlowAggregates = new KaggleLineFlowAggregates();
		final KaggleLineFlowAggregate kaggleLineFlowAggregate = new KaggleLineFlowAggregate();
		kaggleLineFlowAggregate.update("bar", 31);
		kaggleLineFlowAggregates.put("foo", kaggleLineFlowAggregate);
		kaggleAggregatesProducer.setKaggleLineFlowAggregates(kaggleLineFlowAggregates);
		System.out.println(kaggleAggregatesProducer.produce());
		System.out.println(kaggleAggregatesProducer);
	}
}
