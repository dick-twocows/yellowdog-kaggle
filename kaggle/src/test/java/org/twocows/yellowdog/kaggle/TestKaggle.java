package org.twocows.yellowdog.kaggle;

import static org.junit.Assert.*;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.Test;
import org.twocows.yellowdog.kafka.KafkaFactory;
import org.twocows.yellowdog.kafka.SimpleKafkaConsumer;
import org.twocows.yellowdog.kafka.SimpleKafkaProducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestKaggle {

	public static final String KAGGLE_URL = "file:////home/dick/CV/Yellow Dog/commodity_trade_statistics_data.csv";
	
	public static final String MAX_COUNT = String.valueOf(Long.MAX_VALUE);
	
	public static final boolean JSON_PRETTY = true;

	@Test
	public void test() throws InterruptedException {
		final Thread p = new Thread(
			() -> {
				/*
				 * Produce the Kaggle lines.
				 */
				System.out.println("Producing Kaggle lines");
				final KaggleLineProducer kaggleLineProducer = new KaggleLineProducer();
				kaggleLineProducer
					.setProperties(
						KaggleLineProducer.append(
							SimpleKafkaProducer.createProperties(KafkaFactory.BOOTSTRAP_SERVERS_LOCAL_HOST_9092, TestKaggleLineProducer.class.getSimpleName(), LongSerializer.class, KaggleLineSerializer.class),
							KAGGLE_URL, 
							false
						)
					);
				kaggleLineProducer.getProperties().setProperty(KaggleLineProducer.MAX_COUNT, MAX_COUNT);
				System.out.println(kaggleLineProducer.produce());
				System.out.println(kaggleLineProducer);
				
				/*
				 * Consume the Kaggle aggregates.
				 */
				System.out.println("Consume Kaggle aggregates");
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
				/*
				 * JSON output
				 */
				if (JSON_PRETTY) {
					ObjectMapper mapper = new ObjectMapper();
					try {
						System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(kaggleAggregatesConsumer.getAggregates()));
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {
					System.out.println(kaggleAggregatesConsumer.getAggregates());
				}
			}
		);
		p.start();
		
		final Thread c = new Thread(
			() -> {
				/*
				 * Consume the Kaggle lines.
				 */
				System.out.println("Consume Kaggle lines");
				final KaggleLineAggregateConsumer kaggleLineConsumerAggregate = new KaggleLineAggregateConsumer();
				kaggleLineConsumerAggregate
					.setProperties(
						SimpleKafkaConsumer.createConsumerProperties(KafkaFactory.BOOTSTRAP_SERVERS_LOCAL_HOST_9092, TestKaggleLineConsumerAggregate.class.getSimpleName(), LongDeserializer.class, KaggleLineDeserializer.class));
				kaggleLineConsumerAggregate.getProperties().setProperty(SimpleKafkaConsumer.SUBSCRIBE_TOPICS, KaggleLineProducer.TOPIC_DEFAULT);
				System.out.println(kaggleLineConsumerAggregate.consume());
//				System.out.println(kaggleLineConsumerAggregate.getAggregates());
				
				/*
				 * Produce the Kaggle aggregates.
				 */
				System.out.println("Producing Kaggle aggregates");
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
				kaggleAggregatesProducer.setKaggleLineFlowAggregates(kaggleLineConsumerAggregate.getAggregates());
				System.out.println(kaggleAggregatesProducer.produce());
//				System.out.println(kaggleAggregatesProducer);
			}
		);
		c.start();
		
		p.join();
		c.join();
		
		System.out.println("OK");
	}

}
