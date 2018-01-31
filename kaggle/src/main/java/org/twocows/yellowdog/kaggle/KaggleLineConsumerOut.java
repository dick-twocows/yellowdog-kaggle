package org.twocows.yellowdog.kaggle;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.twocows.yellowdog.kafka.SimpleKafkaConsumer;

/**
 * Override handle method and output Kafka ConsumerRecords to System.out.
 * @author dick
 *
 */
public class KaggleLineConsumerOut extends SimpleKafkaConsumer<Long, KaggleLine> {
	
	public KaggleLineConsumerOut() {
	}
	
	@Override
	public ConsumerRecord<Long, KaggleLine> handle(final ConsumerRecord<Long, KaggleLine> consumerRecord) {
		System.out.println(consumerRecord);
		return consumerRecord;
	}
}
