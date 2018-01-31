package org.twocows.yellowdog.kaggle;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Simple Kafka Consumer which needs;
 * 
 * 		the key and value deserializer classes.
 * 		the value eof. 
 * 
 * 		optional override handle method.
 * 
 * @author dick
 *
 * @param <K> The key.
 * @param <V> The value.
 */
public class SimpleKafkaConsumer<K, V> {

	public static final String SUBSCRIBE_TOPICS = "kafke.subscribe.topics";
	
	/**
	 * Convenience to create simple Kafka consumer properties.
	 * @param bootstrapServers
	 * @param groupID
	 * @param keyDeserializer
	 * @param valueDeserializer
	 * @return
	 */
	public static <K, V> Properties createConsumerProperties(final String bootstrapServers, final String groupID, final Class<K> keyDeserializer, final Class<V> valueDeserializer) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        return properties;
	}
	
	protected Properties properties;

	protected boolean poll = true;
	
	public Properties getProperties() {
		return properties;
	}

	public void setProperties(final Properties properties) {
		this.properties = properties;
	}

	/**
	 * Return a new KafkaConsumer using getProperties().  
	 * @return
	 */
	protected KafkaConsumer<K, V> createConsumer() {
        return new KafkaConsumer<>(properties);
	}

	/**
	 * Handle the given ConsumerRecord.
	 * Default is to return the given ConsumerRecord.
	 * Allows you to modify or return a new ConsumerRecord.
	 * Set pool to false from here to exit consume().
	 * @param consumerRecord
	 */
	public ConsumerRecord<K, V> handle(final ConsumerRecord<K, V> consumerRecord) {
		return consumerRecord;
	}

	/**
	 * Create the consumer and poll, handling values until EOF is reached.
	 * @return A count of the consumer records.
	 */
	public long consume() {
		/*
		 * Simple sanity count.
		 */
		long count = 0;
		try (final KafkaConsumer<K, V> consumer = createConsumer()) {
			/*
			 * This property needs to be set.
			 */
			consumer.subscribe(Arrays.asList(properties.getProperty(SUBSCRIBE_TOPICS).split(",")));
			while (poll) {
				ConsumerRecords<K, V> records = consumer.poll(100);
				for (ConsumerRecord<K, V> consumerRecord : records) {
					count++;
					handle(consumerRecord);
				}
			}
			return count;
    	} catch (final Exception e) {
    		System.err.println(e);
    		return count;
    	} 
	}
}
