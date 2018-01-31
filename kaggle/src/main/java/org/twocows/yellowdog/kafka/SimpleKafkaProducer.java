package org.twocows.yellowdog.kafka;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 
 * @author dick
 *
 */
public abstract class SimpleKafkaProducer<K, V> {

	public static final String MAX_COUNT = "kafka.producer.max.count";

	/*
	 * It's a properties value so declare as a String.
	 */
	public static final String MAX_COUNT_DEFAULT = "-1";
	
	public static Properties createProperties(final String bootstrapServers, final String clientID, final Class<?> keySerializer, final Class<?> valueSerializer) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
        return properties;
	}
	
	protected Properties properties;

	protected Producer<K, V> producer;
	
	protected long count = 0;

	protected long maxCount;
	
    public SimpleKafkaProducer() {
    }

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(final Properties properties) {
		this.properties = properties;
	}

	/**
	 * Create the appropriate KafkaProducer.
	 * @return
	 */
    protected KafkaProducer<K, V> createProducer() {
        return new KafkaProducer<>(getProperties());
    }
	
	protected abstract Iterator<ProducerRecord<K, V>> iterator();

	protected ProducerRecord<K, V> start() {
		return null;
	}
	
	protected ProducerRecord<K, V> end() {
		return null;
	}

	protected RecordMetadata send(final ProducerRecord<K, V> producerRecord) throws InterruptedException, ExecutionException, TimeoutException {
		if (producerRecord == null) {
			return null;
		}
		return producer.send(producerRecord).get(5, TimeUnit.SECONDS);
	}
	
	/**
	 * Send zero or more producer records to a Kafka server.
	 * What is sent depends on the iterator().
	 * @return A count.
	 */
    public long produce() {
    	System.out.println(properties);
		/*
		 * Let the JVM close the Kafka Producer.
		 */
		producer = createProducer();
		maxCount = Long.valueOf(properties.getProperty(MAX_COUNT, "-1"));
    	try {
    		send(start());
    		final Iterator<ProducerRecord<K, V>> iterator = iterator();
			while (iterator.hasNext()) {
				count++;
			    send(iterator.next());
			    if (maxCount > 0 && count == maxCount) {
			    	break;
			    }
			}
	    	send(end());
    	} catch (final Exception e) {
    		System.err.println(e);
    	} finally {
    		producer.close();
    	}
		return count;
    }
}
