package org.twocows.yellowdog.kaggle;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper; 
 
/**
 * Generic JSON deserializer. 
 */ 
public abstract class KafkaJsonDeserializer<T> implements Deserializer<T> { 

	private ObjectMapper objectMapper = new ObjectMapper(); 
 
	public KafkaJsonDeserializer() {
	} 
 
	protected abstract Class<T> getType();
	
	@Override 
	public void configure(Map<String, ?> props, boolean isKey) { 
	} 
 
	@Override 
	public T deserialize(String topic, byte[] data) { 
		try { 
			return objectMapper.readValue(data, getType()); 
		} catch (Exception e) { 
			throw new SerializationException(e); 
		} 
	} 
 
	@Override 
	public void close() { 
	} 
}