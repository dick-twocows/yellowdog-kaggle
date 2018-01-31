package org.twocows.yellowdog.kafka;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper; 
 
/**
 * Serialize objects to UTF-8 JSON. This works with any object which is serializable with Jackson. 
 */ 
public class KafkaJsonSerializer<T> implements Serializer<T> { 
 
	private ObjectMapper objectMapper = new ObjectMapper(); 
 
	public KafkaJsonSerializer() { 
	} 
 
	@Override 
	public void configure(Map<String, ?> config, boolean isKey) { 
	} 
 
	@Override 
	public byte[] serialize(String topic, T data) { 
		try { 
			return objectMapper.writeValueAsBytes(data); 
		} catch (Exception e) { 
			throw new SerializationException("Error serializing JSON message", e);
		} 
	} 
 
	@Override 
	public void close() {
	} 
 
}