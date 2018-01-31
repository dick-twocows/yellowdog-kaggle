package org.twocows.yellowdog.kaggle;

import java.util.Map;

public class KaggleLineDeserializer extends KafkaJsonDeserializer<KaggleLine> {
	
	public KaggleLineDeserializer() {
	}

	@Override
	protected Class<KaggleLine> getType() {
		return KaggleLine.class;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public KaggleLine deserialize(String topic, byte[] data) {
		return super.deserialize(topic, data);
	}
}
