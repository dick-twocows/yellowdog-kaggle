package org.twocows.yellowdog.kaggle;

import java.util.Map;

public class KaggleLineSerializer extends KafkaJsonSerializer<KaggleLine> {

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] serialize(String topic, KaggleLine data) {
		return super.serialize(topic, data);
	}

}
