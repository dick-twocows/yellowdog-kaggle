package org.twocows.yellowdog.kaggle;

import org.twocows.yellowdog.kafka.KafkaJsonSerializer;

public class KaggleLineSerializer extends KafkaJsonSerializer<KaggleLine> {

	@Override
	public byte[] serialize(String topic, KaggleLine data) {
		return super.serialize(topic, data);
	}

}
