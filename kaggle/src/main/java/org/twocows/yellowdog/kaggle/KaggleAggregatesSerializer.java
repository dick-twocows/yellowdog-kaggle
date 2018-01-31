package org.twocows.yellowdog.kaggle;

import org.twocows.yellowdog.kafka.KafkaJsonSerializer;

public class KaggleAggregatesSerializer extends KafkaJsonSerializer<KaggleAggregates> {

	@Override
	public byte[] serialize(String topic, KaggleAggregates data) {
		return super.serialize(topic, data);
	}

}
