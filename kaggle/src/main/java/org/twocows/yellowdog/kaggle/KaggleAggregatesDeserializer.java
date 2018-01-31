package org.twocows.yellowdog.kaggle;

import org.twocows.yellowdog.kafka.KafkaJsonDeserializer;

public class KaggleAggregatesDeserializer extends KafkaJsonDeserializer<KaggleAggregates> {

	@Override
	protected Class<KaggleAggregates> getType() {
		return KaggleAggregates.class;
	}

}
