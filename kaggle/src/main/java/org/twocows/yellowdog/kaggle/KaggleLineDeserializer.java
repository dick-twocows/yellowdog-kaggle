package org.twocows.yellowdog.kaggle;

import org.twocows.yellowdog.kafka.KafkaJsonDeserializer;

public class KaggleLineDeserializer extends KafkaJsonDeserializer<KaggleLine> {
	
	public KaggleLineDeserializer() {
	}

	@Override
	protected Class<KaggleLine> getType() {
		return KaggleLine.class;
	}
}
