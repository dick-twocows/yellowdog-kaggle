package org.twocows.yellowdog.kaggle;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.twocows.yellowdog.kafka.SimpleKafkaConsumer;

/**
 * @author dick
 */
public class KaggleAggregatesConsumer extends SimpleKafkaConsumer<Long, KaggleAggregates> {

    protected KaggleAggregates aggregates;
    
    protected String group;
    
    protected KaggleLine end;
    
	public KaggleAggregatesConsumer() {
	}
	
	public KaggleAggregates getAggregates() {
		return aggregates;
	}

	public void setAggregates(KaggleAggregates aggregates) {
		this.aggregates = aggregates;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	@Override
	public ConsumerRecord<Long, KaggleAggregates> handle(final ConsumerRecord<Long, KaggleAggregates> consumerRecord) {
		if (consumerRecord.key() == -1) {
			poll = false;
			return consumerRecord;
		}
		aggregates = consumerRecord.value();
		return consumerRecord;
	}
}
