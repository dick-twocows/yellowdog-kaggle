package org.twocows.yellowdog.kaggle;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.twocows.yellowdog.kafka.SimpleKafkaConsumer;

/**
 * @author dick
 */
public class KaggleLineAggregateConsumer extends SimpleKafkaConsumer<Long, KaggleLine> {

	public static final String TOPIC_DEFAULT = "KaggleLine";

    /*
     * There are ~195 countries in the world so use 256.
     */
    protected final KaggleLineFlowAggregates aggregates = new KaggleLineFlowAggregates(256);
    
    protected String group;
    
    protected KaggleLine end;
    
	public KaggleLineAggregateConsumer() {
	}
	
	public KaggleLineFlowAggregates getAggregates() {
		return aggregates;
	}

	@Override
	public ConsumerRecord<Long, KaggleLine> handle(final ConsumerRecord<Long, KaggleLine> consumerRecord) {
		if (consumerRecord.value().getId() == 0) {
			group = consumerRecord.value().getGroup();
			end = new KaggleLine(group, -1, new String[0]);
			return consumerRecord;
		} else if (consumerRecord.value().equals(end)) {
			poll = false;
			return consumerRecord;
		}

		final String[] line = consumerRecord.value().getLine();
		/*
		 * Get or put as required.
		 */
		KaggleLineFlowAggregate importExportAggregate = aggregates.get(line[KaggleLine.COUNTRY_OR_AREA_INDEX]);
		if (importExportAggregate == null) {
			importExportAggregate = new KaggleLineFlowAggregate();
			aggregates.put(line[KaggleLine.COUNTRY_OR_AREA_INDEX], importExportAggregate);
		}
		/*
		 * Update aggregate.
		 */
		importExportAggregate.update(line[KaggleLine.FLOW_INDEX], Long.valueOf(line[KaggleLine.TRADE_USD_INDEX]));
		
		return consumerRecord;
	}
}
