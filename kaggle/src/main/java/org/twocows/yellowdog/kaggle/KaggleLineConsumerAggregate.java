package org.twocows.yellowdog.kaggle;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author dick
 */
public class KaggleLineConsumerAggregate extends SimpleKafkaConsumer<Long, KaggleLine> {

	public static final String TOPIC_DEFAULT = "KaggleLine";

    /*
     * There are 195 countries in the world so use 256.
     */
    private final Map<String, KaggleLineImportExportAggregate> aggregates = new HashMap<>(256);
    
	public KaggleLineConsumerAggregate() {
	}
	
	public Map<String, KaggleLineImportExportAggregate> getAggregates() {
		return Collections.unmodifiableMap(aggregates);
	}

	@Override
	public ConsumerRecord<Long, KaggleLine> handle(final ConsumerRecord<Long, KaggleLine> consumerRecord) {
		if (consumerRecord.value().equals(KaggleLineProducer.START)) {
			return consumerRecord;
		} else if (consumerRecord.value().equals(KaggleLineProducer.END)) {
			poll = false;
			return consumerRecord;
		}

		final String[] line = consumerRecord.value().getLine();
		/*
		 * Get or put as required.
		 */
		KaggleLineImportExportAggregate importExportAggregate = aggregates.get(line[KaggleLine.COUNTRY_OR_AREA_INDEX]);
		if (importExportAggregate == null) {
			importExportAggregate = new KaggleLineImportExportAggregate();
			aggregates.put(line[KaggleLine.COUNTRY_OR_AREA_INDEX], importExportAggregate);
		}
		/*
		 * Update based on flow or exception.
		 */
		importExportAggregate.update(line[KaggleLine.FLOW_INDEX], Long.valueOf(line[KaggleLine.TRADE_USD_INDEX]));
		
		return consumerRecord;
	}
}
