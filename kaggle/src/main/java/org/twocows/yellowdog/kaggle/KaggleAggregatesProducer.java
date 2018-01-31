package org.twocows.yellowdog.kaggle;

import java.util.Iterator;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.twocows.yellowdog.kafka.SimpleKafkaProducer;

/**
 * Class to produce JSON serialized lines for Kafka.
 * 
 * https://www.kaggle.com/unitednations/global-commodity-trade-statistics/downloads/commodity_trade_statistics_data.csv
 * 
 * @author dick
 *
 */
public class KaggleAggregatesProducer extends SimpleKafkaProducer<Long, KaggleAggregates> {

	public static final String CLIENT_ID_CONFIG = "KaggleAggregateProducer";

	public static final String TOPIC_DEFAULT = "KaggleAggregates";
	
	private String group;
	
	private KaggleLineFlowAggregates kaggleLineFlowAggregates;
	
    public KaggleAggregatesProducer() {
		super();
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public KaggleLineFlowAggregates getKaggleLineFlowAggregates() {
		return kaggleLineFlowAggregates;
	}

	public void setKaggleLineFlowAggregates(KaggleLineFlowAggregates kaggleLineFlowAggregates) {
		this.kaggleLineFlowAggregates = kaggleLineFlowAggregates;
	}

	@Override
	protected ProducerRecord<Long, KaggleAggregates> start() {
		return new ProducerRecord<Long, KaggleAggregates>(TOPIC_DEFAULT, 0L, new KaggleAggregates(group, new KaggleLineFlowAggregates()));
	}

	@Override
	protected ProducerRecord<Long, KaggleAggregates> end() {
		return new ProducerRecord<Long, KaggleAggregates>(TOPIC_DEFAULT, -1L, new KaggleAggregates(group, new KaggleLineFlowAggregates()));
	}

	@Override
	protected Iterator<ProducerRecord<Long, KaggleAggregates>> iterator() {
		return new Iterator<ProducerRecord<Long,KaggleAggregates>>() {

			boolean hasNext = true;
			
			@Override
			public boolean hasNext() {
				return hasNext;
			}

			@Override
			public ProducerRecord<Long, KaggleAggregates> next() {
				hasNext = false;
				final ProducerRecord<Long, KaggleAggregates> producerRecord = new ProducerRecord<Long, KaggleAggregates>(TOPIC_DEFAULT, 1L, new KaggleAggregates(group, kaggleLineFlowAggregates));
				return producerRecord;
			}
		};
	}

	@Override
	public String toString() {
		return group + " " + count;
	}
	
	
}
