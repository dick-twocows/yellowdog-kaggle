package org.twocows.yellowdog.kaggle;

import java.util.HashMap;
import java.util.Map;

/**
 * Aggregate by line flow using a Map<String, Long>.
 * 
 * @author dick
 *
 */
public class KaggleLineFlowAggregate {

	private Map<String, Long> aggregates = new HashMap<>();

	public KaggleLineFlowAggregate() {
		super();
	}
	
	public void update(final String flow, final long tradeUSD) {
		aggregates.merge(
			flow, 
			tradeUSD, 
			(current, increment) -> {
				return current + increment;
			}
		);
	}

	/*
	 * POJO
	 */

	public Map<String, Long> getAggregates() {
		return aggregates;
	}

	public void setAggregates(Map<String, Long> aggregates) {
		this.aggregates = aggregates;
	}
	

	@Override
	public String toString() {
		return aggregates.toString();
	}
}
