package org.twocows.yellowdog.kaggle;

import java.util.Map;

/**
 * 
 * @author dick
 *
 */
public class KaggleAggregates {
	
	/**
	 * The group this belongs to.
	 */
	protected String group;
	
	/**
	 * The data.
	 */
	protected KaggleLineFlowAggregates aggregates;

	public KaggleAggregates() {
	}
	
	public KaggleAggregates(String group, KaggleLineFlowAggregates aggregates) {
		super();
		this.group = group;
		this.aggregates = aggregates;
	}
	
	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public KaggleLineFlowAggregates getAggregates() {
		return aggregates;
	}

	public void setAggregates(KaggleLineFlowAggregates aggregates) {
		this.aggregates = aggregates;
	}

	@Override
	public String toString() {
		return group + " " + aggregates;
	}

}
