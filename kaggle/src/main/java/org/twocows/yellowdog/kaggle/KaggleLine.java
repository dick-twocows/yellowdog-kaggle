package org.twocows.yellowdog.kaggle;

import java.util.Arrays;

/**
 * POJO around the following input.
 * 
 * country_or_area,year,comm_code,commodity,flow,trade_usd,weight_kg,quantity_name,quantity,category
 * Afghanistan,2016,010410,"Sheep, live",Export,6088,2339,Number of items,51,01_live_animals
 * Afghanistan,2016,010420,"Goats, live",Export,3958,984,Number of items,53,01_live_animals
 * Afghanistan,2008,010210,"Bovine animals, live pure-bred breeding",Import,1026804,272,Number of items,3769,01_live_animals
 * Albania,2016,010290,"Bovine animals, live, except pure-bred breeding",Import,2414533,1114023,Number of items,6853,01_live_animals
 * Albania,2016,010392,"Swine, live except pure-bred breeding > 50 kg",Import,14265937,9484953,Number of items,96040,01_live_animals
 * 
 * @author dick
 *
 */
public class KaggleLine {
	
	public static final int COUNTRY_OR_AREA_INDEX = 0;
	
	public static final int FLOW_INDEX = 4;
	
	public static final int TRADE_USD_INDEX = 5;

	/**
	 * The group this belongs to.
	 */
	private String group;
	
	/**
	 * Unique id per group.
	 */
	private long id;
	
	/**
	 * The data.
	 */
	private String[] line;

	public KaggleLine() {
	}
	
	public KaggleLine(String group, long id, String[] line) {
		super();
		this.group = group;
		this.id = id;
		this.line = line;
	}
	
	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String[] getLine() {
		return line;
	}

	public void setLine(String[] line) {
		this.line = line;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((group == null) ? 0 : group.hashCode());
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + Arrays.hashCode(line);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KaggleLine other = (KaggleLine) obj;
		if (group == null) {
			if (other.group != null)
				return false;
		} else if (!group.equals(other.group))
			return false;
		if (id != other.id)
			return false;
		if (!Arrays.equals(line, other.line))
			return false;
		return true;
	}
}
