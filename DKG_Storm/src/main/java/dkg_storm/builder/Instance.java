package dkg_storm.builder;

import java.util.ArrayList;

/**
 * @author Nicolo Rivetti
 * 
 */

public class Instance {

	public final int id;

	private double load;
	private final ArrayList<Integer> partitions;

	/**
	 * This class represents the available operator instances to which the
	 * scheduler has to map partitions of the key space.
	 * 
	 * Creates a new instance with the given identifier and no initial load.
	 * 
	 * @param id
	 *            instance identifier
	 */
	public Instance(int id) {
		this.id = id;
		this.load = 0;
		this.partitions = new ArrayList<Integer>();

	}

	/**
	 * 
	 * This class represents the available operator instances to which the
	 * scheduler has to map partitions of the key space.
	 * 
	 * Creates a new instance with the given identifier and initial load.
	 * 
	 * @param id
	 *            instance identifier
	 * @param load
	 *            initial load
	 */
	public Instance(int id, double load) {
		this.id = id;
		this.load = load;
		this.partitions = new ArrayList<Integer>();

	}

	/**
	 * @return the set of identifiers of the key partitions mapped to this
	 *         instance
	 */
	public ArrayList<Integer> getPartitions() {
		return partitions;
	}

	/**
	 * @return the total (linear) load mapped to this instance
	 */
	public double getLoad() {
		return this.load;
	}

	/**
	 * Add a partitions, and its load, to this instance
	 * 
	 * @param partition
	 *            identigier of the partition to be added
	 * @param load
	 *            load of the partition to be added
	 */
	public void addLoad(Integer partition, double load) {
		partitions.add(partition);
		load += load;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + ((partitions == null) ? 0 : partitions.hashCode());
		long temp;
		temp = Double.doubleToLongBits(load);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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
		Instance other = (Instance) obj;
		if (id != other.id)
			return false;
		if (partitions == null) {
			if (other.partitions != null)
				return false;
		} else if (!partitions.equals(other.partitions))
			return false;
		if (Double.doubleToLongBits(load) != Double.doubleToLongBits(other.load))
			return false;
		return true;
	}

}