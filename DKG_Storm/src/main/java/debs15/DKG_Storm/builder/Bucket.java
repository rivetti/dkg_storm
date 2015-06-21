package debs15.DKG_Storm.builder;

import java.util.ArrayList;

public class Bucket {

	public final int id;

	private double load;
	private final ArrayList<Integer> items;

	public Bucket(int id) {
		this.id = id;
		this.load = 0;
		this.items = new ArrayList<Integer>();

	}

	public Bucket(int id, double size) {
		this.id = id;
		this.load = size;
		this.items = new ArrayList<Integer>();

	}

	public ArrayList<Integer> getItems() {
		return items;
	}

	public double getLoad() {
		return this.load;
	}

	public void addLoad(Integer item, double value) {
		items.add(item);
		load += value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + ((items == null) ? 0 : items.hashCode());
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
		Bucket other = (Bucket) obj;
		if (id != other.id)
			return false;
		if (items == null) {
			if (other.items != null)
				return false;
		} else if (!items.equals(other.items))
			return false;
		if (Double.doubleToLongBits(load) != Double.doubleToLongBits(other.load))
			return false;
		return true;
	}

}