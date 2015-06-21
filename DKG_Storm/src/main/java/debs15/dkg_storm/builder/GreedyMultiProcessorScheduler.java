package debs15.dkg_storm.builder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

/**
 * @author Nicolo Rivetti
 * 
 *         This class implements a Greedy Multi-Processor Scheduling algorithm.
 *
 */
public class GreedyMultiProcessorScheduler {

	public final HashMap<Integer, Double> partitions;
	public final ArrayList<Instance> instances;
	public final TreeSet<Integer> ids;

	/**
	 * Instantiate a Greedy Multi-Processor Scheduler to map the given set of
	 * partitions to the given number of instances.
	 * 
	 * @param partitions
	 *            a set of key partitions identifiers and frequencies.
	 * @param instancesNum
	 *            the number of available instances
	 */
	public GreedyMultiProcessorScheduler(HashMap<Integer, Double> partitions, int instancesNum) {

		this.partitions = partitions;
		this.ids = new TreeSet<Integer>(new PartitionSorter(partitions));
		this.ids.addAll(partitions.keySet());

		instances = new ArrayList<Instance>();

		for (int i = 0; i < instancesNum; i++) {
			instances.add(new Instance(i));
		}
	}

	/**
	 * Runs the Multi-Processor Scheduling algorithm.
	 * 
	 * @return A set of Instance objects that encapsulate the mapping from key
	 *         partitions to instances.
	 */
	public ArrayList<Instance> run() {
		for (Iterator<Integer> iterator = ids.iterator(); iterator.hasNext();) {
			int id = (Integer) iterator.next();
			getEmptiestInstance().addLoad(id, partitions.get(id));
		}

		return instances;
	}

	private Instance getEmptiestInstance() {
		Instance target = null;
		for (Instance instance : instances) {

			if (target == null) {
				target = instance;
			} else if (target.getLoad() > instance.getLoad()) {
				target = instance;
			}

		}
		return target;
	}

	class PartitionSorter implements Comparator<Integer> {

		Map<Integer, Double> base;

		public PartitionSorter(Map<Integer, Double> base) {
			this.base = base;
		}

		public int compare(Integer a, Integer b) {
			if (base.get(a) > base.get(b)) {
				return -1;
			} else if (base.get(a) < base.get(b)) {
				return 1;
			} else {
				if (a < b) {
					return -1;
				} else if (a > b) {
					return 1;
				}
				return 0;

			}
		}
	}

}
