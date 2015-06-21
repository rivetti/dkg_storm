package debs15.DKG_Storm.builder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

public class MultiProcessorScheduling {

	public final HashMap<Integer, Double> jobs;
	public final ArrayList<Bucket> bins;
	public final TreeSet<Integer> ids;

	public MultiProcessorScheduling(HashMap<Integer, Double> jobs, int binsNum) {

		this.jobs = jobs;
		this.ids = new TreeSet<Integer>(new JobSorter(jobs));
		this.ids.addAll(jobs.keySet());

		bins = new ArrayList<Bucket>();

		for (int i = 0; i < binsNum; i++) {
			bins.add(new Bucket(i));
		}
	}

	public ArrayList<Bucket> run() {
		for (Iterator<Integer> iterator = ids.iterator(); iterator.hasNext();) {
			int id = (Integer) iterator.next();
			getEmptiestBin().addLoad(id, jobs.get(id));
		}

		return bins;
	}

	private Bucket getEmptiestBin() {
		Bucket target = null;
		for (Bucket bin : bins) {

			if (target == null) {
				target = bin;
			} else if (target.getLoad() > bin.getLoad()) {
				target = bin;
			}

		}
		return target;
	}

	class JobSorter implements Comparator<Integer> {

		Map<Integer, Double> base;

		public JobSorter(Map<Integer, Double> base) {
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
