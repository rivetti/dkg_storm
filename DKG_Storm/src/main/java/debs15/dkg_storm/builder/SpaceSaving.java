package debs15.dkg_storm.builder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * @author Nicolo Rivetti
 * 
 *         This class implements the Space Saving algorithm presented by
 *         Metwally, Agrawal and El Abbadi in "Efficient computation of frequent
 *         and top-k elements in data streams" (ICDT'05).
 *
 */
public class SpaceSaving {

	public final double epsilon;
	public final double phi;

	private final HashMap<Integer, SpaceSaving.Elem> l;
	private final int lSizeLimit;

	private final TreeMap<Integer, HashSet<Integer>> minSets;

	private int min = 1;
	public double m = 0;

	/**
	 * @param epsilon
	 *            double value in (0,theta], precision parameter setting the
	 *            memory footprint, ie, the algorithm stores 1/epsilon entries.
	 * @param theta
	 *            double value in (0,1], heavy hitter threshold, i.e., all keys
	 *            with an empirical probability larger than or equal to theta
	 *            belong to the heavy hitter set.
	 */
	public SpaceSaving(double epsilon, double theta) {
		super();
		this.epsilon = epsilon;
		this.phi = theta;

		this.lSizeLimit = (int) Math.ceil(1.0 / epsilon);

		this.l = new HashMap<Integer, SpaceSaving.Elem>((int) Math.ceil(1 + lSizeLimit / 0.75));

		this.minSets = new TreeMap<Integer, HashSet<Integer>>();

	}

	/**
	 * Updated the Space Saving algorithm with an identifier.
	 * 
	 * @param identifier
	 */
	public void newSample(int identifier) {

		m++;

		if (l.containsKey(identifier)) {
			int count = l.get(identifier).count;

			minSets.get(count).remove(identifier);
			if (minSets.get(count).isEmpty()) {
				minSets.remove(count);
			}

			l.get(identifier).count++;
			count++;

			if (!minSets.containsKey(count)) {
				minSets.put(count, new HashSet<Integer>((int) Math.ceil(1 + lSizeLimit / 0.75)));
			}

			minSets.get(count).add(identifier);

		} else if (l.size() < lSizeLimit) {
			l.put(identifier, new Elem(1, 0));

			if (!minSets.containsKey(1)) {
				minSets.put(min, new HashSet<Integer>((int) Math.ceil(1 + lSizeLimit / 0.75)));
			}

			minSets.get(1).add(identifier);

		} else {
			int victimKey = minSets.firstEntry().getValue().iterator().next();
			Elem victimEntry = l.get(victimKey);

			l.remove(victimKey);
			minSets.firstEntry().getValue().remove(victimKey);

			if (minSets.firstEntry().getValue().isEmpty()) {
				minSets.remove(minSets.firstEntry().getKey());
			}

			l.put(identifier, new Elem(victimEntry.count + 1, victimEntry.count));

			if (!minSets.containsKey(victimEntry.count + 1)) {
				minSets.put(victimEntry.count + 1, new HashSet<Integer>((int) Math.ceil(1 + lSizeLimit / 0.75)));
			}

			minSets.get(victimEntry.count + 1).add(identifier);

		}

	}

	private class Elem {
		public int count;
		public int e;

		public Elem(int count, int e) {
			super();
			this.count = count;
			this.e = e;
		}

	}

	/**
	 * @param identifier
	 * @return true if the identifier belongs to the Heavy Hitter set.
	 */
	public boolean isHeavyHitter(int identifier) {
		return l.get(identifier).count - l.get(identifier).e >= Math.ceil(phi * m);
	}

	/**
	 * @return a <Identifier, Frequency> map containing all the identifiers of
	 *         the Heavy Hitters and their frequency estimation
	 */
	public HashMap<Integer, Integer> getHeavyHitters() {
		HashMap<Integer, Integer> res = new HashMap<Integer, Integer>();
		for (Entry<Integer, Elem> entry : l.entrySet()) {
			if (isHeavyHitter(entry.getKey())) {
				res.put(entry.getKey(), entry.getValue().count - entry.getValue().e);
			}
		}
		return res;
	}
}
