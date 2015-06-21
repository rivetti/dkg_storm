package debs15.dkg_storm.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.math3.random.RandomDataGenerator;

/**
 * @author Nicolo Rivetti
 * 
 *         This class implements the learning and building phase of DKG
 *
 */
public class DKGBuilder {

	private final HashMap<Integer, Double> mostFrequent;
	private final int k;

	private final CWHashFunction hashFunction;
	private final double[] hashFunctionDistrib;

	private final SpaceSaving ss;

	/**
	 * @param theta
	 *            double value in (0,1], heavy hitter threshold, i.e., all keys
	 *            with an empirical probability larger than or equal to theta
	 *            belong to the heavy hitter set.
	 * @param epsilonFactor
	 *            defines the ration between the Space Saving's Heavy Hitter
	 *            threshold theta and precision parameter epsilon: epsilon =
	 *            theta / epsilonFactor.
	 * @param k
	 *            the number of available instances
	 * @param factor
	 *            double value >=1, set the number of buckets of sparse items to
	 *            factor * k (number of available instances).
	 */
	public DKGBuilder(double theta, double epsilonFactor, int k, double factor) {
		this.k = k;
		this.mostFrequent = new HashMap<Integer, Double>();

		int codomain = (int) Math.ceil(k * factor);

		RandomDataGenerator uniformGenerator = new RandomDataGenerator();
		uniformGenerator.reSeed(1000);

		long prime = 10000019;
		long a = uniformGenerator.nextLong(1, prime - 1);
		long b = uniformGenerator.nextLong(1, prime - 1);

		this.hashFunction = new CWHashFunction(codomain, prime, a, b);
		this.hashFunctionDistrib = new double[codomain];

		this.ss = new SpaceSaving(theta / epsilonFactor, theta);
	}

	/**
	 * Updates DKG's data structures with key, ie, performs the learning phase
	 * of DKG.
	 * 
	 * @param key
	 */

	public void newSample(int key) {
		ss.newSample(key);
		hashFunctionDistrib[hashFunction.hash(key)]++;
	}

	/**
	 * Returns a DKGHash objects that encapsulate a close the optimal mapping
	 * built given the updates from the learning phase.
	 * 
	 * @return a DKGHash object thath encapsulates DKG's global mapping (Heavy
	 *         Hitters to operator instances and buckets of Sparse Items to
	 *         instances)
	 */
	public DKGHash build() {

		HashMap<Integer, Integer> ssMap = ss.getHeavyHitters();

		for (Entry<Integer, Integer> entry : ssMap.entrySet()) {
			hashFunctionDistrib[hashFunction.hash(entry.getKey())] -= entry.getValue();
		}

		for (Entry<Integer, Integer> entry : ssMap.entrySet()) {
			this.mostFrequent.put(entry.getKey(), (double) entry.getValue());
		}

		for (int i = 0; i < hashFunctionDistrib.length; i++) {
			mostFrequent.put(-(i + 1), hashFunctionDistrib[i]);
		}
		GreedyMultiProcessorScheduler mps = new GreedyMultiProcessorScheduler(mostFrequent, k);

		ArrayList<Instance> instances = mps.run();

		HashMap<Integer, Integer> mostFrequentMapping = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> hashFunctionMapping = new HashMap<Integer, Integer>();
		for (Instance instance : instances) {
			for (int itemId : instance.getPartitions()) {
				if (itemId >= 0) {
					mostFrequentMapping.put(itemId, instance.id);
				} else {
					hashFunctionMapping.put((-itemId) - 1, instance.id);
				}
			}

		}

		return new DKGHash(mostFrequentMapping, hashFunction, hashFunctionMapping);

	}
}
