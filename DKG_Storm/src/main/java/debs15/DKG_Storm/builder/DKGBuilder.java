package debs15.DKG_Storm.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.math3.random.RandomDataGenerator;

public class DKGBuilder {

	private final HashMap<Integer, Double> mostFrequent;
	private final int k;

	private final CWHashFunction hashFunction;
	private final double[] hashFunctionDistrib;

	private final SpaceSaving ss;

	public DKGBuilder(double theta, double epsilonFactor, int k, double codomainFactor) {
		this.k = k;
		this.mostFrequent = new HashMap<Integer, Double>();

		int codomain = (int) Math.ceil(k * codomainFactor);

		RandomDataGenerator uniformGenerator = new RandomDataGenerator();
		uniformGenerator.reSeed(1000);

		long prime = 10000019;
		long a = uniformGenerator.nextLong(1, prime - 1);
		long b = uniformGenerator.nextLong(1, prime - 1);

		this.hashFunction = new CWHashFunction(codomain, prime, a, b);
		this.hashFunctionDistrib = new double[codomain];

		this.ss = new SpaceSaving(theta / epsilonFactor, theta);
	}

	public void newSample(int value) {
		ss.newSample(value);
		hashFunctionDistrib[hashFunction.hash(value)]++;
	}

	public DKGHash run() {

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
		MultiProcessorScheduling mps = new MultiProcessorScheduling(mostFrequent, k);

		ArrayList<Bucket> buckets = mps.run();

		HashMap<Integer, Integer> mostFrequentMapping = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> hashFunctionMapping = new HashMap<Integer, Integer>();
		for (Bucket bucket : buckets) {
			for (int itemId : bucket.getItems()) {
				if (itemId >= 0) {
					mostFrequentMapping.put(itemId, bucket.id);
				} else {
					hashFunctionMapping.put((-itemId) - 1, bucket.id);
				}
			}

		}

		return new DKGHash(mostFrequentMapping, hashFunction, hashFunctionMapping);

	}
}
