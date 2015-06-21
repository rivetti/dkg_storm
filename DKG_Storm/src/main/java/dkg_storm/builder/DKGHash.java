package dkg_storm.builder;

import java.util.HashMap;

/**
 * @author Nicolo Rivetti
 */
public class DKGHash {

	public final HashMap<Integer, Integer> mostFrequentMapping;
	public final CWHashFunction hashfunction;
	public final HashMap<Integer, Integer> hashFunctionMapping;

	/**
	 * 
	 * This class encapsulates DKG's global mapping (Heavy Hitters to operator
	 * instances and buckets of Sparse Items to instances)
	 * 
	 * @param mostFrequentMapping
	 *            Heavy Hitters to operator instances mapping
	 * @param hashfunction
	 *            the hash function that partitions the Sparse Items into
	 *            buckets.
	 * @param hashFunctionMapping
	 *            buckets of Sparse Items to instances mapping
	 */
	public DKGHash(HashMap<Integer, Integer> mostFrequentMapping, CWHashFunction hashfunction,
			HashMap<Integer, Integer> hashFunctionMapping) {
		super();
		this.mostFrequentMapping = mostFrequentMapping;
		this.hashfunction = hashfunction;
		this.hashFunctionMapping = hashFunctionMapping;
	}

	/**
	 * @param key
	 *            a Heavy Hitter or Sparse Item key value.
	 * @return the instances mapped to the provided Heavy Hitter or Sparse Item
	 *         key.
	 */
	public int map(int key) {
		Integer ret = mostFrequentMapping.get(key);
		if (ret != null) {
			return ret;
		}

		return hashFunctionMapping.get(hashfunction.hash(key));
	}
}
