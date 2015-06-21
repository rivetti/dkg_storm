package dkg_storm.builder;

/**
 * @author Nicolo Rivetti
 */
public class CWHashFunction {

	public final long codomainSize;
	public final long prime;
	public final long a;
	public final long b;

	/**
	 * 
	 * Instantiate a Hash Function approximating the 2-universality property,
	 * through the technique proposed by Carter and Wegman.
	 * 
	 * @param codomainSize
	 *            desired co-domain cardinality of the hash function.
	 * @param prime
	 *            a prime number that must be larger than the cardinality of the
	 *            hash function domain.
	 * @param a
	 *            a random non null integer modulo prime
	 * @param b
	 *            a random integer modulo prime
	 */
	public CWHashFunction(int codomainSize, long prime, long a, long b) {
		super();
		this.codomainSize = codomainSize;
		this.prime = prime;
		this.a = a;
		this.b = b;
	}

	/**
	 * @param value
	 *            a value belonging to the hash function domain.
	 * @return the image in the hash function co-domain of provided value
	 *         parameter.
	 */
	public int hash(int value) {
		long longValue = (long) value;
		return (int) ((int) ((a * longValue + b) % prime) % codomainSize);
	}

}
