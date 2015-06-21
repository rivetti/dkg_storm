package debs15.dkg_storm.builder;

/**
 * @author Nicolo Rivetti
 * 
 *         The instances of this class are Hash Functions approximating the
 *         2-universality property, implementing the technique proposed by
 *         Carter and Wegman.
 *
 */
public class CWHashFunction {

	public final long codomainSize;
	public final long prime;
	public final long a;
	public final long b;

	/**
	 * @param codomainSize desired co-domain cardinality of the hash function.
	 * @param prime a prime number that must be larger than the cardinality of the hash function domain.
	 * @param a a random integer such that TODO
	 * @param b a random integer such that TODO
	 */
	public CWHashFunction(int codomainSize, long prime, long a, long b) {
		super();
		this.codomainSize = codomainSize;
		this.prime = prime;
		this.a = a;
		this.b = b;
	}

	
	/**
	 * @param value a value belonging to the hash function domain.
	 * @return the image in the hash function co-domain of provided value parameter. 
	 */
	public int hash(int value) {
		long longValue = (long) value;
		return (int) ((int) ((a * longValue + b) % prime) % codomainSize);
	}

}
