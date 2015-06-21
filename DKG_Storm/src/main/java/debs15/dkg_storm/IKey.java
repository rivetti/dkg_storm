package debs15.dkg_storm;

import java.util.List;

/**
 * @author Nicolo Rivetti
 * 
 *         This interface provides the mean through which the user is able to
 *         define the key, through which the operator state is partitioned,
 *         associated with each tuple of the stream
 *
 */
public interface IKey {

	
	/**
	 * @param values A list of object, ie, the representation of a tuple in Apache Storm.
	 * @return an integer representing the key, through which the operator state is partitioned,
 *         associated with each tuple of the stream.
	 */
	public int get(List<Object> values);

}
