package edu.jhu.wilson.david.record.serialization;

import java.io.Closeable;
import java.util.Iterator;

import edu.jhu.wilson.david.record.model.Record;

/**
 * Interface for iterating over 0 -> many {@link Record}s
 * 
 * A simpler definition of Java's {@link Iterator}
 */
public interface RecordReader extends Closeable {

	/**
	 * Whether or not there is another {@link Record} to read
	 * 
	 * @return - true if there is another {@link Record}, else false
	 */
	boolean hasNext();

	/**
	 * Returns the next {@link Record} available
	 * 
	 * @return - the next {@link Record}
	 */
	Record next();

}
