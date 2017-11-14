package edu.jhu.wilson.david.record.operations;

import edu.jhu.wilson.david.record.model.Record;

/**
 * Defines a simple API for deciding whether or not to a given a {@link Record}
 * satisfies a particular filter criteria
 *
 */
public interface RecordFilter {

	/**
	 * Returns whether or not the {@link Record} satisfies this filter
	 * 
	 * @param record
	 *            - the {@link Record} to test against the filter
	 * @return - true iff the record should be filtered, else false
	 */
	boolean filter(final Record record);

}
