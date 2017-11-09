package edu.jhu.wilson.david.record.operations;

import edu.jhu.wilson.david.record.model.Record;

/**
 * Defines a simple API for deciding whether or not to a given a {@link Record}
 * satisfies a particular filter criteria
 *
 */
public interface RecordFilter {

	boolean filter(final Record record);

}
