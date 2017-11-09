package edu.jhu.wilson.david.record.operations;

import edu.jhu.wilson.david.record.model.Field;
import edu.jhu.wilson.david.record.model.Record;

/**
 * Defines a simple API for filtering/removing {@link Field}s from a
 * {@link Record}
 *
 */
public interface FieldFilter {

	/**
	 * Returns a {@link Record} based on the input, minus any filtered
	 * {@link Field}s. This could be the given {@link Record} or a new instance.
	 * 
	 * @param record
	 *            - {@link Record} for which to filter
	 * @return - A filtered version of the given {@link Record}
	 */
	Record filterFields(final Record record);

}
