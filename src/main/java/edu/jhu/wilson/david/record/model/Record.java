package edu.jhu.wilson.david.record.model;

/**
 * A simple container of {@link Field}s. Defines basic functionality of adding,
 * removing, and retrieving these {@link Field}s
 */
public interface Record {

	/**
	 * Returns an iterable of all fields in this record
	 * 
	 * @return {@link Iterable}
	 */
	Iterable<Field> getFields();

	/**
	 * Adds the given field to this record
	 * 
	 * @param field
	 *            - the {@link Field} to add
	 */
	void addField(final Field field);

	/**
	 * Removes all fields with the given name
	 * 
	 * @param name
	 *            - the name of the {@link Field}s to remove from this record
	 */
	void removeFieldsByName(final String name);

}
