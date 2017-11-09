package edu.jhu.wilson.david.record.model;

/**
 * A simple container of {@link Field}s. Defines basic functionality of adding,
 * removing, and retrieving these {@link Field}s
 */
public interface Record {

	Iterable<Field> getFields();

	void addField(final Field field);

	void removeFieldsByName(final String name);

}
