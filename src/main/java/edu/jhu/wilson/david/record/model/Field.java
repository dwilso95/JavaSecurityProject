package edu.jhu.wilson.david.record.model;

/**
 * Interface which defines a simple three-tuple POJO like object containing a
 * Name, Value, and Visibility.
 * 
 * Meant to be used in conjunction with {@link Record}
 */
public interface Field {

	String getName();

	String getValue();

	String getVisibility();

	void setName(final String name);

	void setValue(final String value);

	void setVisibility(final String visibility);
}
