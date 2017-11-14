package edu.jhu.wilson.david.record.model;

/**
 * Interface which defines a simple three-tuple POJO like object containing a
 * Name, Value, and Visibility.
 * 
 * Meant to be used in conjunction with {@link Record}
 */
public interface Field {

	/**
	 * Returns the name of the field
	 * 
	 * @return String
	 */
	String getName();

	/**
	 * Returns the value of the field
	 * 
	 * @return String
	 */
	String getValue();

	/**
	 * Returns the visibility of the field
	 * 
	 * @return String
	 */
	String getVisibility();

	/**
	 * Sets the name of the field
	 */
	void setName(final String name);

	/**
	 * Sets the value of the field
	 */
	void setValue(final String value);

	/**
	 * Sets the visibility of the field
	 */
	void setVisibility(final String visibility);
}
