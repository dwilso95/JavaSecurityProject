package edu.jhu.wilson.david.record;

public interface Field {

	String getName();

	String getValue();

	String getVisibility();

	void setName(final String name);

	void setValue(final String value);

	void setVisibility(final String visibility);
}
