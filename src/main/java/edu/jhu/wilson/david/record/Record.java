package edu.jhu.wilson.david.record;

public interface Record {

	Iterable<Field> getFields();

	void addField(final Field field);

	void removeFieldsByName(final String name);
	
}
