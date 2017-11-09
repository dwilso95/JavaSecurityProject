package edu.jhu.wilson.david.record;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

public class StandardRecord implements Record {

	private final List<Field> fields;

	public StandardRecord() {
		fields = Lists.newArrayList();
	}

	public Iterable<Field> getFields() {
		return fields;
	}

	public void addField(final Field field) {
		fields.add(field);
	}

	public void removeFieldsByName(String name) {
		final Iterator<Field> iter = fields.iterator();
		while (iter.hasNext()) {
			if (iter.next().getName().equals(name)) {
				iter.remove();
			}
		}
	}

}
