package edu.jhu.wilson.david.record.operations;

import java.util.Iterator;
import java.util.List;

import edu.jhu.wilson.david.record.model.Field;
import edu.jhu.wilson.david.record.model.Record;

public class KeyValueSearch {

	private final List<FieldFilter> fieldFilters;
	private final List<RecordFilter> recordFilters;

	public KeyValueSearch(List<FieldFilter> fieldFilters, List<RecordFilter> recordFilters) {
		this.fieldFilters = fieldFilters;
		this.recordFilters = recordFilters;
	}

	public boolean containsFieldValuePair(String name, String value, Record record) {
		applyFieldFilters(record);
		if (filterRecord(record)) {
			return false;
		}

		final Iterator<Field> iter = record.getFields().iterator();
		while (iter.hasNext()) {
			final Field field = iter.next();
			if (field.getName().equals(name) && field.getValue().equals(value)) {
				return true;
			}
		}

		return false;
	}

	private void applyFieldFilters(final Record record) {
		for (FieldFilter filter : fieldFilters) {
			filter.filterFields(record);
		}
	}

	private boolean filterRecord(final Record record) {
		for (RecordFilter recordFilter : recordFilters) {
			if (recordFilter.filter(record)) {
				return true;
			}
		}
		return false;
	}

}
