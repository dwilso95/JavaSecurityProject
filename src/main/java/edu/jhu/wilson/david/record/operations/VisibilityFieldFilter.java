package edu.jhu.wilson.david.record.operations;

import java.util.Iterator;
import java.util.Set;

import edu.jhu.wilson.david.record.Field;
import edu.jhu.wilson.david.record.Record;

public class VisibilityFieldFilter implements FieldFilter {

	private final Set<String> visibilities;

	public VisibilityFieldFilter(final Set<String> visibilities) {
		this.visibilities = visibilities;
	}

	@Override
	public Record filterFields(final Record record) {
		final Iterator<Field> iter = record.getFields().iterator();
		while (iter.hasNext()) {
			if (!visibilities.contains(iter.next().getVisibility())) {
				iter.remove();
			}
		}
		return record;
	}

}
