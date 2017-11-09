package edu.jhu.wilson.david.record.operations;

import java.util.Iterator;
import java.util.Set;

import edu.jhu.wilson.david.record.model.Field;
import edu.jhu.wilson.david.record.model.Record;

/**
 * {@link FieldFilter} implementation which removes {@link Field}s which do not
 * have visibilities in the specified set
 * 
 */
public class VisibilityFieldFilter implements FieldFilter {

	private final Set<String> visibilities;

	/**
	 * @param visibilities
	 *            - Visibilities to use in {@link #filterFields(Record)}
	 */
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
