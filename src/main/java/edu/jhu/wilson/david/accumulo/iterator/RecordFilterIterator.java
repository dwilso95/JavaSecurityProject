package edu.jhu.wilson.david.accumulo.iterator;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import edu.jhu.wilson.david.record.model.Field;
import edu.jhu.wilson.david.record.model.Record;
import edu.jhu.wilson.david.record.operations.VisibilityFieldFilter;
import edu.jhu.wilson.david.record.serialization.RecordJSONSerializer;

/**
 * An iterator for filtering {@link Field}s in a {@link Record} before it is
 * returned to the client.
 */
public class RecordFilterIterator extends WrappingIterator {

	private static final Gson gson = new GsonBuilder().registerTypeAdapter(Record.class, new RecordJSONSerializer())
			.create();
	private Value topValue;
	private boolean currentTopFiltered = false;

	private VisibilityFieldFilter filter;

	@Override
	public Value getTopValue() {
		topValue = getSource().getTopValue();
		if (!currentTopFiltered) {
			filterTopValue();
		}
		return topValue;
	}

	@Override
	public void next() throws IOException {
		getSource().next();
		currentTopFiltered = false;
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
			throws IOException {

		setSource(source);
		final Set<String> authSet = Sets.newHashSet();
		for (byte[] authBytes : env.getAuthorizations().getAuthorizations()) {
			authSet.add(new String(authBytes));
		}
		filter = new VisibilityFieldFilter(authSet);
	}

	/**
	 * Filters the current "top" value
	 */
	private void filterTopValue() {
		currentTopFiltered = true;
		if (topValue != null && topValue.get() != null && filter != null) {
			final Record record = gson.fromJson(new String(topValue.get()), Record.class);
			topValue = new Value(gson.toJson(filter.filterFields(record)).getBytes());
		}
	}

}
