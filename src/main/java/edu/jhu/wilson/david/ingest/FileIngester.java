package edu.jhu.wilson.david.ingest;

import java.util.List;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.beust.jcommander.internal.Lists;

import edu.jhu.wilson.david.record.model.Field;
import edu.jhu.wilson.david.record.model.Record;
import edu.jhu.wilson.david.record.serialization.RecordReader;

/**
 * Class to handle reading a file and writing its contents into an Accumulo
 * instance using a provided {@link Connector} and {@link RecordReader}
 * 
 *
 */
public class FileIngester {
	private final Connector connector;

	/**
	 * Most basic constructor
	 * 
	 * @param connector
	 */
	public FileIngester(final Connector connector) {
		this.connector = connector;
	}

	/**
	 * Writes {@link Record}s provided by the {@link RecordReader} to the
	 * specified table
	 * 
	 * @param tableName
	 *            - table for which to write {@link Record}s
	 * @param recordReader
	 *            - {@link RecordReader} from which to read {@link Record}s
	 */
	public final void ingestToTable(final String tableName, final RecordReader recordReader) {
		final BatchWriter batchWriter = getBatchWriter(tableName);
		try {
			while (recordReader.hasNext()) {
				final Iterable<Mutation> mutations = buildMutations(recordReader.next());
				batchWriter.addMutations(mutations);
			}
		} catch (MutationsRejectedException e) {
			throw new RuntimeException("Mutations rejected and could not be written");
		}
	}

	/**
	 * Internal method for building {@link Mutation}s from {@link Record}s
	 * 
	 * @param record
	 *            - {@link Record} for which to generate mutations
	 * @return {@link Mutation}s for Accumulo table
	 */
	private Iterable<Mutation> buildMutations(final Record record) {
		final List<Mutation> mutations = Lists.newArrayList();
		for (final Field field : record.getFields()) {
			final Mutation mutation = new Mutation(field.getValue());
			mutation.put(field.getName(), "", new ColumnVisibility(field.getVisibility()), "");
			mutations.add(mutation);
		}

		return mutations;
	}

	/**
	 * Helper method to create a {@link BatchWriter} for the given table
	 * 
	 * @param tableName
	 *            - table for which {@link BatchWriter} will write
	 * @return - {@link BatchWriter}
	 */
	private BatchWriter getBatchWriter(final String tableName) {
		try {
			return connector.createBatchWriter(tableName, new BatchWriterConfig());
		} catch (TableNotFoundException e) {
			throw new RuntimeException("Table not found: " + tableName);
		}
	}
}
