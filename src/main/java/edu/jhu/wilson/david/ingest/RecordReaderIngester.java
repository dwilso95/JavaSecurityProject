package edu.jhu.wilson.david.ingest;

import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.beust.jcommander.internal.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import edu.jhu.wilson.david.record.model.Field;
import edu.jhu.wilson.david.record.model.Record;
import edu.jhu.wilson.david.record.serialization.RecordJSONSerializer;
import edu.jhu.wilson.david.record.serialization.RecordReader;

/**
 * Class to handle reading a file and writing its contents into an Accumulo
 * instance using a provided {@link Connector} and {@link RecordReader}
 */
public class RecordReaderIngester {
	private final Connector connector;
	private final Gson gson = new GsonBuilder().registerTypeAdapter(Record.class, new RecordJSONSerializer()).create();

	/**
	 * Constructor which requires an Accumulo connector in order to create a
	 * batch writer
	 * 
	 * @param connector
	 *            - {@link Connector}
	 */
	public RecordReaderIngester(final Connector connector) {
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
				batchWriter.flush();
			}
			batchWriter.close();
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
			mutation.put(field.getName(), UUID.randomUUID().toString(), new ColumnVisibility(field.getVisibility()),
					gson.toJson(record));
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
