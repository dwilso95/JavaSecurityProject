package edu.jhu.wilson.david.ingest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.beust.jcommander.internal.Lists;

import edu.jhu.wilson.david.record.Field;
import edu.jhu.wilson.david.record.Record;
import edu.jhu.wilson.david.record.serialization.JSONRecordReader;

public class FileIngester {
	private final Connector connector;

	public FileIngester(final Connector connector) {
		this.connector = connector;
	}

	public final void ingestFile(final String tableName, final File file) {
		final BatchWriter batchWriter = getBathWriter(tableName);
		try (final JSONRecordReader reader = new JSONRecordReader(new FileInputStream(file));) {

			while (reader.hasNext()) {
				final Iterable<Mutation> mutations = buildMutations(reader.next());
				batchWriter.addMutations(mutations);
			}

		} catch (FileNotFoundException fnfe) {
			throw new RuntimeException("File cannot be found: " + file.getPath());
		} catch (MutationsRejectedException e) {
			throw new RuntimeException("Mutations rejected and could not be written");
		}
	}

	private Iterable<Mutation> buildMutations(final Record record) {
		final List<Mutation> mutations = Lists.newArrayList();
		for (final Field field : record.getFields()) {
			final Mutation mutation = new Mutation(field.getValue());
			mutation.put(field.getName(), "", new ColumnVisibility(field.getVisibility()), "");
			mutations.add(mutation);
		}

		return mutations;
	}

	private BatchWriter getBathWriter(final String tableName) {
		try {
			return connector.createBatchWriter(tableName, new BatchWriterConfig());
		} catch (TableNotFoundException e) {
			throw new RuntimeException("Table not found: " + tableName);
		}
	}
}
