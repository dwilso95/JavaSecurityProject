package edu.jhu.wilson.david.accumulo.record.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import edu.jhu.wilson.david.record.model.Field;
import edu.jhu.wilson.david.record.model.Record;
import edu.jhu.wilson.david.record.serialization.JSONRecordReader;
import edu.jhu.wilson.david.record.serialization.RecordJSONSerializer;

public class JSONRecordReaderTest {

	@Test
	public void testReadFile() throws IOException {

		try (final JSONRecordReader reader = new JSONRecordReader(
				new FileInputStream(new File("src/test/resources/record.json")));) {
			while (reader.hasNext()) {
				Record record = reader.next();
				for (Field f : record.getFields()) {
					assertNotNull(f.getName());
					assertNotNull(f.getValue());
					assertNotNull(f.getVisibility());
				}
				assertEquals("Record should have only 3 fields", 3, Iterables.size(record.getFields()));
			}
		}

	}

	@Test
	public void testSerializeDeserializeRoundtripDoesNotFail() throws Exception {
		try (final JSONRecordReader reader = new JSONRecordReader(
				new FileInputStream(new File("src/test/resources/record.json")));) {
			Gson gson = new GsonBuilder().registerTypeAdapter(Record.class, new RecordJSONSerializer()).create();
			gson.fromJson(gson.toJson(reader.next()), Record.class);
		}
	}

}
