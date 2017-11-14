package edu.jhu.wilson.david.accumulo.record.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import edu.jhu.wilson.david.record.model.Field;
import edu.jhu.wilson.david.record.model.Record;
import edu.jhu.wilson.david.record.model.StandardField;
import edu.jhu.wilson.david.record.model.StandardRecord;
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

	private final String[] months = { "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12" };
	private final String[] days = { "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14",
			"15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28" };

	@Ignore
	@Test
	public void generateJsonTestFiles() throws Exception {

		for (int numJohn = 0; numJohn <= 50; numJohn = numJohn + 5) {
			int total = 10000;
			int numNotJohn = total - numJohn;

			try (final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream(new File("usecase_3_records_" + numJohn + ".json"))));) {

				Gson gson = new GsonBuilder().registerTypeAdapter(Record.class, new RecordJSONSerializer()).create();
				out.write("[\n".getBytes());

				for (int i = 0; i < numJohn; i++) {
					final Record r = new StandardRecord();
					r.addField(new StandardField("Name", "John", "XXXX"));
					r.addField(new StandardField("DOB",
							ThreadLocalRandom.current().nextInt(1950, 2010) + "-"
									+ months[ThreadLocalRandom.current().nextInt(0, 11)] + "-"
									+ days[ThreadLocalRandom.current().nextInt(0, 27)],
							"XXXX"));
					r.addField(new StandardField("SSN", RandomStringUtils.randomNumeric(3) + "-"
							+ RandomStringUtils.randomNumeric(2) + "-" + RandomStringUtils.randomNumeric(4), "XXXX"));
					out.write(gson.toJson(r).getBytes());
					if (i + 1 < total) {
						out.write(",".getBytes());
					}
					out.write("\n".getBytes());
				}

				for (int i = 0; i < numNotJohn; i++) {
					final Record r = new StandardRecord();
					r.addField(new StandardField("Name",
							RandomStringUtils.randomAlphabetic(ThreadLocalRandom.current().nextInt(5, 10)), "XXXX"));
					r.addField(new StandardField("DOB",
							ThreadLocalRandom.current().nextInt(1950, 2010) + "-"
									+ months[ThreadLocalRandom.current().nextInt(0, 11)] + "-"
									+ days[ThreadLocalRandom.current().nextInt(0, 27)],
							"YYYY"));
					r.addField(new StandardField("SSN", RandomStringUtils.randomNumeric(3) + "-"
							+ RandomStringUtils.randomNumeric(2) + "-" + RandomStringUtils.randomNumeric(4), "YYYY"));
					out.write(gson.toJson(r).getBytes());
					if (i + 1 < numNotJohn) {
						out.write(",".getBytes());
					}
					out.write("\n".getBytes());
				}
				out.write("]".getBytes());
			}
		}

	}

}
