package edu.jhu.wilson.david.accumulo.record.serialization;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.junit.Test;

import com.google.common.collect.Iterables;

import edu.jhu.wilson.david.record.serialization.JSONRecordReader;

public class JSONRecordReaderTest {

	@Test
	public void testReadFile() throws IOException {

		try (final JSONRecordReader reader = new JSONRecordReader(
				new FileInputStream(new File("src/test/resources/record.json")));) {
			while (reader.hasNext()) {
				assertEquals("Record should have only 3 fields", 3, Iterables.size(reader.next().getFields()));
			}
		}

	}

}
