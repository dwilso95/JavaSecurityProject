package edu.jhu.wilson.david.record.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.InstanceCreator;
import com.google.gson.stream.JsonReader;

import edu.jhu.wilson.david.record.model.Field;
import edu.jhu.wilson.david.record.model.Record;
import edu.jhu.wilson.david.record.model.impl.StandardField;
import edu.jhu.wilson.david.record.model.impl.StandardRecord;

/**
 * Reads {@link Record} serialized as JSON.
 */
public class JSONRecordReader implements RecordReader {

	final Gson gson;
	final JsonReader reader;

	/**
	 * Specifies {@link InputStream} from which to read JSON serialized
	 * {@link Record}s
	 * 
	 * @param inputStream
	 */
	public JSONRecordReader(final InputStream inputStream) {
		try {
			this.reader = new JsonReader(new InputStreamReader(inputStream, "UTF-8"));
			this.gson = new GsonBuilder().registerTypeAdapter(Field.class, new FieldInstanceCreater()).create();
			reader.beginArray();
		} catch (IOException e) {
			throw new RuntimeException("Problem reading JSON input");
		}
	}

	@Override
	public boolean hasNext() {
		try {
			return reader.hasNext();
		} catch (IOException e) {
			throw new RuntimeException("Problem reading JSON input");
		}
	}

	@Override
	public Record next() {
		return gson.fromJson(reader, StandardRecord.class);
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	class FieldInstanceCreater implements InstanceCreator<Field> {

		@Override
		public Field createInstance(Type type) {
			return new StandardField("", "", "");
		}

	}

}
