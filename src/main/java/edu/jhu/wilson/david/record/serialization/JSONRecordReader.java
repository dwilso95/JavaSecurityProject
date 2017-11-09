package edu.jhu.wilson.david.record.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.InstanceCreator;
import com.google.gson.stream.JsonReader;

import edu.jhu.wilson.david.record.Field;
import edu.jhu.wilson.david.record.Record;
import edu.jhu.wilson.david.record.StandardField;
import edu.jhu.wilson.david.record.StandardRecord;

public class JSONRecordReader implements RecordReader, AutoCloseable {

	final Gson gson;
	final JsonReader reader;

	public JSONRecordReader(final InputStream inputStream) {
		try {
			this.reader = new JsonReader(new InputStreamReader(inputStream, "UTF-8"));
			this.gson = new GsonBuilder().registerTypeAdapter(Field.class, new FieldInstanceCreater()).create();
			reader.beginArray();
		} catch (IOException e) {
			throw new RuntimeException("Problem reading JSON input");
		}
	}

	public boolean hasNext() {
		try {
			return reader.hasNext();
		} catch (IOException e) {
			throw new RuntimeException("Problem reading JSON input");
		}
	}

	public Record next() {
		return gson.fromJson(reader, StandardRecord.class);
	}

	@Override
	public void close() {
		try {
			reader.close();
		} catch (IOException e) {
		}
	}

	class FieldInstanceCreater implements InstanceCreator<Field> {

		@Override
		public Field createInstance(Type arg0) {
			return new StandardField("", "", "");
		}

	}

}
