package edu.jhu.wilson.david.record.serialization;

import java.lang.reflect.Type;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import edu.jhu.wilson.david.record.model.Field;
import edu.jhu.wilson.david.record.model.Record;
import edu.jhu.wilson.david.record.model.StandardField;
import edu.jhu.wilson.david.record.model.StandardRecord;

public class RecordJSONSerializer implements JsonDeserializer<Record>, JsonSerializer<Record> {

	@Override
	public Record deserialize(JsonElement element, Type type, JsonDeserializationContext context)
			throws JsonParseException {

		if (!element.isJsonObject()) {
			throw new RuntimeException("Cannot parse JSON Record");
		}

		final Record record = new StandardRecord();
		final JsonElement fieldArray = element.getAsJsonObject().get("fields").getAsJsonArray();
		if (fieldArray.isJsonArray()) {
			for (JsonElement arrayElement : fieldArray.getAsJsonArray()) {
				if (arrayElement.isJsonObject()) {
					record.addField(deserializeField(arrayElement.getAsJsonObject()));
				} else {
					throw new RuntimeException("Cannot parse JSON Field");
				}
			}
		}

		return record;

	}

	private Field deserializeField(final JsonObject object) {
		return new StandardField(object.get("name").getAsString(), object.get("value").getAsString(),
				object.get("visibility").getAsString());
	}

	@Override
	public JsonElement serialize(Record record, Type type, JsonSerializationContext contexto) {

		final JsonObject recordObject = new JsonObject();
		final JsonArray fieldsArray = new JsonArray();

		for (final Field field : record.getFields()) {
			final JsonObject fieldObject = new JsonObject();
			fieldObject.addProperty("name", field.getName());
			fieldObject.addProperty("value", field.getValue());
			fieldObject.addProperty("visibility", field.getVisibility());
			fieldsArray.add(fieldObject);
		}

		recordObject.add("fields", fieldsArray);

		return recordObject;
	}

}
