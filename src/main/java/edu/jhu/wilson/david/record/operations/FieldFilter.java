package edu.jhu.wilson.david.record.operations;

import edu.jhu.wilson.david.record.Record;

public interface FieldFilter {

	Record filterFields(final Record record);

}
