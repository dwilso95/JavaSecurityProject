package edu.jhu.wilson.david.record.serialization;

import edu.jhu.wilson.david.record.Record;

public interface RecordReader {

	boolean hasNext();

	Record next();

}
