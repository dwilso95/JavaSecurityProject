package edu.jhu.wilson.david.accumulo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.jhu.wilson.david.accumulo.iterator.RecordFilterIterator;
import edu.jhu.wilson.david.ingest.RecordReaderIngester;
import edu.jhu.wilson.david.record.serialization.JSONRecordReader;
import edu.jhu.wilson.david.record.serialization.RecordReader;

public class Demo {

	private static MiniAccumuloCluster ACCUMULO;
	Connector connector;

	@BeforeClass
	public static void setupClass() {
		ACCUMULO = AccumuloMiniClusterFactory.createAccumulo("test", "pass");
	}

	@AfterClass
	public static void cleanupClass() throws IOException, InterruptedException {
		ACCUMULO.stop();
	}

	@Before
	public void setupTest() throws AccumuloException, AccumuloSecurityException {
		connector = ACCUMULO.getConnector("root", "pass");
		connector.securityOperations().changeUserAuthorizations("root", new Authorizations("XXXX", "YYYY", "ZZZZ"));
	}

	@Test
	public void demo() throws Exception {
		final String tableName = "someTable";
		connector.tableOperations().create(tableName);
		final RecordReaderIngester ingester = new RecordReaderIngester(connector);
		final RecordReader recordReader = new JSONRecordReader(
				new FileInputStream(new File("src/test/resources/record.json")));
		ingester.ingestToTable(tableName, recordReader);

		Scanner scanner = connector.createScanner(tableName, new Authorizations("YYYY", "XXXX"));
		scanner.setRange(Range.exact("John", "Name"));
		for (final Map.Entry<Key, Value> scanResult : scanner) {
			System.out.println(scanResult.getKey().getRow() + " " + scanResult.getKey().getColumnFamily() + " "
					+ scanResult.getKey().getColumnQualifier());
			System.out.println(new String(scanResult.getValue().get()));
		}

		IteratorSetting iteratorSetting = new IteratorSetting(1, RecordFilterIterator.class, Collections.emptyMap());

		scanner = connector.createScanner(tableName, new Authorizations("XXXX"));
		scanner.addScanIterator(iteratorSetting);

		scanner.setRange(Range.exact("John", "Name"));
		for (final Map.Entry<Key, Value> scanResult : scanner) {
			System.out.println(scanResult.getKey().getRow() + " " + scanResult.getKey().getColumnFamily() + " "
					+ scanResult.getKey().getColumnQualifier());
			System.out.println(new String(scanResult.getValue().get()));
		}

	}

}
