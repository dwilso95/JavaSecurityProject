package edu.jhu.wilson.david.accumulo.ingest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.jhu.wilson.david.accumulo.AccumuloMiniClusterFactory;
import edu.jhu.wilson.david.ingest.RecordReaderIngester;
import edu.jhu.wilson.david.record.serialization.JSONRecordReader;
import edu.jhu.wilson.david.record.serialization.RecordReader;

public class RecordReaderIngestTest {

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
	public void testIngestSimpleJsonFile() throws Exception {
		final String tableName = "someTable";
		connector.tableOperations().create(tableName);
		final RecordReaderIngester ingester = new RecordReaderIngester(connector);
		final RecordReader recordReader = new JSONRecordReader(
				new FileInputStream(new File("src/test/resources/record.json")));
		ingester.ingestToTable(tableName, recordReader);

		int count = 0;
		for (Map.Entry<Key, Value> scanResult : connector.createScanner(tableName,
				new Authorizations("YYYY", "XXXX"))) {
			count++;
			assertNotNull(scanResult.getKey().getRow());
		}

		assertEquals("there should be three records", 3, count);
	}

}
