package edu.jhu.wilson.david.accumulo.record.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.jhu.wilson.david.accumulo.AccumuloMiniClusterFactory;
import edu.jhu.wilson.david.accumulo.iterator.RecordFilterIterator;
import edu.jhu.wilson.david.ingest.RecordReaderIngester;
import edu.jhu.wilson.david.record.serialization.JSONRecordReader;
import edu.jhu.wilson.david.record.serialization.RecordReader;

@RunWith(Parameterized.class)
public class RunTimeTest {

	private static final String TABLE_NAME = "someTable";
	private static Map<String, Long> runtimesPerTest = Maps.newHashMap();
	private static MiniAccumuloCluster ACCUMULO;
	private Connector connector;
	private File file;

	public RunTimeTest(final File file) {
		this.file = file;
	}

	@BeforeClass
	public static void setupClass() {
		ACCUMULO = AccumuloMiniClusterFactory.createAccumulo("test", "pass");

	}

	@AfterClass
	public static void cleanupClass() throws IOException, InterruptedException {
		ACCUMULO.stop();
		for (Entry<String, Long> entry : runtimesPerTest.entrySet()) {
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
	}

	@Before
	public void setupTest() throws Exception {
		connector = ACCUMULO.getConnector("root", "pass");
		connector.securityOperations().changeUserAuthorizations("root", new Authorizations("XXXX", "YYYY", "ZZZZ"));
		deleteTable();
	}

	@After
	public void cleanup() throws Exception {
		deleteTable();
	}

	private void deleteTable() throws Exception {
		if (connector.tableOperations().exists(TABLE_NAME)) {
			connector.tableOperations().delete(TABLE_NAME);
		}
	}

	@Parameters
	public static Collection<File> parameter() throws Exception {
		final List<File> files = Lists.newArrayList();

		for (final File directory : new File("src/test/resources/testFiles").listFiles()) {
			for (final File file : directory.listFiles()) {
				files.add(file);
			}
		}

		return files;
	}

	@Ignore
	@Test
	public void testParameters() {
		System.out.println(this.file.getAbsolutePath());
	}

	@Test
	public void runTest() throws Exception {
		final String tableName = TABLE_NAME;
		connector.tableOperations().create(tableName);
		final RecordReaderIngester ingester = new RecordReaderIngester(connector);
		final RecordReader recordReader = new JSONRecordReader(new FileInputStream(this.file));
		ingester.ingestToTable(tableName, recordReader);

		final long start = System.nanoTime();
		final IteratorSetting iteratorSetting = new IteratorSetting(1, RecordFilterIterator.class,
				Collections.emptyMap());
		final Scanner scanner = connector.createScanner(tableName, new Authorizations("XXXX"));
		scanner.addScanIterator(iteratorSetting);
		scanner.setRange(Range.exact("John", "Name"));
		

		for (final Map.Entry<Key, Value> scanResult : scanner) {
			System.out.println(scanResult.getKey().getRow() + " " + scanResult.getKey().getColumnFamily() + " "
					+ scanResult.getKey().getColumnQualifier());
			System.out.println(new String(scanResult.getValue().get()));
		}

		final long timeElapsed = System.nanoTime() - start;
		System.out.println(timeElapsed);

		runtimesPerTest.put(file.getName(), timeElapsed);

	}

}
