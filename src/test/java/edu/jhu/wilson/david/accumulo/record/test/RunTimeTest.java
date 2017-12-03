package edu.jhu.wilson.david.accumulo.record.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import edu.jhu.wilson.david.accumulo.AccumuloMiniClusterFactory;
import edu.jhu.wilson.david.accumulo.iterator.RecordFilterIterator;
import edu.jhu.wilson.david.ingest.RecordReaderIngester;
import edu.jhu.wilson.david.record.model.Record;
import edu.jhu.wilson.david.record.operations.KeyValueSearch;
import edu.jhu.wilson.david.record.operations.VisibilityFieldFilter;
import edu.jhu.wilson.david.record.serialization.JSONRecordReader;
import edu.jhu.wilson.david.record.serialization.RecordJSONSerializer;
import edu.jhu.wilson.david.record.serialization.RecordReader;

@RunWith(Parameterized.class)
public class RunTimeTest {

	private static final String TABLE_NAME = "someTable";
	private static Map<String, Long> runtimesPerNonAccumuloTest1 = Maps.newTreeMap();
	private static Map<String, Long> runtimesPerAccumuloTest1 = Maps.newTreeMap();
	private static Map<String, Long> runtimesPerNonAccumuloTest2 = Maps.newTreeMap();
	private static Map<String, Long> runtimesPerAccumuloTest2 = Maps.newTreeMap();
	private static Map<String, Long> runtimesPerNonAccumuloTest3 = Maps.newTreeMap();
	private static Map<String, Long> runtimesPerAccumuloTest3 = Maps.newTreeMap();

	private static MiniAccumuloCluster ACCUMULO;
	private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Record.class, new RecordJSONSerializer())
			.create();
	private Connector connector;
	private UseCase useCase;

	public RunTimeTest(UseCase useCase) {
		this.useCase = useCase;
	}

	@BeforeClass
	public static void setupClass() throws Exception {
		ACCUMULO = AccumuloMiniClusterFactory.createAccumulo("test", "pass");
	}

	@AfterClass
	public static void cleanupClass() throws IOException, InterruptedException {
		ACCUMULO.stop();
		System.out.println("\nNon-Accumulo 1");
		for (Entry<String, Long> entry : runtimesPerNonAccumuloTest1.entrySet()) {
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
		System.out.println("\nAccumulo 1");
		for (Entry<String, Long> entry : runtimesPerAccumuloTest1.entrySet()) {
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
		System.out.println("\nNon-Accumulo 2");
		for (Entry<String, Long> entry : runtimesPerNonAccumuloTest2.entrySet()) {
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
		System.out.println("\nAccumulo 2");
		for (Entry<String, Long> entry : runtimesPerAccumuloTest2.entrySet()) {
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
		System.out.println("\nNon-Accumulo 3");
		for (Entry<String, Long> entry : runtimesPerNonAccumuloTest3.entrySet()) {
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
		System.out.println("\nAccumulo 3");
		for (Entry<String, Long> entry : runtimesPerAccumuloTest3.entrySet()) {
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
	}

	@Before
	public void setupTest() throws Exception {
		connector = ACCUMULO.getConnector("root", "pass");
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
	public static Collection<UseCase> parameters() throws Exception {

//		final List<UseCase> useCases = generateUseCases(new File("src/test/resources/testFiles/usecase1"), "Use Case 1",
//				Sets.newHashSet(), true);
//
//		useCases.addAll(generateUseCases(new File("src/test/resources/testFiles/usecase2"), "Use Case 2",
//				Sets.newHashSet("XXXX"), false));

		List<UseCase> useCases = generateUseCases(new File("src/test/resources/testFiles/usecase3"), "Use Case 3",
				Sets.newHashSet("XXXX", "YYYY"), false);

		return useCases;
	}

	private static List<UseCase> generateUseCases(final File directory, final String useCaseName,
			final Set<String> authorizationStrings, final boolean overrideExpectedToZero) {

		final List<UseCase> useCases = Lists.newArrayList();

		final Pattern p = Pattern.compile("[0-9][0-9]");

		for (final File file : directory.listFiles()) {
			final Matcher m = p.matcher(file.getName());
			m.find();

			final UseCase useCase = new UseCase();
			useCase.setFile(file);
			useCase.setAuthorizationStrings(authorizationStrings);
			useCase.setName(useCaseName);
			if (overrideExpectedToZero) {
				useCase.setExpectedResults(0);
			} else {
				useCase.setExpectedResults(Integer.parseInt(m.group(0)));
			}
			useCases.add(useCase);
		}

		return useCases;
	}

	// @Test
	public void testParameters() {
		System.out.println(this.useCase.getFile().getAbsolutePath());
	}

	@Test
	public void runTestAccumulo() throws Exception {
		this.connector.tableOperations().create(TABLE_NAME);
		final RecordReaderIngester ingester = new RecordReaderIngester(this.connector);
		final RecordReader recordReader = new JSONRecordReader(new FileInputStream(this.useCase.getFile()));
		ingester.ingestToTable(TABLE_NAME, recordReader);

		final IteratorSetting iteratorSetting = new IteratorSetting(1, RecordFilterIterator.class,
				Collections.emptyMap());
		final Authorizations authorizations = new Authorizations(
				useCase.getAuthorizationStrings().toArray(new String[useCase.getAuthorizationStrings().size()]));
		this.connector.securityOperations().changeUserAuthorizations("root", authorizations);

		final List<String> jsonResults = Lists.newArrayList();
		final long start = System.nanoTime();
		final Scanner scanner = this.connector.createScanner(TABLE_NAME, authorizations);
		scanner.addScanIterator(iteratorSetting);
		scanner.setRange(Range.exact("John", "Name"));

		for (final Map.Entry<Key, Value> scanResult : scanner) {
			jsonResults.add(scanResult.getValue().toString());
		}

		assertEquals(this.useCase.getExpectedResults(), jsonResults.size());
		final long timeElapsed = System.nanoTime() - start;

		switch (useCase.getName()) {
		case "Use Case 1":
			runtimesPerAccumuloTest1.put(this.useCase.getFile().getName(), timeElapsed);
			break;
		case "Use Case 2":
			runtimesPerAccumuloTest2.put(this.useCase.getFile().getName(), timeElapsed);
			break;
		case "Use Case 3":
			runtimesPerAccumuloTest3.put(this.useCase.getFile().getName(), timeElapsed);
			break;
		}

	}

	@Test
	public void runTestNonAccumulo() throws Exception {
		this.connector.tableOperations().create(TABLE_NAME);
		final RecordReaderIngester ingester = new RecordReaderIngester(this.connector);
		final RecordReader recordReader = new JSONRecordReader(new FileInputStream(this.useCase.getFile()));
		ingester.ingestToTable(TABLE_NAME, recordReader);
		final Authorizations authorizations = new Authorizations("XXXX", "YYYY");
		this.connector.securityOperations().changeUserAuthorizations("root", authorizations);

		final List<String> jsonResults = Lists.newArrayList();
		final long start = System.nanoTime();
		final Scanner scanner = this.connector.createScanner(TABLE_NAME, authorizations);
		scanner.setRange(Range.exact("John", "Name"));
		final KeyValueSearch searcher = new KeyValueSearch(
				Lists.newArrayList(new VisibilityFieldFilter(useCase.getAuthorizationStrings())),
				Collections.emptyList());

		// outside accumulo filtering
		for (final Map.Entry<Key, Value> scanResult : scanner) {
			final Record record = GSON.fromJson(new String(scanResult.getValue().get()), Record.class);
			if (searcher.containsFieldValuePair("Name", "John", record)) {
				jsonResults.add(GSON.toJson(record));
			}
		}

		assertEquals(this.useCase.getExpectedResults(), jsonResults.size());

		final long timeElapsed = System.nanoTime() - start;

		switch (useCase.getName()) {
		case "Use Case 1":
			runtimesPerNonAccumuloTest1.put(this.useCase.getFile().getName(), timeElapsed);
			break;
		case "Use Case 2":
			runtimesPerNonAccumuloTest2.put(this.useCase.getFile().getName(), timeElapsed);
			break;
		case "Use Case 3":
			runtimesPerNonAccumuloTest3.put(this.useCase.getFile().getName(), timeElapsed);
			break;
		}

	}

	@SuppressWarnings("unused")
	private void printScannerResults(final Scanner scanner) {
		for (final Map.Entry<Key, Value> scanResult : scanner) {
			System.out.println(scanResult.getKey().getRow() + " " + scanResult.getKey().getColumnFamily() + " "
					+ scanResult.getKey().getColumnQualifier());
			System.out.println(new String(scanResult.getValue().get()));
		}

	}

}
