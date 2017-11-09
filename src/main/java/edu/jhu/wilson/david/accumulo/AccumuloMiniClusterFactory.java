package edu.jhu.wilson.david.accumulo;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;

import com.google.common.io.Files;

/**
 * Simple factory for creating {@link MiniAccumuloCluster} instances
 * 
 */
public class AccumuloMiniClusterFactory {

	/**
	 * Private on purpose so this factory can be treated as a 'static class'
	 */
	private AccumuloMiniClusterFactory() {
		// do nothing
	}

	/**
	 * Creates a {@link MiniAccumuloCluster} instance using the provided
	 * instance name and password. The instance uses a random directory
	 * generated from {@link Files#createTempDir()}.
	 * 
	 * Use the default username 'root' to get a connector to the created
	 * instance
	 * 
	 * @param instanceName
	 *            - Name of instance to create
	 * @param password
	 *            - password to use for instance
	 * @return a new {@link MiniAccumuloCluster}
	 */
	public static MiniAccumuloCluster createAccumulo(String instanceName, String password) {
		final File tempDir = Files.createTempDir();

		final MiniAccumuloConfig config = new MiniAccumuloConfig(tempDir, password).setNumTservers(2)
				.setInstanceName(instanceName);

		try {
			final MiniAccumuloCluster accumulo = new MiniAccumuloCluster(config);
			accumulo.start();
			return accumulo;
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException("Unable to build accumulo instance");
		}
	}

}
