package edu.jhu.wilson.david.accumulo;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;

import com.google.common.io.Files;

public class AccumuloMiniClusterFactory {

	private AccumuloMiniClusterFactory() {
		// do nothing
	}

	public static MiniAccumuloCluster createCluster(String instanceName, byte[] password) {
		File tempDir = Files.createTempDir();

		MiniAccumuloConfig config = new MiniAccumuloConfig(tempDir, new String(password)).setNumTservers(2)
				.setInstanceName(instanceName);

		try {
			MiniAccumuloCluster accumulo = new MiniAccumuloCluster(config);
			accumulo.start();
			Instance instance = new ZooKeeperInstance(instanceName, accumulo.getZooKeepers());
			Connector conn = instance.getConnector("root", new PasswordToken(new String(password)));
			System.out.println(conn.getInstance().getInstanceName());
			return accumulo;
		} catch (IOException | InterruptedException | AccumuloException | AccumuloSecurityException e) {
			throw new RuntimeException("Unable to build accumulo instance");
		}
	}

}
