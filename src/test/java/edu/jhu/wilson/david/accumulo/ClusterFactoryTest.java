package edu.jhu.wilson.david.accumulo;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.Test;

public class ClusterFactoryTest {

	@Test
	public void testClusterFactory() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		MiniAccumuloCluster cluster = AccumuloMiniClusterFactory.createCluster("test", "pass".getBytes());
		Connector connector = cluster.getConnector("test", "pass");
		assertEquals("test", cluster.getInstanceName());
		assertEquals("test", connector.getInstance().getInstanceName());
	}

}
