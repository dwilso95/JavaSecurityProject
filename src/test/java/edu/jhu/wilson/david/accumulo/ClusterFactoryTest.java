package edu.jhu.wilson.david.accumulo;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.Test;

public class ClusterFactoryTest {

	@Test
	public void testClusterFactory() throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
			IOException, InterruptedException, TableExistsException {
		MiniAccumuloCluster accumulo = AccumuloMiniClusterFactory.createAccumulo("test", "pass");
		Connector connector = accumulo.getConnector("root", "pass");
		assertEquals("test", accumulo.getInstanceName());
		assertEquals("test", connector.getInstance().getInstanceName());

		accumulo.stop();
	}

}
