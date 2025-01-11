package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Collections;
import java.util.List;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class HAMultiDCCrudTest extends AbstractServerClusterTest {
  private static final int SERVERS = 3;

  @Override
  public String getDatabaseName() {
    return "HAMultiDCCrudTest";
  }

  @Test
  @Ignore
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onAfterDatabaseCreation(OrientGraph db) {
    db.executeSql("CREATE CLASS Item extends V").close();
    db.executeSql("CREATE PROPERTY Item.name STRING").close();
    db.executeSql("CREATE PROPERTY Item.map EMBEDDEDMAP").close();
  }

  @Override
  protected void executeTest() throws Exception {
    final ODistributedConfiguration cfg =
        serverInstance
            .get(0)
            .getServerInstance()
            .getDistributedManager()
            .getDatabaseConfiguration(getDatabaseName());

    Assert.assertTrue(cfg.hasDataCenterConfiguration());
    Assert.assertTrue(cfg.isLocalDataCenterWriteQuorum());

    Assert.assertEquals(cfg.getDataCenterOfServer("europe-0"), "rome");
    Assert.assertEquals(cfg.getDataCenterOfServer("europe-1"), "rome");
    Assert.assertEquals(cfg.getDataCenterOfServer("europe-2"), "rome");
    Assert.assertEquals(cfg.getDataCenterOfServer("usa-0"), "austin");
    Assert.assertEquals(cfg.getDataCenterOfServer("usa-1"), "austin");
    Assert.assertEquals(cfg.getDataCenterOfServer("usa-2"), "austin");

    final List<String> romeDc = cfg.getDataCenterServers("rome");
    Assert.assertEquals(romeDc.size(), 3);
    Assert.assertTrue(romeDc.contains("europe-0"));
    Assert.assertTrue(romeDc.contains("europe-1"));
    Assert.assertTrue(romeDc.contains("europe-2"));

    final List<String> austinDc = cfg.getDataCenterServers("austin");
    Assert.assertEquals(austinDc.size(), 3);
    Assert.assertTrue(austinDc.contains("usa-0"));
    Assert.assertTrue(austinDc.contains("usa-1"));
    Assert.assertTrue(austinDc.contains("usa-2"));

    OrientDB ctx = new OrientDB("remote:localhost:2424", OrientDBConfig.defaultConfig());
    OrientDB ctx1 = new OrientDB("remote:localhost:2425", OrientDBConfig.defaultConfig());
    OrientDB ctx2 = new OrientDB("remote:localhost:2426", OrientDBConfig.defaultConfig());

    ODatabaseDocument db = ctx.open(getDatabaseName(), "admin", "admin");
    try {
      db.command("INSERT into Item (name) values ('foo')").close();
    } finally {
      db.close();
    }

    db = ctx1.open(getDatabaseName(), "admin", "admin");
    try {
      OResultSet result = db.command("select set(name) as names from Item");
      Assert.assertEquals(Collections.singleton("foo"), result.next().getProperty("names"));

      result = db.command("select list(name) as names from Item");
      Assert.assertEquals(Collections.singletonList("foo"), result.next().getProperty("names"));

      db.command("INSERT into Item (map) values ({'a':'b'}) return @this").close();

      result = db.command("select map(map) as names from Item");
      Assert.assertEquals(Collections.singletonMap("a", "b"), result.next().getProperty("names"));

    } finally {
      db.close();
    }

    // TRY AN INSERT AGAINST THE DC WITHOUT QUORUM EXPECTING TO FAIL
    db = ctx2.open(getDatabaseName(), "admin", "admin");
    try {
      db.command("INSERT into Item (map) values ({'a':'b'}) return @this").close();
      Assert.fail("Quorum not reached, but no failure has been caught");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause().toString().contains("Quorum"));
    } finally {
      db.close();
    }

    // KILL ONE SERVER TO CHECK IF QUORUM FAILS
    serverInstance.get(0).getServerInstance().shutdown();

    // RETRY AN INSERT AND CHECK IT FAILS (NO QUORUM)
    db = ctx1.open(getDatabaseName(), "admin", "admin");
    try {
      db.command("INSERT into Item (map) values ({'a':'b'}) return @this").close();
      Assert.fail("Quorum not reached, but no failure has been caught");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause().toString().contains("Quorum"));
    } finally {
      db.close();
    }

    // RESTART THE FIRST SERVER DOWN
    serverInstance.get(0).getServerInstance().restart();

    waitForDatabaseIsOnline(
        serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName(),
        getDatabaseName(),
        30000);

    // RETRY AN INSERT AND CHECK IT DOESN'T FAILS (QUORUM REACHED)
    db = ctx1.open(getDatabaseName(), "admin", "admin");
    try {
      db.command("INSERT into Item (map) values ({'a':'b'}) return @this").close();
    } finally {
      db.close();
    }
    ctx.close();
    ctx1.close();
    ctx2.close();
  }

  @Override
  protected String getDistributedServerConfiguration(ServerRun server) {
    return "dc-server-config-" + server.getServerId() + ".xml";
  }
}
