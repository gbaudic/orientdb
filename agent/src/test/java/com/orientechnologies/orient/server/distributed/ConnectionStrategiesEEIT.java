package com.orientechnologies.orient.server.distributed;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CLIENT_CONNECTION_STRATEGY;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.client.remote.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ConnectionStrategiesEEIT {
  private static final OLogger logger =
      OLogManager.instance().logger(ConnectionStrategiesEEIT.class);

  private OServer server0;
  private OServer server1;
  private OServer server2;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    remote.execute(
        "create database `"
            + ConnectionStrategiesEEIT.class.getSimpleName()
            + "` plocal users(admin identified by 'admin' role admin, reader identified by 'reader'"
            + " role reader, writer identified by 'writer' role writer)");
    remote.close();
    waitForDbOnlineStatus(ConnectionStrategiesEEIT.class.getSimpleName());
  }

  private void waitForDbOnlineStatus(String dbName) throws InterruptedException {
    server0
        .getDistributedManager()
        .waitUntilNodeOnline(server0.getDistributedManager().getLocalNodeName(), dbName);
    server1
        .getDistributedManager()
        .waitUntilNodeOnline(server1.getDistributedManager().getLocalNodeName(), dbName);
    server2
        .getDistributedManager()
        .waitUntilNodeOnline(server2.getDistributedManager().getLocalNodeName(), dbName);
  }

  @Ignore
  @Test
  public void testRoundRobinShutdownWrite()
      throws InterruptedException,
          ClassNotFoundException,
          InstantiationException,
          IllegalAccessException,
          IOException {
    OrientDB remote1 =
        new OrientDB(
            "remote:localhost;localhost:2425;localhost:2426",
            "root",
            "root",
            OrientDBConfig.builder()
                .addConfig(CLIENT_CONNECTION_STRATEGY, "ROUND_ROBIN_CONNECT")
                .build());

    Set<String> urls = new HashSet<>();
    ODatabaseSession session =
        remote1.open(ConnectionStrategiesEEIT.class.getSimpleName(), "admin", "admin");
    urls.add(((ODatabaseDocumentRemote) session).getSessionMetadata().getDebugLastHost());
    session.close();

    ODatabaseSession session1 =
        remote1.open(ConnectionStrategiesEEIT.class.getSimpleName(), "admin", "admin");
    urls.add(((ODatabaseDocumentRemote) session1).getSessionMetadata().getDebugLastHost());
    session1.close();
    ODatabaseSession session3 =
        remote1.open(ConnectionStrategiesEEIT.class.getSimpleName(), "admin", "admin");
    urls.add(((ODatabaseDocumentRemote) session3).getSessionMetadata().getDebugLastHost());
    session3.close();

    assertEquals(urls.stream().filter((x) -> x.contains("2424")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2425")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2426")).count(), 1);

    server1.shutdown();
    server1.waitForShutdown();
    urls.clear();

    for (int i = 0; i < 10; i++) {
      ODatabaseSession session2 =
          remote1.open(ConnectionStrategiesEEIT.class.getSimpleName(), "admin", "admin");
      urls.add(((ODatabaseDocumentRemote) session2).getSessionMetadata().getDebugLastHost());
      for (int ji = 0; ji < 100; ji++) {
        session2.save(session2.newVertex());
      }
      session2.close();
    }

    assertEquals(urls.stream().filter((x) -> x.contains("2424")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2426")).count(), 1);

    server1.startup(
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("orientdb-simple-dserver-config-1.xml"));
    server1.activate();
    server1.getDistributedManager().waitUntilNodeOnline();
    server1
        .getDistributedManager()
        .waitUntilNodeOnline(
            server1.getDistributedManager().getLocalNodeName(),
            ConnectionStrategiesEEIT.class.getSimpleName());

    for (int i = 0; i < 10; i++) {
      ODatabaseSession session2 =
          remote1.open(ConnectionStrategiesEEIT.class.getSimpleName(), "admin", "admin");
      try (OResultSet res = session2.query("select count(*) as count from V")) {
        assertEquals((long) res.next().getProperty("count"), 1000l);
      }
      urls.add(((ODatabaseDocumentRemote) session2).getSessionMetadata().getDebugLastHost());
      session2.close();
    }

    assertEquals(urls.stream().filter((x) -> x.contains("2424")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2425")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2426")).count(), 1);
    remote1.close();
  }

  @Ignore
  @Test
  public void testRoundRobinShutdownWriteRestartWithoutWait()
      throws InterruptedException,
          ClassNotFoundException,
          InstantiationException,
          IllegalAccessException,
          IOException {
    OrientDB remote1 =
        new OrientDB(
            "remote:localhost;localhost:2425;localhost:2426",
            "root",
            "root",
            OrientDBConfig.builder()
                .addConfig(CLIENT_CONNECTION_STRATEGY, "ROUND_ROBIN_CONNECT")
                .build());
    Set<String> urls = new HashSet<>();
    ODatabaseSession session =
        remote1.open(ConnectionStrategiesEEIT.class.getSimpleName(), "admin", "admin");
    urls.add(((ODatabaseDocumentRemote) session).getSessionMetadata().getDebugLastHost());
    session.close();

    long CYCLES = 10l;

    long V_PER_CYCLE = 100L;

    for (int i = 0; i < CYCLES; i++) {
      ODatabaseSession session2 =
          remote1.open(ConnectionStrategiesEEIT.class.getSimpleName(), "admin", "admin");
      urls.add(((ODatabaseDocumentRemote) session2).getSessionMetadata().getDebugLastHost());
      for (int ji = 0; ji < V_PER_CYCLE; ji++) {
        for (int retry = 0; retry < 10; retry++) {
          try {
            session2.save(session2.newVertex());
            break;
          } catch (ONeedRetryException ex) {

          }
        }
      }
      session2.close();
    }

    ODatabaseSession session1 =
        remote1.open(ConnectionStrategiesEEIT.class.getSimpleName(), "admin", "admin");
    urls.add(((ODatabaseDocumentRemote) session1).getSessionMetadata().getDebugLastHost());
    session1.close();

    assertEquals(urls.stream().filter((x) -> x.contains("2424")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2425")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2426")).count(), 1);

    server1.shutdown();
    server1.waitForShutdown();
    urls.clear();

    for (int i = 0; i < CYCLES; i++) {
      ODatabaseSession session2 =
          remote1.open(ConnectionStrategiesEEIT.class.getSimpleName(), "admin", "admin");
      urls.add(((ODatabaseDocumentRemote) session2).getSessionMetadata().getDebugLastHost());
      for (int ji = 0; ji < V_PER_CYCLE; ji++) {
        for (int retry = 0; retry < 10; retry++) {
          try {
            session2.save(session2.newVertex());
            break;
          } catch (ONeedRetryException ex) {

          }
        }
      }
      session2.close();
    }

    assertEquals(urls.stream().filter((x) -> x.contains("2424")).count(), 1);

    Thread start =
        new Thread(
            () -> {
              try {
                server1.startup(
                    Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream("orientdb-simple-dserver-config-1.xml"));
                server1.activate();
              } catch (Exception e) {
                e.printStackTrace();
              }
            });
    start.setDaemon(true);
    start.start();

    Thread.sleep(20000);

    for (int i = 0; i < 1000; i++) {
      logger.error("phase 3 opening session %d", null, i);
      ODatabaseSession session2 =
          remote1.open(ConnectionStrategiesEEIT.class.getSimpleName(), "admin", "admin");
      try (OResultSet res = session2.query("select count(*) as count from V")) {
        assertEquals(CYCLES * V_PER_CYCLE * 2, (long) res.next().getProperty("count"));
      }
      urls.add(((ODatabaseDocumentRemote) session2).getSessionMetadata().getDebugLastHost());
      session2.close();
    }

    assertEquals(urls.stream().filter((x) -> x.contains("2424")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2425")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2426")).count(), 1);
    remote1.close();
  }

  @After
  public void after() throws InterruptedException {
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    remote.drop(ConnectionStrategiesEEIT.class.getSimpleName());
    remote.close();

    server0.shutdown();
    server1.shutdown();
    server2.shutdown();
  }
}
