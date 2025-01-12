package com.orientechnologies.orient.test.server;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import org.testng.annotations.Test;

public class OServerTest {
  private static final OLogger logger = OLogManager.instance().logger(OServerTest.class);

  /** Test for https://github.com/orientechnologies/orientdb/issues/1667 */
  @Test
  public void testRestart() throws Exception {
    // set ORIENTDB_HOME
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    System.setProperty(
        "ORIENTDB_HOME", buildDirectory + File.separator + OServerTest.class.getSimpleName());

    logger.info("ORIENTDB_HOME: %s", System.getProperty("ORIENTDB_HOME"));

    // loop for start & stop server
    for (int i = 0; i < 5; i++) {
      logger.info("Iteration %d", i);
      OServer server = new OServer(false).activate();
      // create database if does not exist
      OrientDB ctx = server.getContext();
      if (!ctx.exists("test-db")) {
        ctx.execute(
            "create database test-db plocal users(admin identified by 'adminpwd' role admin)");
      }

      ODatabaseObject database =
          new OObjectDatabaseTx(
              (ODatabaseDocumentInternal) ctx.open("test-db", "admin", "adminpwd"));
      database.countClass("ouser");
      database.close();
      server.shutdown();
    }
  }
}
