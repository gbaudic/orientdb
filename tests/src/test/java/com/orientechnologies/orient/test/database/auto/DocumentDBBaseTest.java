package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import java.io.IOException;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/3/14
 */
@Test
public abstract class DocumentDBBaseTest extends BaseTest<ODatabaseDocumentInternal> {
  protected DocumentDBBaseTest() {}

  @Parameters(value = "url")
  protected DocumentDBBaseTest(@Optional String url) {
    super(url);
  }

  @Parameters(value = "url")
  protected DocumentDBBaseTest(@Optional String url, String prefix) {
    super(url, prefix);
  }

  protected ODatabaseDocumentInternal createDatabaseInstance(String url) {
    return new ODatabaseDocumentTx(url);
  }

  protected void reopendb(String user, String password) {
    if (!database.isClosed() && !database.isActiveOnCurrentThread()) {
      database = new ODatabaseDocumentTx(this.url);
    }
    database = (ODatabaseDocumentInternal) database.open(user, password);
  }

  protected ODatabaseSession openSession(String user, String password) {
    ODatabaseSession session = new ODatabaseDocumentTx(this.url);
    session.open(user, password);
    return session;
  }

  protected ODatabaseSession openSession(String database, String user, String password) {
    ODatabaseSession session =
        new ODatabaseDocumentTx(getStorageType() + ":" + "./target/" + database);
    session.open(user, password);
    return session;
  }

  protected void dropdb() {
    try {
      ODatabaseHelper.deleteDatabase(database, getStorageType());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void dropdb(String name) {
    final ODatabaseSession db =
        new ODatabaseDocumentTx(getStorageType() + ":" + "./target/" + name);
    try {
      ODatabaseHelper.dropDatabase(db, getStorageType());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected ODatabaseSession createdb(String database) throws IOException {
    final ODatabaseSession db =
        new ODatabaseDocumentTx(getStorageType() + ":" + "./target/" + database);
    db.create();
    return db;
  }

  protected void dropAndCreateDatabase(String suffix) throws IOException {
    ODatabaseDocument database = new ODatabaseDocumentTx(url + suffix);
    ODatabaseHelper.dropDatabase(database, getStorageType());
    ODatabaseHelper.createDatabase(database, url + suffix, getStorageType());
  }

  protected boolean existsdb() {
    return database.exists();
  }

  protected static String getTestEnv() {
    return System.getProperty("orientdb.test.env");
  }
}
