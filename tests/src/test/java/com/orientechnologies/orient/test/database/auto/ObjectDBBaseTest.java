package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import java.io.IOException;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/3/14
 */
@Test
public class ObjectDBBaseTest extends BaseTest<ODatabaseObject> {
  private static final OLogger logger = OLogManager.instance().logger(ObjectDBBaseTest.class);

  public ObjectDBBaseTest() {}

  @Parameters(value = "url")
  public ObjectDBBaseTest(@Optional String url) {
    super(url);
  }

  @Parameters(value = "url")
  public ObjectDBBaseTest(@Optional String url, String prefix) {
    super(url, prefix);
  }

  @Override
  protected OObjectDatabaseTx createDatabaseInstance(String url) {
    return new OObjectDatabaseTx(url);
  }

  protected ODatabaseSession rawSession(String user, String password) {
    ODatabaseDocumentTx session = new ODatabaseDocumentTx(this.url);
    session.open(user, password);
    return session;
  }

  protected ODatabaseSession rawSession(String suffix, String user, String password) {
    ODatabaseDocumentTx session = new ODatabaseDocumentTx(this.url + suffix);
    session.open(user, password);
    return session;
  }

  protected ODatabaseObject session(String suffix, String user, String password) {
    OObjectDatabaseTx session = new OObjectDatabaseTx(this.url + suffix);
    session.open(user, password);
    return session;
  }

  protected void dropAndCreateDatabase(String suffix) throws IOException {
    ODatabaseObject database = new OObjectDatabaseTx(url + suffix);
    if (ODatabaseHelper.existsDatabase(url + suffix))
      ODatabaseHelper.dropDatabase(database, getStorageType());
    ODatabaseHelper.createDatabase(database, url + suffix, getStorageType());
  }

  protected void reopendb(String user, String password) {
    if (database == null || !database.isClosed() && !database.isActiveOnCurrentThread()) {
      database = new OObjectDatabaseTx(this.url);
    }
    database.open(user, password);
  }

  protected void reopenpool(String user, String password) {
    if (!database.isClosed()) {
      database.close();
    }
    database = new OObjectDatabaseTx(this.url);
    database.open(user, password);
  }

  protected ODatabaseObject openpool(String user, String password) {
    return session("", user, password);
  }

  protected void dropdb() throws IOException {
    String prefix = url.substring(0, url.indexOf(':') + 1);
    logger.info("deleting database %s", url);
    ODatabaseHelper.dropDatabase(session("", "admin", "admin"), prefix);
  }
}
