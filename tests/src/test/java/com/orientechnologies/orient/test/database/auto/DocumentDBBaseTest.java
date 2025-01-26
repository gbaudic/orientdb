package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
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

  protected void reopendb(String user, String password) {
    database = (ODatabaseDocumentInternal) baseContext.open(data.getDbName(), user, password);
  }

  protected ODatabaseSession openSession(String user, String password) {
    return baseContext.open(data.getDbName(), user, password);
  }

  protected ODatabaseSession openSession(String database, String user, String password) {
    return baseContext.open(database, user, password);
  }

  @Override
  protected ODatabaseDocumentInternal session(String database, String user, String password) {
    return (ODatabaseDocumentInternal) baseContext.open(database, user, password);
  }

  protected void dropdb() {
    baseContext.drop(data.getDbName());
  }

  protected void dropdb(String name) {
    baseContext.drop(name);
  }

  protected ODatabaseSession createdb(String database) throws IOException {
    createBaseDb(database);
    return openSession(database, "admin", "admin");
  }

  protected boolean existsdb() {
    return baseContext.exists(data.getDbName());
  }

  protected static String getTestEnv() {
    return System.getProperty("orientdb.test.env");
  }
}
