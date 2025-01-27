package com.orientechnologies.orient.test.database.base;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;

public abstract class OrientMonoThreadDBTest extends OrientMonoThreadTest {
  protected OrientDB context;
  protected ODatabaseSession database;
  protected OURLConnection conn;

  public OrientMonoThreadDBTest(int iCycles) {
    super(iCycles);
  }

  @Override
  public void init() {
    String url = System.getProperty("url");
    conn = OURLHelper.parse(url);
    context =
        new OrientDB(
            conn.getType() + ":" + conn.getPath(), "root", "root", OrientDBConfig.defaultConfig());
    database = context.open(conn.getDbName(), "admin", "admin");
  }

  public void dropAndCreate() {
    String dbName = conn.getDbName();
    if (context.exists(dbName)) {
      context.drop(dbName);
    }

    context.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", dbName);

    database = context.open(conn.getDbName(), "admin", "admin");
  }

  @Override
  public void deinit() {
    if (database != null) database.close();
    context.close();
    super.deinit();
  }
}
