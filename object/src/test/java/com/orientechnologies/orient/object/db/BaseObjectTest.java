package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import org.junit.After;
import org.junit.Before;

public class BaseObjectTest {

  protected ODatabaseObject database;
  protected OrientDBObject ctx;

  @Before
  public void before() {
    ctx = new OrientDBObject("memory:", OrientDBConfig.defaultConfig());
    ctx.execute(
            "create database "
                + getDatabaseName()
                + " memory users (admin identified by 'adminpwd' role admin)")
        .close();
    database = ctx.open(getDatabaseName(), "admin", "adminpwd");
  }

  protected String getDatabaseName() {
    return this.getClass().getSimpleName();
  }

  @After
  public void after() {
    database.close();
    ctx.drop(getDatabaseName());
    ctx.close();
  }

  protected void reopen(String user, String password) {
    database.close();
    database = ctx.open(getDatabaseName(), user, password);
  }
}
