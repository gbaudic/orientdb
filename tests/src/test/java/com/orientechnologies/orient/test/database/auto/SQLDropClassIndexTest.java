/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class SQLDropClassIndexTest {
  private static final OType EXPECTED_PROP1_TYPE = OType.DOUBLE;
  private static final OType EXPECTED_PROP2_TYPE = OType.INTEGER;

  private ODatabaseDocumentInternal database;
  private final String url;
  private OrientDB ctx;

  @Parameters(value = "url")
  public SQLDropClassIndexTest(@Optional final String url) {
    this.url = BaseTest.prepareUrl(url);
  }

  @BeforeClass
  public void beforeClass() {
    OURLConnection urlData = OURLHelper.parse(url);
    ctx =
        new OrientDB(
            urlData.getType() + ":" + urlData.getPath(),
            "root",
            "root",
            OrientDBConfig.defaultConfig());
    if (!ctx.exists(urlData.getDbName())) {
      ctx.execute(
              "create database "
                  + urlData.getDbName()
                  + " plocal users(admin identified by 'admin' role admin)")
          .close();
    }
    database = (ODatabaseDocumentInternal) ctx.open(urlData.getDbName(), "admin", "admin");

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.createClass("SQLDropClassTestClass");
    oClass.createProperty("prop1", EXPECTED_PROP1_TYPE);
    oClass.createProperty("prop2", EXPECTED_PROP2_TYPE);

    database.close();
  }

  @BeforeMethod
  public void beforeMethod() {
    if (database.isClosed()) {
      OURLConnection urlData = OURLHelper.parse(url);
      database = (ODatabaseDocumentInternal) ctx.open(urlData.getDbName(), "admin", "admin");
    }
  }

  @AfterMethod
  public void afterMethod() {
    database.close();
  }

  @Test
  public void testIndexDeletion() throws Exception {
    database
        .command(
            "CREATE INDEX SQLDropClassCompositeIndex ON SQLDropClassTestClass (prop1, prop2)"
                + " UNIQUE")
        .close();
    database.getMetadata().getIndexManagerInternal().reload(database);

    Assert.assertNotNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "SQLDropClassCompositeIndex"));

    database.command("DROP CLASS SQLDropClassTestClass").close();
    database.getMetadata().getIndexManagerInternal().reload(database);
    database.getMetadata().getSchema().reload();

    Assert.assertNull(database.getMetadata().getSchema().getClass("SQLDropClassTestClass"));
    Assert.assertNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "SQLDropClassCompositeIndex"));
    database.close();
    OURLConnection urlData = OURLHelper.parse(url);
    database = (ODatabaseDocumentInternal) ctx.open(urlData.getDbName(), "admin", "admin");
    Assert.assertNull(database.getMetadata().getSchema().getClass("SQLDropClassTestClass"));
    Assert.assertNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "SQLDropClassCompositeIndex"));
  }
}
