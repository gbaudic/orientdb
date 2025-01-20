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

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ServerTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public ServerTest(@Optional String url) {
    super(url);
  }

  @Test
  public void testDbList() throws IOException {
    OrientDB ctx = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());

    try {
      List<String> dbs = ctx.list();
      Assert.assertFalse(dbs.isEmpty());
    } finally {
      ctx.close();
    }
  }

  @Test
  public void testOpenCloseCreateClass() throws IOException {

    OrientDB ctx = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    ctx.execute(
        "create database doubleOpenTest memory users(admin identified by 'admin' role admin)");

    ODatabaseDocument db = ctx.open("doubleOpenTest", "admin", "admin");
    try {
      ODocument d = new ODocument("User");
      db.save(d);
    } finally {
      db.close();
    }
    db = ctx.open("doubleOpenTest", "admin", "admin");
    try {
      ODocument d = new ODocument("User");
      db.save(d);
    } finally {
      db.close();
    }
    ctx.drop("doubleOpenTest");
    ctx.close();
  }
}
