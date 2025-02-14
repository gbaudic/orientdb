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

import com.orientechnologies.orient.test.database.BaseMemoryDatabase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SQLCreateClassTest extends BaseMemoryDatabase {
  @Test
  public void testSimpleCreate() {
    Assert.assertFalse(db.getMetadata().getSchema().existsClass("testSimpleCreate"));
    db.command("create class testSimpleCreate").close();
    Assert.assertTrue(db.getMetadata().getSchema().existsClass("testSimpleCreate"));
  }

  @Test
  public void testIfNotExists() {
    Assert.assertFalse(db.getMetadata().getSchema().existsClass("testIfNotExists"));
    db.command("create class testIfNotExists if not exists").close();
    db.getMetadata().getSchema().reload();
    Assert.assertTrue(db.getMetadata().getSchema().existsClass("testIfNotExists"));
    db.command("create class testIfNotExists if not exists").close();
    db.getMetadata().getSchema().reload();
    Assert.assertTrue(db.getMetadata().getSchema().existsClass("testIfNotExists"));
    try {
      db.command("create class testIfNotExists").close();
      Assert.fail();
    } catch (Exception e) {
    }
  }
}
