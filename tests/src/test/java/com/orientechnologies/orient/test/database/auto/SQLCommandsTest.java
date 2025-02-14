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

import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMap;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import java.io.File;
import java.util.Collection;
import java.util.Locale;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-delete")
public class SQLCommandsTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLCommandsTest(@Optional String url) {
    super(url);
  }

  public void createProperty() {
    OSchema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("account")) schema.createClass("account");

    database.command("create property account.timesheet string").close();

    Assert.assertEquals(
        database.getMetadata().getSchema().getClass("account").getProperty("timesheet").getType(),
        OType.STRING);
  }

  @Test(dependsOnMethods = "createProperty")
  public void createLinkedClassProperty() {
    database.command("create property account.knows embeddedmap account").close();

    Assert.assertEquals(
        database.getMetadata().getSchema().getClass("account").getProperty("knows").getType(),
        OType.EMBEDDEDMAP);
    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("account")
            .getProperty("knows")
            .getLinkedClass(),
        database.getMetadata().getSchema().getClass("account"));
  }

  @Test(dependsOnMethods = "createLinkedClassProperty")
  public void createLinkedTypeProperty() {
    database.command("create property account.tags embeddedlist string").close();

    Assert.assertEquals(
        database.getMetadata().getSchema().getClass("account").getProperty("tags").getType(),
        OType.EMBEDDEDLIST);
    Assert.assertEquals(
        database.getMetadata().getSchema().getClass("account").getProperty("tags").getLinkedType(),
        OType.STRING);
  }

  @Test(dependsOnMethods = "createLinkedTypeProperty")
  public void removeProperty() {
    database.command("drop property account.timesheet").close();
    database.command("drop property account.tags").close();

    Assert.assertFalse(
        database.getMetadata().getSchema().getClass("account").existsProperty("timesheet"));
    Assert.assertFalse(
        database.getMetadata().getSchema().getClass("account").existsProperty("tags"));
  }

  @Test(dependsOnMethods = "removeProperty")
  public void testSQLScript() {
    String cmd = "";
    cmd += "select from ouser limit 1;begin;";
    cmd += "let a = create vertex set script = true;";
    cmd += "let b = select from v limit 1;";
    cmd += "create edge from $a to $b;";
    cmd += "commit;";
    cmd += "return $a;";

    OResultSet result = database.execute("sql", cmd);

    Assert.assertTrue(result.hasNext());
    Assert.assertTrue(result.next().getProperty("script"));
  }

  public void testClusterRename() {
    if (database.getURL().startsWith("memory:")) return;

    Collection<String> names = database.getClusterNames();
    Assert.assertFalse(names.contains("testClusterRename".toLowerCase(Locale.ENGLISH)));

    database.command("create cluster testClusterRename").close();

    names = database.getClusterNames();
    Assert.assertTrue(names.contains("testClusterRename".toLowerCase(Locale.ENGLISH)));

    database.command("alter cluster testClusterRename name testClusterRename42").close();
    names = database.getClusterNames();

    Assert.assertTrue(names.contains("testClusterRename42".toLowerCase(Locale.ENGLISH)));
    Assert.assertFalse(names.contains("testClusterRename".toLowerCase(Locale.ENGLISH)));

    if (database.getURL().startsWith("plocal:")) {
      String storagePath = database.getStorage().getConfiguration().getDirectory();

      final OWOWCache wowCache =
          (OWOWCache) ((OLocalPaginatedStorage) database.getStorage()).getWriteCache();

      File dataFile =
          new File(
              storagePath,
              wowCache.nativeFileNameById(
                  wowCache.fileIdByName("testclusterrename42" + OPaginatedCluster.DEF_EXTENSION)));
      File mapFile =
          new File(
              storagePath,
              wowCache.nativeFileNameById(
                  wowCache.fileIdByName(
                      "testclusterrename42" + OClusterPositionMap.DEF_EXTENSION)));

      Assert.assertTrue(dataFile.exists());
      Assert.assertTrue(mapFile.exists());
    }
  }
}
