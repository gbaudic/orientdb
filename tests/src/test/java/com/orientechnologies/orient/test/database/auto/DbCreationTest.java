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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import java.io.IOException;
import java.util.Locale;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "db")
public class DbCreationTest extends ObjectDBBaseTest {

  @Parameters(value = "url")
  public DbCreationTest(@Optional String url) {
    super(url);
    setAutoManageDatabase(false);

    Orient.instance().getProfiler().startRecording();
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {}

  @AfterClass
  @Override
  public void afterClass() throws Exception {}

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {}

  @AfterMethod
  @Override
  public void afterMethod() throws Exception {}

  @AfterMethod
  public void tearDown() {}

  @Test
  public void testDbCreationDefault() throws IOException {
    dropAndCreateDatabase("");
  }

  @Test(dependsOnMethods = {"testDbCreationDefault"})
  public void testChangeLocale() throws IOException {
    reopendb("admin", "admin");
    database.command(" ALTER DATABASE LOCALELANGUAGE  ?", Locale.GERMANY.getLanguage()).close();
    database.command(" ALTER DATABASE LOCALECOUNTRY  ?", Locale.GERMANY.getCountry()).close();
    database.reload();
    Assert.assertEquals(
        database.get(ODatabase.ATTRIBUTES.LOCALELANGUAGE), Locale.GERMANY.getLanguage());
    Assert.assertEquals(
        database.get(ODatabase.ATTRIBUTES.LOCALECOUNTRY), Locale.GERMANY.getCountry());
    database.set(ODatabase.ATTRIBUTES.LOCALECOUNTRY, Locale.ENGLISH.getCountry());
    database.set(ODatabase.ATTRIBUTES.LOCALELANGUAGE, Locale.ENGLISH.getLanguage());
    Assert.assertEquals(
        database.get(ODatabase.ATTRIBUTES.LOCALECOUNTRY), Locale.ENGLISH.getCountry());
    Assert.assertEquals(
        database.get(ODatabase.ATTRIBUTES.LOCALELANGUAGE), Locale.ENGLISH.getLanguage());
  }
}
