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
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OClusterDoesNotExistException;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "schema")
public class SchemaTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public SchemaTest(@Optional String url) {
    super(url);
  }

  public void createSchema() throws IOException {

    if (database.getMetadata().getSchema().existsClass("Account")) return;

    createBasicTestSchema();
  }

  @Test(dependsOnMethods = "createSchema")
  public void checkSchema() {

    OSchema schema = database.getMetadata().getSchema();

    assert schema != null;
    assert schema.getClass("Profile") != null;
    assert schema.getClass("Profile").getProperty("nick").getType() == OType.STRING;
    assert schema.getClass("Profile").getProperty("name").getType() == OType.STRING;
    assert schema.getClass("Profile").getProperty("surname").getType() == OType.STRING;
    assert schema.getClass("Profile").getProperty("registeredOn").getType() == OType.DATETIME;
    assert schema.getClass("Profile").getProperty("lastAccessOn").getType() == OType.DATETIME;

    assert schema.getClass("Whiz") != null;
    assert schema.getClass("whiz").getProperty("account").getType() == OType.LINK;
    assert schema
        .getClass("whiz")
        .getProperty("account")
        .getLinkedClass()
        .getName()
        .equalsIgnoreCase("Account");
    assert schema.getClass("WHIZ").getProperty("date").getType() == OType.DATE;
    assert schema.getClass("WHIZ").getProperty("text").getType() == OType.STRING;
    assert schema.getClass("WHIZ").getProperty("text").isMandatory();
    assert schema.getClass("WHIZ").getProperty("text").getMin().equals("1");
    assert schema.getClass("WHIZ").getProperty("text").getMax().equals("140");
    assert schema.getClass("whiz").getProperty("replyTo").getType() == OType.LINK;
    assert schema
        .getClass("Whiz")
        .getProperty("replyTo")
        .getLinkedClass()
        .getName()
        .equalsIgnoreCase("Account");
  }

  @Test(dependsOnMethods = "checkSchema")
  public void checkInvalidNamesBefore30() {

    OSchema schema = database.getMetadata().getSchema();

    schema.createClass("TestInvalidName,");
    Assert.assertNotNull(schema.getClass("TestInvalidName,"));
    schema.createClass("TestInvalidName;");
    Assert.assertNotNull(schema.getClass("TestInvalidName;"));
    schema.createClass("TestInvalid Name");
    Assert.assertNotNull(schema.getClass("TestInvalid Name"));
    schema.createClass("TestInvalid.Name");
    Assert.assertNotNull(schema.getClass("TestInvalid.Name"));
  }

  @Test(dependsOnMethods = "checkSchema")
  public void checkSchemaApi() {

    OSchema schema = database.getMetadata().getSchema();

    try {
      Assert.assertNull(schema.getClass("Animal33"));
    } catch (OSchemaException e) {
    }
  }

  @Test(dependsOnMethods = "checkSchemaApi")
  public void checkClusters() {

    for (OClass cls : database.getMetadata().getSchema().getClasses()) {
      if (!cls.isAbstract()) assert database.getClusterNameById(cls.getDefaultClusterId()) != null;
    }
  }

  @Test(dependsOnMethods = "createSchema")
  public void checkTotalRecords() {

    Assert.assertTrue(database.countRecords() > 0);
  }

  @Test(expectedExceptions = OValidationException.class)
  public void checkErrorOnUserNoPasswd() {

    database.getMetadata().getSecurity().createUser("error", null, (String) null);
  }

  @Test
  public void testMultiThreadSchemaCreation() throws InterruptedException {

    Thread thread =
        new Thread(
            new Runnable() {

              @Override
              public void run() {
                ODatabaseRecordThreadLocal.instance().set(database);
                ODocument doc = new ODocument("NewClass");
                database.save(doc);

                database.delete(doc);
                database.getMetadata().getSchema().dropClass("NewClass");
              }
            });

    thread.start();
    thread.join();
  }

  @Test
  public void createAndDropClassTestApi() {

    final String testClassName = "dropTestClass";
    final int clusterId;
    OClass dropTestClass = database.getMetadata().getSchema().createClass(testClassName);
    clusterId = dropTestClass.getDefaultClusterId();
    database.getMetadata().getSchema().reload();
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));

    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));
    database.getMetadata().getSchema().dropClass(testClassName);
    database.getMetadata().getSchema().reload();
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));

    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));
  }

  @Test
  public void createAndDropClassTestCommand() {

    final String testClassName = "dropTestClass";
    final int clusterId;
    OClass dropTestClass = database.getMetadata().getSchema().createClass(testClassName);
    clusterId = dropTestClass.getDefaultClusterId();
    database.getMetadata().getSchema().reload();
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));

    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));
    database.command("drop class " + testClassName).close();
    database.reload();
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));

    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));
  }

  @Test(dependsOnMethods = "createSchema")
  public void customAttributes() {

    // TEST CUSTOM PROPERTY CREATION
    database
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .setCustom("stereotype", "icon");

    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        "icon");

    // TEST CUSTOM PROPERTY EXISTS EVEN AFTER REOPEN

    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        "icon");

    // TEST CUSTOM PROPERTY REMOVAL
    database
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .setCustom("stereotype", null);
    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        null);

    // TEST CUSTOM PROPERTY UPDATE
    database
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .setCustom("stereotype", "polygon");
    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        "polygon");

    // TEST CUSTOM PROPERTY UDPATED EVEN AFTER REOPEN

    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        "polygon");

    // TEST CUSTOM PROPERTY WITH =

    database
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .setCustom("equal", "this = that");

    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("equal"),
        "this = that");

    // TEST CUSTOM PROPERTY WITH = AFTER REOPEN

    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("equal"),
        "this = that");
  }

  @Test(dependsOnMethods = "createSchema")
  public void alterAttributes() {

    OClass company = database.getMetadata().getSchema().getClass("Company");
    OClass superClass = company.getSuperClass();

    Assert.assertNotNull(superClass);
    boolean found = false;
    for (OClass c : superClass.getSubclasses()) {
      if (c.equals(company)) {
        found = true;
        break;
      }
    }
    Assert.assertEquals(found, true);

    company.setSuperClass(null);
    Assert.assertNull(company.getSuperClass());
    for (OClass c : superClass.getSubclasses()) {
      Assert.assertNotSame(c, company);
    }

    database
        .command("alter class " + company.getName() + " superclass " + superClass.getName())
        .close();

    database.getMetadata().getSchema().reload();
    company = database.getMetadata().getSchema().getClass("Company");
    superClass = company.getSuperClass();

    Assert.assertNotNull(company.getSuperClass());
    found = false;
    for (OClass c : superClass.getSubclasses()) {
      if (c.equals(company)) {
        found = true;
        break;
      }
    }
    Assert.assertEquals(found, true);
  }

  @Test
  public void invalidClusterWrongClusterId() {

    try {
      database.command("create class Antani cluster 212121").close();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OClusterDoesNotExistException);
    }
  }

  @Test
  public void invalidClusterWrongClusterName() {

    try {
      database.command("create class Antani cluster blaaa").close();
      Assert.fail();

    } catch (Exception e) {
      Assert.assertTrue(e instanceof OCommandSQLParsingException);
    }
  }

  @Test
  public void invalidClusterWrongKeywords() {

    try {
      database.command("create class Antani the pen is on the table").close();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OCommandSQLParsingException);
    }
  }

  @Test
  public void testRenameClass() {

    OClass oClass = database.getMetadata().getSchema().createClass("RenameClassTest");

    ODocument document = new ODocument("RenameClassTest");
    database.save(document);

    document.reset();

    document.setClassName("RenameClassTest");
    database.save(document);

    OResultSet result = database.query("select from RenameClassTest");
    Assert.assertEquals(result.stream().count(), 2);

    oClass.set(OClass.ATTRIBUTES.NAME, "RenameClassTest2");

    database.getLocalCache().clear();

    result = database.query("select from RenameClassTest2");
    Assert.assertEquals(result.stream().count(), 2);
  }

  public void testMinimumClustersAndClusterSelection() {

    database.command("alter database minimumclusters 3").close();

    try {
      database.command("create class multipleclusters").close();

      database.reload();

      Assert.assertTrue(database.existsCluster("multipleclusters"));

      for (int i = 1; i < 3; ++i) {
        Assert.assertTrue(database.existsCluster("multipleclusters_" + i));
      }

      for (int i = 0; i < 6; ++i) {
        ODocument doc = new ODocument("multipleclusters").field("num", i);
        database.save(doc);
      }

      // CHECK THERE ARE 2 RECORDS IN EACH CLUSTER (ROUND-ROBIN STRATEGY)
      Assert.assertEquals(
          database.countClusterElements(database.getClusterIdByName("multipleclusters")), 2);
      for (int i = 1; i < 3; ++i) {
        Assert.assertEquals(
            database.countClusterElements(database.getClusterIdByName("multipleclusters_" + i)), 2);
      }

      // DELETE ALL THE RECORDS
      long deleted =
          database.command("delete from cluster:multipleclusters_2").next().getProperty("count");
      Assert.assertEquals(deleted, 2);

      // CHANGE CLASS STRATEGY to BALANCED
      database.command("alter class multipleclusters clusterselection balanced").close();
      database.reload();
      database.getMetadata().getSchema().reload();

      for (int i = 0; i < 2; ++i) {
        ODocument doc = new ODocument("multipleclusters");
        doc.field("num", i);
        database.save(doc);
      }

      Assert.assertEquals(
          database.countClusterElements(database.getClusterIdByName("multipleclusters_2")), 2);

    } finally {
      // RESTORE DEFAULT
      database.command("alter database minimumclusters 0").close();
    }
  }

  public void testExchangeCluster() {

    try {
      database.command("CREATE CLASS TestRenameClusterOriginal clusters 2").close();

      swapClusters(database, 1);
      swapClusters(database, 2);
      swapClusters(database, 3);
    } finally {

    }
  }

  public void testExistsProperty() {
    OSchema schema = database.getMetadata().getSchema();
    OClass classA = schema.createClass("TestExistsA");
    classA.createProperty("property", OType.STRING);
    Assert.assertTrue(classA.existsProperty("property"));
    Assert.assertNotNull(classA.getProperty("property"));
    OClass classB = schema.createClass("TestExistsB", classA);

    Assert.assertNotNull(classB.getProperty("property"));
    Assert.assertTrue(classB.existsProperty("property"));

    schema = ((OMetadataInternal) database.getMetadata()).getImmutableSchemaSnapshot();
    classB = schema.getClass("TestExistsB");

    Assert.assertNotNull(classB.getProperty("property"));
    Assert.assertTrue(classB.existsProperty("property"));
  }

  public void testWrongClassNameWithAt() {
    //    try {
    database.command("create class `Ant@ni`").close();
    //      Assert.fail();
    // why...? it can be allowed now with backtick quoting...
    // TODO review this
    //    } catch (Exception e) {
    //      Assert.assertTrue(e instanceof OSchemaException);
    //    }
  }

  public void testWrongClassNameWithSpace() {
    //    try {
    database.getMetadata().getSchema().createClass("Anta ni");
    //      Assert.fail();
    // TODO review//
    //    } catch (Exception e) {
    //      Assert.assertTrue(e instanceof OSchemaException);
    //    }
  }

  public void testWrongClassNameWithComma() {
    //    try {
    database.getMetadata().getSchema().createClass("Anta,ni");
    //      Assert.fail();
    //    TODO review
    //    } catch (Exception e) {
    //      Assert.assertTrue(e instanceof OSchemaException);
    //    }
  }

  public void testRenameWithSameNameIsNop() {
    database.getMetadata().getSchema().getClass("V").setName("V");
  }

  public void testRenameWithExistentName() {
    try {
      database.getMetadata().getSchema().getClass("V").setName("OUser");
      Assert.fail();
    } catch (OSchemaException e) {
    } catch (OCommandExecutionException e) {
    }
  }

  public void testShortNameAlreadyExists() {
    try {
      database.getMetadata().getSchema().getClass("V").setShortName("OUser");
      Assert.fail();
    } catch (IllegalArgumentException e) {
    } catch (OCommandExecutionException e) {
    }
  }

  @Test
  public void testDeletionOfDependentClass() {
    OSchema schema = database.getMetadata().getSchema();
    OClass oRestricted = schema.getClass(OSecurityShared.RESTRICTED_CLASSNAME);
    OClass classA = schema.createClass("TestDeletionOfDependentClassA", oRestricted);
    OClass classB = schema.createClass("TestDeletionOfDependentClassB", classA);
    schema.dropClass(classB.getName());
  }

  @Test
  public void testCaseSensitivePropNames() {
    String className = "TestCaseSensitivePropNames";
    String propertyName = "propName";
    database.command("create class " + className);
    database.command(
        "create property "
            + className
            + "."
            + propertyName.toUpperCase(Locale.ENGLISH)
            + " STRING");
    database.command(
        "create property "
            + className
            + "."
            + propertyName.toLowerCase(Locale.ENGLISH)
            + " STRING");

    database.command(
        "create index "
            + className
            + "."
            + propertyName.toLowerCase(Locale.ENGLISH)
            + " on "
            + className
            + "("
            + propertyName.toLowerCase(Locale.ENGLISH)
            + ") NOTUNIQUE");
    database.command(
        "create index "
            + className
            + "."
            + propertyName.toUpperCase(Locale.ENGLISH)
            + " on "
            + className
            + "("
            + propertyName.toUpperCase(Locale.ENGLISH)
            + ") NOTUNIQUE");

    database.command(
        "insert into "
            + className
            + " set "
            + propertyName.toUpperCase(Locale.ENGLISH)
            + " = 'FOO', "
            + propertyName.toLowerCase(Locale.ENGLISH)
            + " = 'foo'");
    database.command(
        "insert into "
            + className
            + " set "
            + propertyName.toUpperCase(Locale.ENGLISH)
            + " = 'BAR', "
            + propertyName.toLowerCase(Locale.ENGLISH)
            + " = 'bar'");

    try (OResultSet rs =
        database.command(
            "select from "
                + className
                + " where "
                + propertyName.toLowerCase(Locale.ENGLISH)
                + " = 'foo'")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }

    try (OResultSet rs =
        database.command(
            "select from "
                + className
                + " where "
                + propertyName.toLowerCase(Locale.ENGLISH)
                + " = 'FOO'")) {
      Assert.assertFalse(rs.hasNext());
    }

    try (OResultSet rs =
        database.command(
            "select from "
                + className
                + " where "
                + propertyName.toUpperCase(Locale.ENGLISH)
                + " = 'FOO'")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }

    try (OResultSet rs =
        database.command(
            "select from "
                + className
                + " where "
                + propertyName.toUpperCase(Locale.ENGLISH)
                + " = 'foo'")) {
      Assert.assertFalse(rs.hasNext());
    }

    OMetadataInternal md = database.getMetadata();
    md.reload();
    OSchema schema = md.getSchema();
    schema.reload();
    OClass clazz = schema.getClass(className);
    Set<OIndex> idx = clazz.getIndexes();
    Set<String> indexes = new HashSet<>();
    for (OIndex id : idx) {
      indexes.add(id.getName());
    }
    Assert.assertTrue(indexes.contains(className + "." + propertyName.toLowerCase(Locale.ENGLISH)));
    Assert.assertTrue(indexes.contains(className + "." + propertyName.toUpperCase(Locale.ENGLISH)));
    schema.dropClass(className);
  }

  private void swapClusters(ODatabaseDocumentInternal session, int i) {
    session
        .command("CREATE CLASS TestRenameClusterNew extends TestRenameClusterOriginal clusters 2")
        .close();

    session.command("INSERT INTO TestRenameClusterNew (iteration) VALUES(" + i + ")").close();

    session
        .command("ALTER CLASS TestRenameClusterOriginal removecluster TestRenameClusterOriginal")
        .close();
    session.command("ALTER CLASS TestRenameClusterNew removecluster TestRenameClusterNew").close();
    session.command("DROP CLASS TestRenameClusterNew").close();
    session
        .command("ALTER CLASS TestRenameClusterOriginal addcluster TestRenameClusterNew")
        .close();
    session.command("DROP CLUSTER TestRenameClusterOriginal").close();
    session.command("ALTER CLUSTER TestRenameClusterNew name TestRenameClusterOriginal").close();

    session.getLocalCache().clear();

    List<OResult> result =
        session.query("select * from TestRenameClusterOriginal").stream().toList();
    Assert.assertEquals(result.size(), 1);

    OResult document = result.get(0);
    Assert.assertEquals(document.<Object>getProperty("iteration"), i);
  }
}
