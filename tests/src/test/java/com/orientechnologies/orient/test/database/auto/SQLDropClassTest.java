package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.test.database.BaseMemoryDatabase;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Created by luigidellaquila on 09/11/16. */
public class SQLDropClassTest extends BaseMemoryDatabase {
  @Test
  public void testSimpleDrop() {
    Assert.assertFalse(db.getMetadata().getSchema().existsClass("testSimpleDrop"));
    db.command("create class testSimpleDrop").close();
    Assert.assertTrue(db.getMetadata().getSchema().existsClass("testSimpleDrop"));
    db.command("Drop class testSimpleDrop").close();
    Assert.assertFalse(db.getMetadata().getSchema().existsClass("testSimpleDrop"));
  }

  @Test
  public void testIfExists() {
    Assert.assertFalse(db.getMetadata().getSchema().existsClass("testIfExists"));
    db.command("create class testIfExists if not exists").close();
    db.getMetadata().getSchema().reload();
    Assert.assertTrue(db.getMetadata().getSchema().existsClass("testIfExists"));
    db.command("drop class testIfExists if exists").close();
    db.getMetadata().getSchema().reload();
    Assert.assertFalse(db.getMetadata().getSchema().existsClass("testIfExists"));
    db.command("drop class testIfExists if exists").close();
    db.getMetadata().getSchema().reload();
    Assert.assertFalse(db.getMetadata().getSchema().existsClass("testIfExists"));
  }
}
