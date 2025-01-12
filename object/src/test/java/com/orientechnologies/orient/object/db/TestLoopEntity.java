package com.orientechnologies.orient.object.db;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.object.db.entity.LoopEntity;
import org.junit.Test;

/** Created by tglman on 09/05/16. */
public class TestLoopEntity extends BaseObjectTest {

  @Test
  public void testLoop() {
    database.getEntityManager().registerEntityClasses(LoopEntity.class, true);
    assertTrue(database.getMetadata().getSchema().existsClass("LoopEntity"));
    assertTrue(database.getMetadata().getSchema().existsClass("LoopEntityLink"));
  }
}
