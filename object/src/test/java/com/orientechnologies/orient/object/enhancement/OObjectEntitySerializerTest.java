package com.orientechnologies.orient.object.enhancement;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.object.db.BaseObjectTest;
import org.junit.Before;
import org.junit.Test;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 18.05.12
 */
public class OObjectEntitySerializerTest extends BaseObjectTest {

  @Before
  public void before() {
    super.before();
    database.getEntityManager().registerEntityClass(ExactEntity.class);
  }

  @Test
  public void testCallbacksHierarchy() {
    ExactEntity entity = new ExactEntity();
    database.save(entity);

    assertTrue(entity.callbackExecuted());
  }

  @Test
  public void testCallbacksHierarchyUpdate() {
    ExactEntity entity = new ExactEntity();
    entity = database.save(entity);

    entity.reset();
    database.save(entity);
    assertTrue(entity.callbackExecuted());
  }
}
