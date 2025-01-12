package com.orientechnologies.orient.object.enhancement;

import com.orientechnologies.orient.object.db.BaseObjectTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestObjectWithDeletedLink extends BaseObjectTest {

  @Before
  public void before() {
    super.before();
    database.getEntityManager().registerEntityClass(SimpleSelfRef.class);
  }

  @Test
  public void testDeletedLink() {

    SimpleSelfRef ob1 = new SimpleSelfRef();
    ob1.setName("hobby one ");
    SimpleSelfRef ob2 = new SimpleSelfRef();
    ob2.setName("2");
    ob1.setFriend(ob2);

    ob1 = database.save(ob1);

    ob1 = database.reload(ob1, "", true);
    ob2 = ob1.getFriend();
    Assert.assertNotNull(ob1.getFriend());
    database.delete(ob2);

    ob1 = database.reload(ob1, "", true);
    Assert.assertNull(ob1.getFriend());
  }
}
