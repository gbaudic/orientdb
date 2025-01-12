package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.object.db.entity.Car;
import com.orientechnologies.orient.object.db.entity.Person;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 16/12/15. */
public class OPersistentEmbeddedMapTest extends BaseObjectTest {

  @Before
  public void before() {
    super.before();
    database.setAutomaticSchemaGeneration(true);
    OEntityManager entityManager = database.getEntityManager();
    entityManager.registerEntityClass(Car.class);
    entityManager.registerEntityClass(Person.class);

    database.getMetadata().getSchema().synchronizeSchema();
  }

  @Test
  public void embeddedMapShouldContainCorrectValues() {
    Person person = createTestPerson();
    Person retrievedPerson;
    database.save(person);
    retrievedPerson = database.browseClass(Person.class).next();
    retrievedPerson = database.detachAll(retrievedPerson, true);

    Assert.assertEquals(person, retrievedPerson);
  }

  private Person createTestPerson() {
    Map<String, Car> placeToCar = new HashMap<String, Car>();
    placeToCar.put("USA", new Car("Cadillac Escalade", 1990));
    placeToCar.put("Japan", new Car("Nissan Skyline", 2001));
    placeToCar.put("UK", new Car("Jaguar XR", 2007));

    return new Person("John", placeToCar);
  }
}
