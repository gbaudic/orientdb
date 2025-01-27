package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadDBTest;
import java.util.Date;
import org.junit.Ignore;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10/16/13
 */
@Ignore
public class NotUniqueIndexSpeedTest extends OrientMonoThreadDBTest {
  private int counter;
  private Date date;

  public NotUniqueIndexSpeedTest() throws Exception {
    super(50000);
    date = new Date();
  }

  @Override
  public void init() {
    super.init();

    dropAndCreate();

    OSchema schema = database.getMetadata().getSchema();
    OClass testClass = schema.createClass("test");
    testClass.createProperty("indexdate", OType.DATE);
    testClass.createIndex("indexdate_index", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "indexdate");
  }

  @Override
  public void cycle() throws Exception {
    String fVal = counter + "123456790qwertyASD";
    counter++;

    database.command(
        "insert into test (x,    y,    z,    j,    k ,   l,    m,    indexdate), values (?, ?, ?,"
            + " ?, ?, ?, ?, ?)",
        fVal,
        fVal,
        fVal,
        fVal,
        fVal,
        fVal,
        fVal,
        date);
  }
}
