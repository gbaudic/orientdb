package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.common.test.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 9/17/14
 */
public class IteratorSpeedTest extends BaseMemoryDatabase {
  public void testIterationSpeed() {

    OClass oClass = db.getMetadata().getSchema().createClass("SpeedTest");
    for (int i = 0; i < 1000000; i++) {
      ODocument document = new ODocument("SpeedTest");
      db.save(document);
    }

    ORecordIteratorClass iterator =
        new ORecordIteratorClass((ODatabaseDocumentInternal) db, "SpeedTest", true);
    iterator.setRange(
        new ORecordId(oClass.getDefaultClusterId(), 999998),
        new ORecordId(oClass.getDefaultClusterId(), 999999));

    long start = System.nanoTime();

    while (iterator.hasNext()) iterator.next();

    long end = System.nanoTime();
    System.out.println(end - start);
  }
}
