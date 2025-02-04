package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadDBTest;
import java.util.Date;
import org.junit.Ignore;

@Ignore
public class LocalPaginateStorageSpeedTest extends OrientMonoThreadDBTest {
  private ODocument record;
  private Date date = new Date();
  private byte[] content;
  private OAbstractPaginatedStorage storage;

  public LocalPaginateStorageSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000);
  }

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    LocalPaginateStorageSpeedTest test = new LocalPaginateStorageSpeedTest();
    test.data.go(test);
  }

  @Override
  public void init() {
    super.init();
    dropAndCreate();
    OSchema schema = database.getMetadata().getSchema();
    schema.createClass("Account");

    record = database.newInstance();

    database.begin(TXTYPE.NOTX);

    storage = (OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) database).getStorage();
  }

  @Override
  public void cycle() {
    record.reset();

    record.setClassName("Account");
    record.field("id", data.getCyclesDone());
    record.field("name", "Luca");
    record.field("surname", "Garulli");
    record.field("birthDate", date);
    record.field("salary", 3000f + data.getCyclesDone());

    content = record.toStream();

    storage.createRecord(new ORecordId(), content, 0, (byte) 'd');
  }

  @Override
  public void deinit() {
    System.out.println(Orient.instance().getProfiler().dump());
    super.deinit();
  }
}
