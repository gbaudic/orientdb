package com.orientechnologies.orient.test.database.auto;

import static org.testng.AssertJUnit.assertTrue;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/** @author Artem Orobets (enisher-at-gmail.com) */
@Test(groups = "db")
public class RemoteProtocolCommandsTest extends DocumentDBBaseTest {
  private static final String serverPort = System.getProperty("orient.server.port", "2424");

  @Parameters(value = "url")
  public RemoteProtocolCommandsTest(@Optional String url) {
    super(url);
  }

  @Test
  public void testListDatabasesMemoryDB() throws Exception {
    final Random random = new Random();

    final String plocalDatabaseName = "plocalTestListDatabasesMemoryDB" + random.nextInt();
    baseContext.execute("create database ? plocal", plocalDatabaseName);

    final String memoryDatabaseName = "memoryTestListDatabasesMemoryDB" + random.nextInt();
    baseContext.execute("create database ? plocal", memoryDatabaseName);

    List<String> list = baseContext.list();

    Assert.assertTrue(list.contains(plocalDatabaseName), "Check plocal db is in list");
    Assert.assertTrue(list.contains(memoryDatabaseName), "Check memory db is in list");
  }

  @Test(enabled = false)
  // This is not supported anymore direct record operations are removed from the storage, only tx is
  // available
  public void testRawCreateWithoutIDTest() {
    OClass clazz = this.database.getMetadata().getSchema().createClass("RidCreationTestClass");
    OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) this.database.getStorage();
    ODocument doc = new ODocument("RidCreationTestClass");
    doc.field("test", "test");
    ORecordId bad = new ORecordId(-1, -1);
    OStorageOperationResult<OPhysicalPosition> res =
        storage.createRecord(bad, doc.toStream(), doc.getVersion(), ODocument.RECORD_TYPE);

    // assertTrue(" the cluster is not valid", bad.clusterId >= 0);
    String ids = "";
    for (int aId : clazz.getClusterIds()) ids += aId;

    assertTrue(
        " returned id:" + bad.getClusterId() + " shoud be one of:" + ids,
        Arrays.binarySearch(clazz.getClusterIds(), bad.getClusterId()) >= 0);
  }
}
