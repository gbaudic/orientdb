package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.Random;

import org.junit.Ignore;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 30.01.13
 */
@Ignore
public class HashIndexSpeedTest extends SpeedTestMonoThread {
  private ODatabaseDocumentInternal databaseDocumentTx;
  private OIndex hashIndex;
  private Random random = new Random();

  public HashIndexSpeedTest() {
    super(5000000);
  }

  @Override
  public void init() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null) buildDirectory = ".";

    databaseDocumentTx =
        new ODatabaseDocumentTx("plocal:" + buildDirectory + "/uniqueHashIndexTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    hashIndex =
        databaseDocumentTx
            .getMetadata()
            .getIndexManagerInternal()
            .createIndex(
                databaseDocumentTx,
                "hashIndex",
                "UNIQUE_HASH_INDEX",
                new OSimpleKeyIndexDefinition(OType.STRING),
                new int[0],
                null,
                null);
  }

  @Override
  public void cycle() throws Exception {
    databaseDocumentTx.begin();
    String key = "bsadfasfas" + random.nextInt();
    hashIndex.put(key, new ORecordId(0, 0));
    databaseDocumentTx.commit();
  }

  @Override
  public void deinit() throws Exception {
    databaseDocumentTx.drop();
  }
}
