package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexUnique;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.Random;
import org.junit.Ignore;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @since 30.01.13
 */
@Ignore
public class MVRBTreeInsertionSpeedTest extends SpeedTestMonoThread {
  private ODatabaseDocumentInternal databaseDocumentTx;
  private OIndexUnique index;
  private Random random = new Random();

  public MVRBTreeInsertionSpeedTest() {
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

    index =
        (OIndexUnique)
            databaseDocumentTx
                .getMetadata()
                .getIndexManagerInternal()
                .createIndex(
                    databaseDocumentTx,
                    "mvrbtreeIndexTest",
                    "UNIQUE",
                    new OSimpleKeyIndexDefinition(OType.STRING),
                    new int[0],
                    null,
                    null);
  }

  @Override
  public void cycle() throws Exception {
    String key = "bsadfasfas" + random.nextInt();
    index.put(key, new ORecordId(0, 0));
  }

  @Override
  public void deinit() throws Exception {
    databaseDocumentTx.close();
  }
}
