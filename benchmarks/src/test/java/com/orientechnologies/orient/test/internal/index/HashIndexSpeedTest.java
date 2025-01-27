package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadDBTest;
import java.util.Random;
import org.junit.Ignore;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 30.01.13
 */
@Ignore
public class HashIndexSpeedTest extends OrientMonoThreadDBTest {
  private OIndex hashIndex;
  private Random random = new Random();

  public HashIndexSpeedTest() {
    super(5000000);
  }

  @Override
  public void init() {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null) buildDirectory = ".";
    super.init();
    dropAndCreate();

    hashIndex =
        ((ODatabaseDocumentInternal) database)
            .getMetadata()
            .getIndexManagerInternal()
            .createIndex(
                (ODatabaseDocumentInternal) database,
                "hashIndex",
                "UNIQUE_HASH_INDEX",
                new OSimpleKeyIndexDefinition(OType.STRING),
                new int[0],
                null,
                null);
  }

  @Override
  public void cycle() throws Exception {
    database.begin();
    String key = "bsadfasfas" + random.nextInt();
    hashIndex.put(key, new ORecordId(0, 0));
    database.commit();
  }
}
