package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadDBTest;
import java.util.Random;
import org.junit.Ignore;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 14.08.13
 */
@Ignore
public class SBTreeInsertionSpeedTest extends OrientMonoThreadDBTest {
  private OIndex index;
  private Random random = new Random();

  public SBTreeInsertionSpeedTest() {
    super(5000000);
  }

  @Override
  public void init() {

    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null) buildDirectory = ".";
    dropAndCreate();
    database.command("create index  sbtree_index unique String").close();

    index =
        ((ODatabaseDocumentInternal) database)
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex((ODatabaseDocumentInternal) database, "sbtree_index");
  }

  @Override
  public void cycle() throws Exception {
    database.begin();
    String key = "bsadfasfas" + random.nextInt();
    index.put(key, new ORecordId(0, 0));
    database.commit();
  }
}
