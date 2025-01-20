/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/** @author Michael Hiess */
public class MultipleDBTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public MultipleDBTest(@Optional String url) {
    super(url, "-");
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {}

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {}

  @AfterMethod
  @Override
  public void afterMethod() throws Exception {}

  @AfterClass
  @Override
  public void afterClass() throws Exception {}

  @Test
  public void testObjectMultipleDBsThreaded() throws Exception {
    final int operations_write = 1000;
    final int operations_read = 1;
    final int dbs = 10;

    final Set<String> times = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    Set<Future> threads = new HashSet<Future>();
    ExecutorService executorService = Executors.newFixedThreadPool(4);

    for (int i = 0; i < dbs; i++) {

      final String suffix = "" + i;

      Callable<Void> t =
          new Callable<Void>() {

            @Override
            public Void call() throws InterruptedException, IOException {

              OURLConnection data = OURLHelper.parse(url);
              dropdb(data.getDbName() + suffix);
              createdb(data.getDbName() + suffix);
              OObjectDatabaseTx tx =
                  new OObjectDatabaseTx(
                      (ODatabaseDocumentInternal)
                          openSession(data.getDbName() + suffix, "admin", "admin"));

              try {

                tx.set(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 1);
                tx.getMetadata().getSchema().getOrCreateClass("DummyObject");
                tx.getEntityManager().registerEntityClass(DummyObject.class);

                long start = System.currentTimeMillis();
                for (int j = 0; j < operations_write; j++) {
                  DummyObject dummy = new DummyObject("name" + j);

                  dummy = tx.save(dummy);

                  // CAN'T WORK FOR LHPEPS CLUSTERS BECAUSE CLUSTER POSITION CANNOT BE KNOWN
                  Assert.assertEquals(
                      ((ORID) dummy.getId()).getClusterPosition(), j, "RID was " + dummy.getId());
                }
                long end = System.currentTimeMillis();

                String time =
                    "("
                        + getDbId(tx)
                        + ") "
                        + "Executed operations (WRITE) in: "
                        + (end - start)
                        + " ms";
                times.add(time);

                start = System.currentTimeMillis();
                for (int j = 0; j < operations_read; j++) {
                  List<OResult> l = tx.query(" select * from DummyObject ").stream().toList();
                  Assert.assertEquals(l.size(), operations_write);
                }
                end = System.currentTimeMillis();

                time =
                    "("
                        + getDbId(tx)
                        + ") "
                        + "Executed operations (READ) in: "
                        + (end - start)
                        + " ms";
                times.add(time);

                tx.close();

              } finally {
                dropdb(tx.getName());
              }
              return null;
            }
          };

      threads.add(executorService.submit(t));
    }

    for (Future future : threads) future.get();
  }

  @Test
  public void testDocumentMultipleDBsThreaded() throws Exception {
    final int operations_write = 1000;
    final int operations_read = 1;
    final int dbs = 10;

    final Set<String> times = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    Set<Future> results = new HashSet<Future>();
    ExecutorService executorService = Executors.newFixedThreadPool(4);

    for (int i = 0; i < dbs; i++) {

      final String suffix = "" + i;
      Callable<Void> t =
          new Callable<Void>() {

            @Override
            public Void call() throws InterruptedException, IOException {

              OURLConnection data = OURLHelper.parse(url);
              dropdb(data.getDbName() + suffix);
              createdb(data.getDbName() + suffix);
              ODatabaseSession tx = openSession(data.getDbName() + suffix, "admin", "admin");

              try {

                tx.getMetadata().getSchema().createClass("DummyObject", 1);

                long start = System.currentTimeMillis();
                for (int j = 0; j < operations_write; j++) {

                  ODocument dummy = new ODocument("DummyObject");
                  dummy.field("name", "name" + j);

                  dummy = tx.save(dummy);

                  // CAN'T WORK FOR LHPEPS CLUSTERS BECAUSE CLUSTER POSITION CANNOT BE KNOWN
                  Assert.assertEquals(
                      dummy.getIdentity().getClusterPosition(),
                      j,
                      "RID was " + dummy.getIdentity());
                }
                long end = System.currentTimeMillis();

                String time =
                    "("
                        + tx.getName()
                        + ") "
                        + "Executed operations (WRITE) in: "
                        + (end - start)
                        + " ms";

                times.add(time);

                start = System.currentTimeMillis();
                for (int j = 0; j < operations_read; j++) {
                  List<OResult> l = tx.query(" select * from DummyObject ").stream().toList();
                  Assert.assertEquals(l.size(), operations_write);
                }
                end = System.currentTimeMillis();

                time =
                    "("
                        + tx.getName()
                        + ") "
                        + "Executed operations (READ) in: "
                        + (end - start)
                        + " ms";

                times.add(time);

              } finally {
                tx.close();

                dropdb(tx.getName());
              }
              return null;
            }
          };

      results.add(executorService.submit(t));
    }

    for (Future future : results) future.get();
  }

  private String getDbId(ODatabaseInternal tx) {
    if (tx.getStorage() instanceof OStorageRemote)
      return tx.getURL() + " - sessionId: " + ((OStorageRemote) tx.getStorage()).getSessionId();
    else return tx.getURL();
  }
}
