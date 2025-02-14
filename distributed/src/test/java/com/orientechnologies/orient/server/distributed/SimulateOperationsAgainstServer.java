/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/** Executes random operations against multiple servers */
public class SimulateOperationsAgainstServer {
  protected static final int delay = 0;
  private static final int MAX_RETRY = 30;
  protected final AtomicLong totalOperations = new AtomicLong();
  protected int count = 1000;
  protected int threads = 20;
  protected String[] urls =
      new String[] {"remote:localhost:2424/test", "remote:localhost:2425/test"};
  protected String className = "Customer";
  protected String userName = "admin";
  protected String userPassword = "admin";

  private OrientDB ctx;

  public static void main(String[] args) {
    new SimulateOperationsAgainstServer().randomExecute();
  }

  public SimulateOperationsAgainstServer() {
    OGlobalConfiguration.CLIENT_CHANNEL_MAX_POOL.setValue(threads + 5);
  }

  public void randomExecute() {
    final ExecutorService executor = Executors.newFixedThreadPool(threads);

    for (int i = 0; i < threads; ++i) {
      final int id = i;

      executor.submit(
          new Runnable() {
            private int threadId = id;

            @Override
            public void run() {
              for (int i = 0; i < count; ++i) {
                final Random rnd = new Random();

                try {
                  switch (rnd.nextInt(5)) {
                    case 0:
                      createDocument(
                          threadId, i, urls[rnd.nextInt(urls.length)], className, rnd.nextInt(5));
                      break;
                    case 1:
                      queryClass(
                          threadId, i, urls[rnd.nextInt(urls.length)], className, rnd.nextInt(10));
                      break;
                    case 2:
                      updateDocument(
                          threadId, i, urls[rnd.nextInt(urls.length)], className, rnd.nextInt(100));
                      break;
                    case 3:
                      deleteDocument(
                          threadId, i, urls[rnd.nextInt(urls.length)], className, rnd.nextInt(100));
                      break;
                    case 4:
                      pause(threadId, i, rnd.nextInt(2000));
                      break;
                  }

                  totalOperations.addAndGet(1);

                  if (delay > 0)
                    try {
                      Thread.sleep(delay);
                    } catch (InterruptedException e) {
                    }

                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
            }
          });
    }
  }

  protected void createDocument(
      final int threadId,
      final int iCycle,
      final String dbUrl,
      final String className,
      final int iProperties) {
    final ODatabaseDocument db = getDatabase(dbUrl);
    try {
      log(threadId, iCycle, dbUrl, " creating document: class=" + className);

      ODocument doc = new ODocument(className);
      for (int i = 0; i < iProperties; ++i) {
        doc.field("prop" + i, "propValue" + i);
      }
      doc.save();
    } finally {
      db.close();
    }
  }

  protected void queryClass(
      final int threadId,
      final int iCycle,
      final String dbUrl,
      final String className,
      final int iMax) {
    final ODatabaseDocument db = getDatabase(dbUrl);
    try {
      log(threadId, iCycle, dbUrl, " query class=" + className);

      List<OResult> result = db.query("select from " + className).stream().toList();

      int browsed = 0;
      for (OResult r : result) {
        if (browsed++ > iMax) return;

        r.getRecord().toString();
      }

    } finally {
      db.close();
    }
  }

  protected void updateDocument(
      final int threadId,
      final int iCycle,
      final String dbUrl,
      final String className,
      final int iSkip) {
    final ODatabaseDocument db = getDatabase(dbUrl);
    for (int retry = 0; retry < MAX_RETRY; ++retry) {
      ODocument doc = null;
      try {
        List<OResult> result =
            db.query("select from " + className + " skip " + iSkip + " limit 1").stream().toList();

        if (result == null || result.isEmpty())
          log(threadId, iCycle, dbUrl, " update no item " + iSkip + " because out of range");
        else {
          doc = (ODocument) result.get(0).getElement().get();
          doc.field("updated", "" + (doc.getVersion() + 1));
          doc.save();
          log(threadId, iCycle, dbUrl, " updated item " + iSkip + " RID=" + result.get(0));
        }

        // OK
        break;

      } catch (OConcurrentModificationException e) {
        log(
            threadId,
            iCycle,
            dbUrl,
            " concurrent update against record "
                + doc
                + ", reload it and retry "
                + retry
                + "/"
                + MAX_RETRY
                + "...");
        if (doc != null) doc.reload(null, true);

      } catch (ORecordNotFoundException e) {
        log(threadId, iCycle, dbUrl, " update no item " + iSkip + " because not found");
        break;

      } finally {
        db.close();
      }
    }
  }

  protected void deleteDocument(
      final int threadId,
      final int iCycle,
      final String dbUrl,
      final String className,
      final int iSkip) {
    final ODatabaseDocument db = getDatabase(dbUrl);
    for (int retry = 0; retry < MAX_RETRY; ++retry) {
      ODocument doc = null;
      try {
        List<OResult> result =
            db.query("select from " + className + " skip " + iSkip + " limit 1").stream().toList();

        if (result == null || result.isEmpty())
          log(threadId, iCycle, dbUrl, " delete no item " + iSkip + " because out of range");
        else {
          doc = result.get(0).getElement().get().getRecord();
          doc.delete();
          log(threadId, iCycle, dbUrl, " deleted item " + iSkip + " RID=" + result.get(0));
        }
        break;
      } catch (OConcurrentModificationException e) {
        log(
            threadId,
            iCycle,
            dbUrl,
            " concurrent delete against record "
                + doc
                + ", reload it and retry "
                + retry
                + "/"
                + MAX_RETRY
                + "...");
        if (doc != null) doc.reload(null, true);
      } catch (ORecordNotFoundException e) {
        log(threadId, iCycle, dbUrl, " delete no item " + iSkip + " because not found");
      } finally {
        db.close();
      }
    }
  }

  protected void pause(final int threadId, final int iCycle, final long iTime) {
    try {
      log(threadId, iCycle, "-", "pausing " + iTime + "ms");
      Thread.sleep(iTime);
    } catch (InterruptedException e) {
    }
  }

  protected void log(
      final int threadId, final int iCycle, final String dbUrl, final String iMessage) {
    System.out.println(
        String.format(
            "%-12d [%2d:%-4d] %25s %s", totalOperations.get(), threadId, iCycle, dbUrl, iMessage));
  }

  protected ODatabaseDocument getDatabase(final String dbUrl) {
    OURLConnection data = OURLHelper.parse(dbUrl);
    if (ctx == null) {
      ctx = new OrientDB(data.getType() + ":" + data.getPath(), OrientDBConfig.defaultConfig());
    }
    return ctx.open(data.getDbName(), userName, userPassword);
  }
}
