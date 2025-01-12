package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class OLiveIndexRebuildTest {
  private OrientDB ctx;
  private ODatabasePool pool;

  private final String indexName = "liveIndex";
  private final String className = "liveIndexClass";
  private final String propertyName = "liveIndexProperty";

  private final AtomicBoolean stop = new AtomicBoolean();

  @Test
  @Ignore
  public void testLiveIndexRebuild() throws Exception {
    ctx = new OrientDB("memory:", OrientDBConfig.defaultConfig());
    ctx.execute(
        "create database liveIndexRebuild memory users(admin identified by 'adminpwd' role"
            + " 'admin')");
    pool = ctx.cachedPool("liveIndexRebuild", "admin", "adminpwd");
    ODatabaseSession database = pool.acquire();
    final OClass clazz = database.getMetadata().getSchema().createClass(className);
    clazz.createProperty(propertyName, OType.INTEGER);

    clazz.createIndex(indexName, OClass.INDEX_TYPE.UNIQUE, propertyName);

    for (int i = 0; i < 1000000; i++) {
      ODocument document = new ODocument(className);
      document.field(propertyName, i);
      database.save(document);
    }

    ExecutorService executorService = Executors.newFixedThreadPool(6);
    List<Future<?>> futures = new ArrayList<Future<?>>();

    for (int i = 0; i < 5; i++) {
      futures.add(executorService.submit(new Reader()));
    }

    futures.add(executorService.submit(new Writer()));

    Thread.sleep(60 * 60 * 1000);

    stop.set(true);
    executorService.shutdown();

    long minInterval = Long.MAX_VALUE;
    long maxInterval = Long.MIN_VALUE;

    for (Future<?> future : futures) {
      Object result = future.get();
      if (result instanceof long[]) {
        long[] results = (long[]) result;
        if (results[0] < minInterval) minInterval = results[0];

        if (results[1] > maxInterval) maxInterval = results[1];
      }
    }

    System.out.println(
        "Min interval "
            + (minInterval / 1000000)
            + ", max interval "
            + (maxInterval / 1000000)
            + " ms");
  }

  private final class Writer implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      try {
        long rebuildInterval = 0;
        long rebuildCount = 0;
        while (!stop.get()) {
          for (int i = 0; i < 10; i++) {
            final ODatabaseDocument database = pool.acquire();
            try {
              long start = System.nanoTime();
              database.command("rebuild index " + indexName).close();
              long end = System.nanoTime();
              rebuildInterval += (end - start);
              rebuildCount++;
            } finally {
              database.close();
            }

            if (stop.get()) break;
          }

          Thread.sleep(5 * 60 * 1000);
        }

        System.out.println(
            "Average rebuild interval " + ((rebuildInterval / rebuildCount) / 1000000) + ", ms");
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      }
      return null;
    }
  }

  private final class Reader implements Callable<long[]> {
    @Override
    public long[] call() throws Exception {
      long minInterval = Long.MAX_VALUE;
      long maxInterval = Long.MIN_VALUE;

      try {

        while (!stop.get()) {
          ODatabaseDocument database = pool.acquire();
          try {
            long start = System.nanoTime();

            final OResultSet result =
                database.query(
                    "select from "
                        + className
                        + " where "
                        + propertyName
                        + " >= 100 and "
                        + propertyName
                        + "< 200");

            long end = System.nanoTime();
            long interval = end - start;

            if (interval > maxInterval) maxInterval = interval;

            if (interval < minInterval) minInterval = interval;

            Assert.assertEquals(result.stream().count(), 100);
          } finally {
            database.close();
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      }

      return new long[] {minInterval, maxInterval};
    }
  }
}
