package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OSBTreeRidBagConcurrencySingleRidBag {
  public static final String URL = "plocal:target/testdb/OSBTreeRidBagConcurrencySingleRidBag";
  private final AtomicInteger positionCounter = new AtomicInteger();
  private final ConcurrentSkipListSet<ORID> ridTree = new ConcurrentSkipListSet<ORID>();
  private final CountDownLatch latch = new CountDownLatch(1);
  private ORID docContainerRid;
  private ExecutorService threadExecutor = Executors.newCachedThreadPool();
  private volatile boolean cont = true;

  private int topThreshold;
  private int bottomThreshold;
  private OrientDB orientDb;

  @Before
  public void beforeMethod() {
    topThreshold =
        OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(30);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(20);
    orientDb = new OrientDB("embedded:target/testdb/", OrientDBConfig.defaultConfig());
    if (orientDb.exists("OSBTreeRidBagConcurrencySingleRidBag")) {
      orientDb.drop("OSBTreeRidBagConcurrencySingleRidBag");
    }
    orientDb
        .execute(
            "create database OSBTreeRidBagConcurrencySingleRidBag plocal users (admin identified by"
                + " 'adminpwd' role admin)")
        .close();
  }

  @After
  public void afterMethod() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }

  @Test
  public void testConcurrency() throws Exception {
    ODatabaseSession db =
        orientDb.open("OSBTreeRidBagConcurrencySingleRidBag", "admin", "adminpwd");

    ODocument document = new ODocument();
    ORidBag ridBag = new ORidBag();
    ridBag.setAutoConvertToRecord(false);

    document.field("ridBag", ridBag);
    for (int i = 0; i < 100; i++) {
      final ORID ridToAdd = new ORecordId(0, positionCounter.incrementAndGet());
      ridBag.add(ridToAdd);
      ridTree.add(ridToAdd);
    }
    db.save(document);

    docContainerRid = document.getIdentity();

    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    for (int i = 0; i < 5; i++) futures.add(threadExecutor.submit(new RidAdder(i)));

    for (int i = 0; i < 5; i++) futures.add(threadExecutor.submit(new RidDeleter(i)));

    latch.countDown();

    Thread.sleep(30 * 60000);
    cont = false;

    for (Future<Void> future : futures) future.get();

    document = db.load(document.getIdentity());
    document.setLazyLoad(false);

    ridBag = document.field("ridBag");

    for (OIdentifiable identifiable : ridBag)
      Assert.assertTrue(ridTree.remove(identifiable.getIdentity()));

    Assert.assertTrue(ridTree.isEmpty());

    System.out.println("Result size is " + ridBag.size());
    db.close();
  }

  public class RidAdder implements Callable<Void> {
    private final int id;

    public RidAdder(int id) {
      this.id = id;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      int addedRecords = 0;

      try (ODatabaseDocument db =
          orientDb.open("OSBTreeRidBagConcurrencySingleRidBag", "admin", "adminpwd")) {
        while (cont) {
          List<ORID> ridsToAdd = new ArrayList<ORID>();
          for (int i = 0; i < 10; i++) {
            ridsToAdd.add(new ORecordId(0, positionCounter.incrementAndGet()));
          }

          while (true) {
            ODocument document = db.load(docContainerRid);
            document.setLazyLoad(false);

            ORidBag ridBag = document.field("ridBag");
            for (ORID rid : ridsToAdd) ridBag.add(rid);

            try {
              db.save(document);
            } catch (OConcurrentModificationException e) {
              continue;
            }

            break;
          }

          ridTree.addAll(ridsToAdd);
          addedRecords += ridsToAdd.size();
        }
      }

      System.out.println(
          RidAdder.class.getSimpleName() + ":" + id + "-" + addedRecords + " were added.");
      return null;
    }
  }

  public class RidDeleter implements Callable<Void> {
    private final int id;

    public RidDeleter(int id) {
      this.id = id;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      int deletedRecords = 0;

      Random rnd = new Random();

      try (ODatabaseDocument db =
          orientDb.open("OSBTreeRidBagConcurrencySingleRidBag", "admin", "adminpwd")) {
        while (cont) {
          while (true) {
            ODocument document = db.load(docContainerRid);
            document.setLazyLoad(false);
            ORidBag ridBag = document.field("ridBag");
            Iterator<OIdentifiable> iterator = ridBag.iterator();

            List<ORID> ridsToDelete = new ArrayList<ORID>();
            int counter = 0;
            while (iterator.hasNext()) {
              OIdentifiable identifiable = iterator.next();
              if (rnd.nextBoolean()) {
                iterator.remove();
                counter++;
                ridsToDelete.add(identifiable.getIdentity());
              }

              if (counter >= 5) break;
            }

            try {
              db.save(document);
            } catch (OConcurrentModificationException e) {
              continue;
            }

            ridTree.removeAll(ridsToDelete);

            deletedRecords += ridsToDelete.size();
            break;
          }
        }
      }

      System.out.println(
          RidDeleter.class.getSimpleName() + ":" + id + "-" + deletedRecords + " were deleted.");
      return null;
    }
  }
}
