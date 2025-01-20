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

package com.orientechnologies.orient.test.internal;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.OrientDBRemote;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.server.OServer;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

public class FreezeMultiThreadingNonTXIT {
  private static final int TRANSACTIONAL_CREATOR_THREAD_COUNT = 2;
  private static final int TRANSACTIONAL_UPDATER_THREAD_COUNT = 2;
  private static final int TRANSACTIONAL_DELETER_THREAD_COUNT = 2;

  private static final int CREATOR_THREAD_COUNT = 10;
  private static final int UPDATER_THREAD_COUNT = 10;
  private static final int DELETER_THREAD_COUNT = 10;

  private static final int DOCUMENT_COUNT = 1000;

  private static final int TRANSACTIONAL_DOCUMENT_COUNT = 1000;

  private static final String URL = "remote:localhost/FreezeMultiThreadingTestNonTX";

  private static final String STUDENT_CLASS_NAME = "Student";
  private static final String TRANSACTIONAL_WORD = "Transactional";

  private AtomicInteger createCounter = new AtomicInteger(0);
  private AtomicInteger deleteCounter = new AtomicInteger(0);

  private AtomicInteger transactionalCreateCounter = new AtomicInteger(0);
  private AtomicInteger transactionalDeleteCounter = new AtomicInteger(0);

  private final ExecutorService executorService =
      Executors.newFixedThreadPool(
          CREATOR_THREAD_COUNT + UPDATER_THREAD_COUNT + DELETER_THREAD_COUNT + 1);
  private ConcurrentSkipListSet<Integer> deleted = new ConcurrentSkipListSet<Integer>();

  private ConcurrentSkipListSet<Integer> deletedInTransaction =
      new ConcurrentSkipListSet<Integer>();
  private CountDownLatch countDownLatch = new CountDownLatch(1);
  private OServer server;
  private OrientDB remote;

  private class NonTransactionalAdder implements Callable<Void> {
    public Void call() throws Exception {
      Thread.currentThread().setName("Adder - " + Thread.currentThread().getId());
      ODatabaseSession database = remote.open("FreezeMultiThreadingTestNonTX", "admin", "admin");
      try {
        countDownLatch.await();

        long value = createCounter.getAndIncrement();
        while (value < DOCUMENT_COUNT) {
          Thread.sleep(200);
          ODocument document = new ODocument(STUDENT_CLASS_NAME);
          document.field("counter", value);
          database.save(document);

          if (value % 10 == 0)
            System.out.println(Thread.currentThread() + " : document " + value + " added");

          value = createCounter.getAndIncrement();
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      } catch (Throwable e) {
        e.printStackTrace();
        return null;
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
        database.close();
      }
    }
  }

  private class TransactionalAdder implements Callable<Void> {
    public Void call() throws Exception {
      Thread.currentThread().setName("TransactionalAdder - " + Thread.currentThread().getId());
      ODatabaseSession database = remote.open("FreezeMultiThreadingTestNonTX", "admin", "admin");
      try {
        countDownLatch.await();

        long value = transactionalCreateCounter.getAndIncrement();
        while (value < TRANSACTIONAL_DOCUMENT_COUNT) {
          Thread.sleep(200);

          database.begin();
          ODocument document = new ODocument(TRANSACTIONAL_WORD + STUDENT_CLASS_NAME);
          document.field("counter", value);
          database.save(document);

          database.commit();
          if (value % 10 == 0)
            System.out.println(Thread.currentThread() + " : document " + value + " added");

          value = transactionalCreateCounter.getAndIncrement();
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      } catch (Throwable e) {
        e.printStackTrace();
        return null;
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
        database.close();
      }
    }
  }

  private class NonTransactionalUpdater implements Callable<Void> {

    private int updateCounter = 0;

    public Void call() throws Exception {
      Thread.currentThread().setName("Updater - " + Thread.currentThread().getId());
      ODatabaseSession database = remote.open("FreezeMultiThreadingTestNonTX", "admin", "admin");
      try {

        countDownLatch.await();

        while (updateCounter < DOCUMENT_COUNT) {
          if (updateCounter > createCounter.get()) {
            continue;
          }

          List<OResult> execute = null;
          if (updateCounter % 10 == 0)
            System.out.println(
                Thread.currentThread() + " : before search cycle(update)" + updateCounter);
          do {
            try {
              execute =
                  database
                      .command(
                          "select * from " + STUDENT_CLASS_NAME + " where counter = ?",
                          updateCounter)
                      .stream()
                      .toList();
            } catch (ORecordNotFoundException onfe) {
              System.out.println("Record not found for doc " + updateCounter);
            } catch (OException e) {
              if (e.getMessage().contains("Error during loading record with id")) {
                // ignore this because record deleted
                System.out.println(Thread.currentThread() + "exception record already deleted");
                continue;
              }

              if (e.getCause() instanceof ConcurrentModificationException) {
                System.out.println(Thread.currentThread() + "exception concurrent modification");
                // ignore this because record deleted
                continue;
              }

              throw e;
            }
          } while (!deleted.contains(updateCounter) && (execute == null || execute.isEmpty()));
          if (!deleted.contains(updateCounter)) {
            if (updateCounter % 10 == 0)
              System.out.println(
                  Thread.currentThread() + " : after search cycle(update) " + updateCounter);

            OElement document = execute.get(0).getElement().get();
            document.setProperty("counter2", document.<Object>getProperty("counter"));
            try {
              database.save(document);

              if (updateCounter % 10 == 0)
                System.out.println(
                    Thread.currentThread() + " : document " + updateCounter + " updated");

              updateCounter++;
            } catch (ORecordNotFoundException ornfe) {
              System.out.println(Thread.currentThread() + " exception record already deleted");
            } catch (OConcurrentModificationException e) {
              System.out.println(
                  Thread.currentThread()
                      + " : concurrent modification exception while updating! "
                      + updateCounter);
            } catch (Exception e) {
              e.printStackTrace();
              throw e;
            }

          } else {
            System.out.println(
                Thread.currentThread()
                    + " : document "
                    + updateCounter
                    + " already deleted couldn't update!");
            updateCounter++;
          }
        }
        return null;
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
        database.close();
      }
    }
  }

  private class TransactionalUpdater implements Callable<Void> {
    private int updateCounter = 0;

    public Void call() throws Exception {
      Thread.currentThread().setName("TransactionalUpdater - " + Thread.currentThread().getId());
      ODatabaseSession database = remote.open("FreezeMultiThreadingTestNonTX", "admin", "admin");
      try {

        countDownLatch.await();

        while (updateCounter < TRANSACTIONAL_DOCUMENT_COUNT) {
          if (updateCounter > transactionalCreateCounter.get()) {
            continue;
          }
          List<OResult> execute = null;
          if (updateCounter % 10 == 0)
            System.out.println(
                Thread.currentThread() + " : before search cycle(update)" + updateCounter);
          do {
            try {
              execute =
                  database
                      .query(
                          "select * from "
                              + TRANSACTIONAL_WORD
                              + STUDENT_CLASS_NAME
                              + " where counter = ?",
                          updateCounter)
                      .stream()
                      .toList();
            } catch (ORecordNotFoundException onfe) {
              // ignore has been deleted
            } catch (OException e) {
              if (e.getMessage().contains("Error during loading record with id")) {
                // ignore this because record deleted
                System.out.println(Thread.currentThread() + "exception record already deleted");
                continue;
              }

              if (e.getCause() instanceof ConcurrentModificationException) {
                System.out.println(Thread.currentThread() + "exception concurrent modification");
                // ignore this because record deleted
                continue;
              }

              throw e;
            }
          } while (!deletedInTransaction.contains(updateCounter)
              && (execute == null || execute.isEmpty()));
          if (!deletedInTransaction.contains(updateCounter)) {
            if (updateCounter % 10 == 0)
              System.out.println(
                  Thread.currentThread() + " : after search cycle(update) " + updateCounter);

            database.begin();

            OElement document = execute.get(0).getElement().get();
            document.setProperty("counter2", document.<Object>getProperty("counter"));
            try {
              database.save(document);

              database.commit();
              if (updateCounter % 10 == 0)
                System.out.println(
                    Thread.currentThread() + " : document " + updateCounter + " updated");

              updateCounter++;
            } catch (ORecordNotFoundException ornfe) {
              System.out.println(Thread.currentThread() + " exception record already deleted");
            } catch (OConcurrentModificationException e) {
              System.out.println(
                  Thread.currentThread()
                      + " : concurrent modification exception while updating! "
                      + updateCounter);
            } catch (Exception e) {
              e.printStackTrace();
              throw e;
            }

          } else {
            System.out.println(
                Thread.currentThread()
                    + " : document "
                    + updateCounter
                    + " already deleted couldn't update!");
            updateCounter++;
          }
        }
        return null;
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
        database.close();
      }
    }
  }

  private class NonTransactionalDeleter implements Callable<Void> {

    public Void call() throws Exception {
      Thread.currentThread().setName("Deleter - " + Thread.currentThread().getId());

      ODatabaseSession database = remote.open("FreezeMultiThreadingTestNonTX", "admin", "admin");
      try {

        countDownLatch.await();

        int number = deleteCounter.getAndIncrement();
        while (number < DOCUMENT_COUNT) {
          // wait while necessary document will be created
          while (number > createCounter.get())
            ;
          try {

            List<OResult> execute;
            if (number % 10 == 0)
              System.out.println(
                  Thread.currentThread() + " : before search cycle (delete) " + number);
            do {
              execute =
                  database
                      .query("select * from " + STUDENT_CLASS_NAME + " where counter2 = ?", number)
                      .stream()
                      .toList();
            } while (execute == null || execute.isEmpty());

            if (number % 10 == 0)
              System.out.println(
                  Thread.currentThread() + " : after search cycle (delete)" + number);

            OElement document = execute.get(0).getElement().get();
            database.delete(document);

            deleted.add(number);

            if (number % 10 == 0)
              System.out.println(Thread.currentThread() + " : document deleted " + number);

            number = deleteCounter.getAndIncrement();
          } catch (OConcurrentModificationException e) {
            System.out.println(Thread.currentThread() + " : exception while deleted " + number);
          }
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      } catch (Throwable e) {
        e.printStackTrace();
        return null;
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
        database.close();
      }
    }
  }

  private class TransactionalDeleter implements Callable<Void> {

    public Void call() throws Exception {
      Thread.currentThread()
          .setName("TransactionalDeleterDeleter - " + Thread.currentThread().getId());

      ODatabaseSession database = remote.open("FreezeMultiThreadingTestNonTX", "admin", "admin");
      try {

        countDownLatch.await();

        int number = transactionalDeleteCounter.getAndIncrement();
        while (number < TRANSACTIONAL_DOCUMENT_COUNT) {
          // wait while necessary document will be created
          while (number > transactionalCreateCounter.get())
            ;
          try {

            List<OResult> execute;
            if (number % 10 == 0)
              System.out.println(
                  Thread.currentThread() + " : before search cycle (delete) " + number);
            do {
              execute =
                  database
                      .command(
                          "select * from "
                              + TRANSACTIONAL_WORD
                              + STUDENT_CLASS_NAME
                              + " where counter2 = ?",
                          number)
                      .stream()
                      .toList();
            } while (execute == null || execute.isEmpty());

            if (number % 10 == 0)
              System.out.println(
                  Thread.currentThread() + " : after search cycle (delete)" + number);

            database.begin();

            OElement document = execute.get(0).getElement().get();
            database.delete(document);

            database.commit();

            deletedInTransaction.add(number);

            if (number % 10 == 0)
              System.out.println(Thread.currentThread() + " : document deleted " + number);

            number = transactionalDeleteCounter.getAndIncrement();
          } catch (OConcurrentModificationException e) {
            System.out.println(Thread.currentThread() + " : exception while deleted " + number);
          }
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      } catch (Throwable e) {
        e.printStackTrace();
        return null;
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
        database.close();
      }
    }
  }

  private class Locker implements Callable<Void> {

    public Void call() throws Exception {
      Thread.currentThread().setName("Locker - " + Thread.currentThread().getId());

      try {
        countDownLatch.await();
        while (createCounter.get() < DOCUMENT_COUNT) {
          OrientDBRemote internal = (OrientDBRemote) OrientDBInternal.extract(remote);
          internal.freezeDatabase("FreezeMultiThreadingTestNonTX", "root", "root");
          ODatabaseSession database =
              remote.open("FreezeMultiThreadingTestNonTX", "admin", "admin");

          final List<OResult> beforeNonTxDocuments =
              database.query("select from " + STUDENT_CLASS_NAME).stream().toList();
          final List<OResult> beforeTxDocuments =
              database.query("select from " + TRANSACTIONAL_WORD + STUDENT_CLASS_NAME).stream()
                  .toList();

          database.close();

          System.out.println(
              "Freeze DB - nonTx : "
                  + beforeNonTxDocuments.size()
                  + " Tx : "
                  + beforeTxDocuments.size());
          try {
            Thread.sleep(10000);
          } finally {
            database = remote.open("FreezeMultiThreadingTestNonTX", "admin", "admin");
            final List<OResult> afterNonTxDocuments =
                database.query("select from " + STUDENT_CLASS_NAME).stream().toList();
            final List<OResult> afterTxDocuments =
                database.query("select from " + TRANSACTIONAL_WORD + STUDENT_CLASS_NAME).stream()
                    .toList();
            assertDocumentAreEquals(beforeNonTxDocuments, afterNonTxDocuments);
            assertDocumentAreEquals(beforeTxDocuments, afterTxDocuments);

            database.close();
            System.out.println(
                "Release DB - nonTx : "
                    + afterNonTxDocuments.size()
                    + " Tx : "
                    + afterTxDocuments.size());
            internal.releaseDatabase("FreezeMultiThreadingTestNonTX", "root", "root");
          }
          Thread.sleep(10000);
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      } catch (Throwable e) {
        e.printStackTrace();
        return null;
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT.setValue(1000);
    // OGlobalConfiguration.CLIENT_CHANNEL_MAX_POOL.setValue(50);

    server = OServer.startFromClasspathConfig("orientdb-server-config.xml");
    remote = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    System.out.println("Create db");
    // SETUP DB

    System.out.println("Recreating database");
    if (remote.exists("FreezeMultiThreadingTestNonTX")) {
      remote.drop("FreezeMultiThreadingTestNonTX");
    }
    remote.execute(
        "create database FreezeMultiThreadingTestNonTX plocal users(admin identified by 'admin'"
            + " role admin)");

    ODatabaseSession database = remote.open("FreezeMultiThreadingTestNonTX", "admin", "admin");

    OClass student = database.getMetadata().getSchema().createClass(STUDENT_CLASS_NAME);

    student.createProperty("counter", OType.INTEGER);
    student.createProperty("counter2", OType.INTEGER);

    student.createIndex("index1", OClass.INDEX_TYPE.UNIQUE, "counter");
    student.createIndex("index2", OClass.INDEX_TYPE.NOTUNIQUE, "counter2");
    student.createIndex("index3", OClass.INDEX_TYPE.NOTUNIQUE, "counter", "counter2");

    OClass transactionalStudent =
        database.getMetadata().getSchema().createClass(TRANSACTIONAL_WORD + STUDENT_CLASS_NAME);

    transactionalStudent.createProperty("counter", OType.INTEGER);
    transactionalStudent.createProperty("counter2", OType.INTEGER);

    transactionalStudent.createIndex("index4", OClass.INDEX_TYPE.UNIQUE, "counter");
    transactionalStudent.createIndex("index5", OClass.INDEX_TYPE.NOTUNIQUE, "counter2");
    transactionalStudent.createIndex("index6", OClass.INDEX_TYPE.NOTUNIQUE, "counter", "counter2");

    System.out.println("*in before***********CLOSE************************************");
    database.close();
  }

  @After
  public void teardown() {
    remote.drop("FreezeMultiThreadingTestNonTX");
    remote.close();
    server.shutdown();
  }

  @Test
  public void test() throws Exception {
    Set<Future<Void>> threads =
        new HashSet<Future<Void>>(
            CREATOR_THREAD_COUNT + DELETER_THREAD_COUNT + UPDATER_THREAD_COUNT + 1);

    for (int i = 0; i < CREATOR_THREAD_COUNT; ++i) {
      NonTransactionalAdder thread = new NonTransactionalAdder();
      threads.add(executorService.submit(thread));
    }

    for (int i = 0; i < UPDATER_THREAD_COUNT; ++i) {
      NonTransactionalUpdater thread = new NonTransactionalUpdater();
      threads.add(executorService.submit(thread));
    }

    for (int i = 0; i < DELETER_THREAD_COUNT; ++i) {
      NonTransactionalDeleter thread = new NonTransactionalDeleter();
      threads.add(executorService.submit(thread));
    }

    // for (int i = 0; i < TRANSACTIONAL_CREATOR_THREAD_COUNT; ++i) {
    // TransactionalAdder thread = new TransactionalAdder();
    // threads.add(executorService.submit(thread));
    // }

    // for (int i = 0; i < TRANSACTIONAL_UPDATER_THREAD_COUNT; ++i) {
    // TransactionalUpdater thread = new TransactionalUpdater();
    // threads.add(executorService.submit(thread));
    // }
    //
    // for (int i = 0; i < TRANSACTIONAL_DELETER_THREAD_COUNT; ++i) {
    // TransactionalDeleter thread = new TransactionalDeleter();
    // threads.add(executorService.submit(thread));
    // }

    threads.add(executorService.submit(new Locker()));

    countDownLatch.countDown();
    for (Future<Void> future : threads) future.get();

    System.out.println("finish");
  }

  private void assertDocumentAreEquals(List<OResult> firstDocs, List<OResult> secondDocs) {
    if (firstDocs.size() != secondDocs.size()) Assert.fail();

    outer:
    for (final OResult firstDoc : firstDocs) {
      for (final OResult secondDoc : secondDocs) {
        if (firstDoc.equals(secondDoc)) {
          final ODatabaseDocumentInternal databaseRecord =
              ODatabaseRecordThreadLocal.instance().get();
          Assert.assertTrue(
              ODocumentHelper.hasSameContentOf(
                  (ODocument) firstDoc.getElement().get(),
                  databaseRecord,
                  (ODocument) secondDoc.getElement().get(),
                  databaseRecord,
                  null));
          continue outer;
        }
      }

      Assert.fail("Document " + firstDoc + " was changed during DB freeze");
    }
  }
}
