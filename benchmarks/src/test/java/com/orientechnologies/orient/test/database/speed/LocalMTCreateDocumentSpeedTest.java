package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.AfterClass;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/19/14
 */
public class LocalMTCreateDocumentSpeedTest {
  private static final Random random = new Random();
  private OURLConnection data;
  private ODatabaseDocument database;
  private Date date = new Date();
  private CountDownLatch latch = new CountDownLatch(1);
  private List<Future> futures;
  private volatile boolean stop = false;
  private ExecutorService executorService = Executors.newCachedThreadPool();

  private final List<String> users = new ArrayList<String>();

  private OrientDB ctx;

  public void init() {
    data = OURLHelper.parse(System.getProperty("url"));
    ctx = new OrientDB(data.getType() + ":" + data.getPath(), OrientDBConfig.defaultConfig());
    if (ctx.exists(data.getDbName())) {
      ctx.drop(data.getDbName());
    }
    ctx.execute(
        "create database ? " + data.getDbType() + " users(admin identfied by 'admin' role admin)",
        data.getDbName());
    database = ctx.open(data.getDbName(), "admin", "admin");

    database.getMetadata().getSchema().createClass("Account");

    final OSecurity security = database.getMetadata().getSecurity();
    for (int i = 0; i < 100; i++) {
      users.add("user" + i);
      security.createUser("user" + i, "user" + i, "admin");
    }

    futures = new ArrayList<Future>();
    for (int i = 0; i < 1; i++) futures.add(executorService.submit(new Saver()));
  }

  public void cycle() throws Exception {
    latch.countDown();

    Thread.sleep(10 * 60 * 1000);
    stop = true;

    System.out.println("Stop insertion");
    long sum = 0;
    for (Future<Long> future : futures) sum += future.get();

    System.out.println("Speed : " + (sum / futures.size()) + " ns per document.");

    futures.clear();

    latch = new CountDownLatch(1);

    stop = false;
    System.out.println("Start reading");
    System.out.println("Doc count : " + database.countClass("Account"));

    for (int i = 0; i < 8; i++)
      futures.add(
          executorService.submit(
              new Reader(
                  database.countClass("Account"),
                  database.getMetadata().getSchema().getClass("Account").getDefaultClusterId())));

    latch.countDown();

    Thread.sleep(10 * 60 * 1000);

    stop = true;

    sum = 0;
    for (Future future : futures) sum += (Long) future.get();

    System.out.println("Speed : " + (sum / futures.size()) + " ns per document.");
  }

  @AfterClass
  public void deinit() {
    ctx.drop(data.getDbName());
  }

  private final class Saver implements Callable<Long> {

    private Saver() {}

    @Override
    public Long call() throws Exception {
      Random random = new Random();
      latch.await();

      long counter = 0;
      long start = System.nanoTime();
      while (!stop) {

        final String user = users.get(random.nextInt(users.size()));

        final ODatabaseDocument database = ctx.open(data.getDbName(), user, user);

        ODocument record = new ODocument("Account");
        record.field("id", 1);
        record.field("name", "Luca");
        record.field("surname", "Garulli");
        record.field("birthDate", date);
        record.field("salary", 3000f);
        record.save();

        counter++;

        database.close();
      }
      long end = System.nanoTime();

      return ((end - start) / counter);
    }
  }

  private final class Reader implements Callable<Long> {

    private final int docCount;
    private final int clusterId;
    public volatile int size;

    public Reader(long docCount, int clusterId) {
      this.docCount = (int) docCount;
      this.clusterId = clusterId;
    }

    @Override
    public Long call() throws Exception {

      latch.await();
      final ODatabaseDocument database = ctx.open(data.getDbName(), "admin", "admin");

      long counter = 0;
      long start = System.nanoTime();
      while (!stop) {
        ODocument document = database.load(new ORecordId(clusterId, random.nextInt(docCount)));
        if (document != null) counter++;
      }
      long end = System.nanoTime();

      database.close();
      return ((end - start) / counter);
    }
  }
}
