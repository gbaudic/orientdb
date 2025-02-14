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
package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadDBTest;
import java.util.Date;
import org.junit.Ignore;

@Ignore
public class RemoteCreateDocumentSpeedTest extends OrientMonoThreadDBTest {
  private ODocument record;
  private Date date = new Date();
  private long beginRecords;
  private static final long DELAY = 0;

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    RemoteCreateDocumentSpeedTest test = new RemoteCreateDocumentSpeedTest();
    test.data.go(test);
  }

  public RemoteCreateDocumentSpeedTest() throws InstantiationException, IllegalAccessException {
    super(100000);
  }

  @Override
  public void init() {
    super.init();
    Orient.instance().getProfiler().startRecording();

    record = database.newInstance();

    database.begin(TXTYPE.NOTX);

    beginRecords = database.countClass("Account");
    System.out.println("Total accounts: " + beginRecords);
  }

  @Override
  public void cycle() {
    record.reset();

    record.setClassName("Account");
    record.field("id", data.getCyclesDone());
    record.field("name", "Luca");
    record.field("surname", "Garulli");
    record.field("birthDate", date);
    record.field("salary", 3000f + data.getCyclesDone());

    database.save(record);

    if (data.getCyclesDone() == data.getCycles() - 1) database.commit();

    if (DELAY > 0)
      try {
        Thread.sleep(DELAY);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
  }

  @Override
  public void deinit() {
    final long endRecords = database.countClass("Account");
    System.out.println(
        "Total accounts: " + endRecords + ". Expected: " + (beginRecords + data.getCycles()));

    System.out.println(Orient.instance().getProfiler().dump());

    super.deinit();
  }
}
