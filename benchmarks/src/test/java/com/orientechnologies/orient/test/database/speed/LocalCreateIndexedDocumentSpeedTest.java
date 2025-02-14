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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadDBTest;
import org.junit.Ignore;

@Ignore
public class LocalCreateIndexedDocumentSpeedTest extends OrientMonoThreadDBTest {
  private ODocument record;

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    LocalCreateIndexedDocumentSpeedTest test = new LocalCreateIndexedDocumentSpeedTest();
    test.data.go(test);
  }

  public LocalCreateIndexedDocumentSpeedTest()
      throws InstantiationException, IllegalAccessException {
    super(1000000);
  }

  @Override
  public void init() {
    super.init();
    Orient.instance().getProfiler().startRecording();

    record = database.newInstance();

    // REMOVE THE INDEX
    System.out.println("Remove index...");
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndexes();
    System.out.println("Ok");

    database.begin(TXTYPE.NOTX);
  }

  @Override
  public void cycle() {
    record.reset();

    record.setClassName("Profile");
    record.field("nick", String.valueOf(100 + data.getCyclesDone()));
    record.field("name", "Luca");
    record.field("surname", "Garulli");

    record.save();

    if (data.getCyclesDone() == data.getCycles() - 1) {
      database.commit();

      System.out.println("\nIndexing...");
      database
          .getMetadata()
          .getSchema()
          .getClass("Profile")
          .getProperty("nick")
          .createIndex(OClass.INDEX_TYPE.UNIQUE);
      System.out.println("Done");
    }
  }
}
