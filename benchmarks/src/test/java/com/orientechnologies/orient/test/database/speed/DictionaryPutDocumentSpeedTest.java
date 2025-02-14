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
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadDBTest;
import org.junit.Ignore;

@Ignore
public class DictionaryPutDocumentSpeedTest extends OrientMonoThreadDBTest {
  private ODocument record;
  private long startNum;

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    // System.setProperty("url", "remote:localhost:2424/demo");
    DictionaryPutDocumentSpeedTest test = new DictionaryPutDocumentSpeedTest();
    test.data.go(test);
  }

  public DictionaryPutDocumentSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000);
    record = new ODocument();
  }

  @Override
  public void init() {
    super.init();
    for (OIndex idx : database.getMetadata().getSchema().getClass("Account").getIndexes()) {
      idx.delete();
    }

    Orient.instance().getProfiler().startRecording();

    startNum = database.getDictionary().size();

    System.out.println("Total element in the dictionary at the beginning: " + startNum);

    record = new ODocument();
  }

  @Override
  public void cycle() {
    int id = (int) (startNum + data.getCyclesDone());

    record.reset();

    record.setClassName("Account");
    record.field("id", data.getCyclesDone());
    record.field("name", "Luca");
    record.field("surname", "Garulli");
    record.field("salary", 3000f + data.getCyclesDone());

    database.getDictionary().put("doc-" + id, record);
  }

  @Override
  public void deinit() {
    System.out.println(
        "Total element in the dictionary at the end: " + database.getDictionary().size());

    database.close();
    super.deinit();
  }
}
