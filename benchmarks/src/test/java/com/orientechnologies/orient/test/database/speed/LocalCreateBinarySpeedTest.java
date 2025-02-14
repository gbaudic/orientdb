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
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadDBTest;
import java.util.Random;
import org.junit.Ignore;

@Ignore
public class LocalCreateBinarySpeedTest extends OrientMonoThreadDBTest {
  private ORecordBytes record;
  private static final int RECORD_SIZE = 512;
  private byte[] recordContent;

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    LocalCreateBinarySpeedTest test = new LocalCreateBinarySpeedTest();
    test.data.go(test);
  }

  public LocalCreateBinarySpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000);
  }

  @Override
  public void init() {
    super.init();
    Orient.instance().getProfiler().startRecording();

    record = new ORecordBytes();

    database.begin(TXTYPE.NOTX);
    Random rnd = new Random();
    recordContent = new byte[RECORD_SIZE];
    for (int i = 0; i < RECORD_SIZE; ++i) recordContent[i] = (byte) rnd.nextInt(256);
  }

  @Override
  public void cycle() {
    record.reset(recordContent).save("binary");

    if (data.getCyclesDone() == data.getCycles() - 1) database.commit();
  }
}
