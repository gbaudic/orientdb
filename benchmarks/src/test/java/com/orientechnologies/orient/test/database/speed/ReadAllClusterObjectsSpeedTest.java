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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadDBTest;
import java.io.UnsupportedEncodingException;
import org.junit.Ignore;

@Ignore
public class ReadAllClusterObjectsSpeedTest extends OrientMonoThreadDBTest {
  private static final String CLASS_NAME = "Account";
  private int objectsRead;

  public ReadAllClusterObjectsSpeedTest() {
    super(5);
  }

  @Override
  public void cycle() throws UnsupportedEncodingException {
    objectsRead = 0;

    for (ODocument rec : database.browseClass(CLASS_NAME)) {
      ++objectsRead;
    }
  }

  @Override
  public void afterCycle() throws Exception {
    System.out.println(
        data.getCyclesDone()
            + "-> Read "
            + objectsRead
            + " objects in the cluster "
            + CLASS_NAME
            + "="
            + data().takeTimer());
  }
}
