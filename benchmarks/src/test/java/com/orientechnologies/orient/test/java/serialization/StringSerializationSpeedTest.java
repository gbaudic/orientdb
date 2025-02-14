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
package com.orientechnologies.orient.test.java.serialization;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import java.io.IOException;

public class StringSerializationSpeedTest extends SpeedTestMonoThread {

  public StringSerializationSpeedTest() {
    super(1000000);
  }

  @Override
  public void cycle() throws IOException {
    StringBuilder buffer = new StringBuilder();
    buffer.append(Integer.valueOf(300).toString());
    buffer.append(new Boolean(true).toString());
    buffer.append("Questa e una prova di scrittura di una stringa");
    buffer.append(Float.valueOf(3.0f).toString());
    buffer.append(Long.valueOf(30000000L).toString());

    buffer.toString().getBytes();
  }
}
