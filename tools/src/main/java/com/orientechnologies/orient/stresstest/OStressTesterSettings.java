/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.stresstest;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import java.io.File;

/**
 * StressTester settings.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OStressTesterSettings {
  public String dbName;
  public OStressTester.OMode mode;
  public String rootPassword;
  public String resultOutputFile;
  public String plocalPath;
  public int operationsPerTransaction;
  public int delay;
  public int concurrencyLevel;
  public String remoteIp;
  public boolean haMetrics;
  public String workloadCfg;
  public boolean keepDatabaseAfterTest;
  public int remotePort = 2424;
  public boolean checkDatabase = false;
  public OStorageRemote.CONNECTION_STRATEGY loadBalancing =
      OStorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_REQUEST;
  public String dbUser;
  public String dbPassword;

  protected String getUrl() {
    switch (mode) {
      case MEMORY:
        return "memory:";
      case REMOTE:
        return "remote:" + remoteIp + ":" + remotePort + "/" + dbName;
      case DISTRIBUTED:
        return null;
      case PLOCAL:
      default:
        String basePath = System.getProperty("java.io.tmpdir") + "/orientdb/";
        if (plocalPath != null) {
          basePath = plocalPath;
        }

        if (!basePath.endsWith(File.separator)) basePath += File.separator;

        return "embedded:" + basePath;
    }
  }
}
