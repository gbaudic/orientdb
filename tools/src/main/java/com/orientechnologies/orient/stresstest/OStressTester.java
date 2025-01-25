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

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.stresstest.workload.OCheckWorkload;
import com.orientechnologies.orient.stresstest.workload.OWorkload;
import com.orientechnologies.orient.stresstest.workload.OWorkloadFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The main class of the OStressTester. It is instantiated from the OStressTesterCommandLineParser
 * and takes care of launching the needed threads (OOperationsExecutor) for executing the operations
 * of the test.
 *
 * @author Andrea Iacono
 */
public class OStressTester {
  /** The access mode to the database */
  public enum OMode {
    PLOCAL,
    MEMORY,
    REMOTE,
    DISTRIBUTED
  }

  private OConsoleProgressWriter consoleProgressWriter;
  private final OStressTesterSettings settings;

  private static final OWorkloadFactory workloadFactory = new OWorkloadFactory();
  private List<OWorkload> workloads = new ArrayList<OWorkload>();
  private OrientDB context;

  public OStressTester(final List<OWorkload> workloads, final OStressTesterSettings settings)
      throws Exception {
    this.workloads = workloads;
    if (settings.dbUser == null) {
      settings.dbUser = "admin";
    }
    if (settings.dbPassword == null) {
      settings.dbPassword = "adminPwd";
    }

    this.settings = settings;
    context = new OrientDB(settings.getUrl(), OrientDBConfig.defaultConfig());
  }

  public static void main(String[] args) {
    System.out.println(
        String.format(
            "OrientDB Stress Tool v.%s - %s", OConstants.getVersion(), OConstants.COPYRIGHT));

    int returnValue = 1;
    try {
      final OStressTester stressTester = OStressTesterCommandLineParser.getStressTester(args);
      returnValue = stressTester.execute();
    } catch (Exception ex) {
      System.err.println(ex.getMessage());
    }
    System.exit(returnValue);
  }

  @SuppressWarnings("unchecked")
  public int execute() throws Exception {

    int returnCode = 0;

    // we don't want logs from DB
    OLogManager.instance().setConsoleLevel("SEVERE");

    String mode = "memory";
    switch (settings.mode) {
      case MEMORY:
        mode = "memory";
        break;
      case PLOCAL:
        mode = "plocal";
        break;
      case REMOTE:
        mode = "plocal";
        break;
      case DISTRIBUTED:
        mode = "plocal";
        break;
    }

    // creates the temporary DB where to execute the test
    context
        .execute(
            "create database ? " + mode + " users (? identified by ? role admin)",
            settings.dbName,
            settings.dbUser,
            settings.dbPassword)
        .close();

    System.out.println(
        String.format("Created database [%s].", settings.getUrl() + settings.dbName));

    try {
      for (OWorkload workload : workloads) {
        consoleProgressWriter = new OConsoleProgressWriter("Console progress writer", workload);

        consoleProgressWriter.start();

        consoleProgressWriter.printMessage(
            String.format(
                "\nStarting workload %s (concurrencyLevel=%d)...",
                workload.getName(), settings.concurrencyLevel));

        final long startTime = System.currentTimeMillis();

        workload.execute(settings, context);

        final long endTime = System.currentTimeMillis();

        consoleProgressWriter.sendShutdown();

        System.out.println(
            String.format(
                "\n- Total execution time: %.3f secs", ((float) (endTime - startTime) / 1000f)));

        System.out.println(workload.getFinalResult());

        dumpHaMetrics();

        if (settings.checkDatabase && workload instanceof OCheckWorkload) {
          System.out.println(String.format("- Checking database..."));
          ((OCheckWorkload) workload).check(settings, context);
          System.out.println(String.format("- Check completed"));
        }
      }

      if (settings.resultOutputFile != null) writeFile();

    } catch (Exception ex) {
      System.err.println(
          "\nAn error has occurred while running the stress test: " + ex.getMessage());
      returnCode = 1;
    } finally {
      // we don't need to drop the in-memory DB
      if (settings.keepDatabaseAfterTest || settings.mode == OMode.MEMORY)
        consoleProgressWriter.printMessage(
            String.format("\nDatabase is available on [%s].", settings.getUrl() + settings.dbName));
      else {
        context.drop(settings.dbName);
        consoleProgressWriter.printMessage(
            String.format("\nDropped database [%s].", settings.getUrl() + settings.dbName));
      }
    }

    return returnCode;
  }

  private void dumpHaMetrics() {
    if (settings.haMetrics) {
      final ODatabase db = context.open(settings.dbName, "admin", "adminpwd");
      try {
        try (OResultSet output = db.command("ha status -latency -messages -output=text")) {
          System.out.println("HA METRICS");
          while (output.hasNext()) {
            OResult next = output.next();
            for (String property : next.getPropertyNames()) {
              System.out.println(property + " = " + next.getProperty(property));
            }
          }
        }
      } catch (Exception e) {
        // IGNORE IT
      } finally {
        db.close();
      }
    }
  }

  private void writeFile() {
    try {
      final StringBuilder output = new StringBuilder();
      output.append("{\"result\":[");
      int i = 0;
      for (OWorkload workload : workloads) {
        if (i++ > 0) output.append(",");
        output.append(workload.getFinalResultAsJson());
      }
      output.append("]}");

      OIOUtils.writeFile(new File(settings.resultOutputFile), output.toString());
    } catch (IOException e) {
      System.err.println("\nError on writing the result file : " + e.getMessage());
    }
  }

  public int getThreadsNumber() {
    return settings.concurrencyLevel;
  }

  public OMode getMode() {
    return settings.mode;
  }

  public String getPassword() {
    return settings.rootPassword;
  }

  public int getTransactionsNumber() {
    return settings.operationsPerTransaction;
  }

  public static OWorkloadFactory getWorkloadFactory() {
    return workloadFactory;
  }

  public OStressTesterSettings getSettings() {
    return settings;
  }
}
