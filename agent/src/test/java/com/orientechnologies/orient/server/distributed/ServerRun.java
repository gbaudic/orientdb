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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import java.io.File;
import java.io.IOException;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;

/**
 * Running server instance.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ServerRun {
  protected final String serverId;
  protected String rootPath;
  protected OServer server;

  public ServerRun(final String iRootPath, final String serverId) {
    this.rootPath = iRootPath;
    this.serverId = serverId;
  }

  public static String getServerHome(final String iServerId) {
    return "target/server" + iServerId;
  }

  @Override
  public String toString() {
    return server.getDistributedManager().getLocalNodeName() + "(" + serverId + ")";
  }

  public OServer getServerInstance() {
    return server;
  }

  public String getServerId() {
    return serverId;
  }

  public String getBinaryProtocolAddress() {
    return server.getListenerByProtocol(ONetworkProtocolBinary.class).getListeningAddress(true);
  }

  public void deleteNode() {
    OFileUtils.deleteRecursively(new File(getServerHome()));
  }

  public boolean isActive() {
    return server.isActive();
  }

  public void crashServer() {
    if (server != null) {
      server.getClientConnectionManager().killAllChannels();
      ((OHazelcastPlugin) server.getDistributedManager())
          .getHazelcastInstance()
          .getLifecycleService()
          .terminate();
      server.shutdown();
    }
  }

  protected OrientGraph createDatabase(final String iName) {
    return createDatabase(iName, null);
  }

  public OrientGraph createDatabase(
      final String iName, final OCallable<Object, OrientGraphFactory> iCfgCallback) {
    String dbPath = getDatabasePath(iName);

    File folder = new File(dbPath);
    if (folder.exists()) {
      OFileUtils.deleteRecursively(new File(dbPath));
    }
    folder.mkdirs();

    OrientGraphFactory factory = new OrientGraphFactory("plocal:" + dbPath);

    if (iCfgCallback != null) iCfgCallback.call(factory);

    System.out.println("Creating database '" + iName + "' under: " + dbPath + "...");
    return factory.getNoTx();
  }

  public void copyDatabase(final String iDatabaseName, final String iDestinationDirectory)
      throws IOException {
    // COPY THE DATABASE TO OTHER DIRECTORIES
    System.out.println(
        "Dropping any previous database '" + iDatabaseName + "' under: " + iDatabaseName + "...");
    OFileUtils.deleteRecursively(new File(iDestinationDirectory));

    System.out.println(
        "Copying database folder " + iDatabaseName + " to " + iDestinationDirectory + "...");
    OFileUtils.copyDirectory(
        new File(getDatabasePath(iDatabaseName)), new File(iDestinationDirectory));
  }

  public OServer startServer(final String iServerConfigFile) throws Exception {
    System.out.println(
        "Starting server with serverId " + serverId + " from " + getServerHome() + "...");

    System.setProperty("ORIENTDB_HOME", getServerHome());

    if (server == null) server = OServerMain.create(false);

    server.setServerRootDirectory(getServerHome());
    server.startup(getClass().getClassLoader().getResourceAsStream(iServerConfigFile));
    server.activate();

    return server;
  }

  public void shutdownServer() {
    if (server != null) {
      try {
        ((OHazelcastPlugin) server.getDistributedManager()).getHazelcastInstance().shutdown();
      } catch (Exception e) {
      }
      server.shutdown();
    }

    closeStorages();

    Orient.instance().shutdown();
    Orient.instance().startup();
  }

  public void terminateServer() {
    if (server != null) {
      try {
        if (((OHazelcastPlugin) server.getDistributedManager())
            .getHazelcastInstance()
            .getLifecycleService()
            .isRunning())
          ((OHazelcastPlugin) server.getDistributedManager())
              .getHazelcastInstance()
              .getLifecycleService()
              .terminate();
      } catch (Exception e) {
      }
      server.shutdown();
    }

    closeStorages();
  }

  public void closeStorages() {}

  protected String getServerHome() {
    return getServerHome(serverId);
  }

  public String getDatabasePath(final String iDatabaseName) {
    return getDatabasePath(serverId, iDatabaseName);
  }

  public static String getDatabasePath(final String iServerId, final String iDatabaseName) {
    return new File(getServerHome(iServerId) + "/databases/" + iDatabaseName).getAbsolutePath();
  }

  public String getNodeName() {
    return server.getDistributedManager().getLocalNodeName();
  }
}
