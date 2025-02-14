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
package com.orientechnologies.orient.server.distributed.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.impl.task.OGossipTask;
import com.orientechnologies.orient.server.distributed.impl.task.ORequestDatabaseConfigurationTask;
import com.orientechnologies.orient.server.distributed.impl.task.OUpdateDatabaseSequenceStatusTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Timer task that checks periodically the cluster health status.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OClusterHealthChecker implements Runnable {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(OClusterHealthChecker.class);
  private final ODistributedServerManager manager;
  private final long healthCheckerEveryMs;
  private long lastExecution = 0;

  public OClusterHealthChecker(
      final ODistributedServerManager manager, final long healthCheckerEveryMs) {
    this.manager = manager;
    this.healthCheckerEveryMs = healthCheckerEveryMs;
  }

  public synchronized void run() {
    logger.debug("Checking cluster health...");

    final long now = System.currentTimeMillis();

    if (now - lastExecution > (healthCheckerEveryMs / 3)) {
      // CHECK CURRENT STATUS OF DBS
      try {
        checkServerConfig();
        checkServerStatus();
        checkServerInStall();
        checkServerList();
        notifyDatabaseSequenceStatus();

      } catch (HazelcastInstanceNotActiveException e) {
        // IGNORE IT
      } catch (Exception t) {
        if (manager.getServerInstance().isActive())
          logger.error("Error on checking cluster health", t);
        else
          // SHUTDOWN IN PROGRESS
          logger.debug("Error on checking cluster health", t);
      } finally {
        logger.debug("Cluster health checking completed");
      }
    } else
      logger.debug(
          "Cluster health finished recently (%dms ago), skip this execution", now - lastExecution);

    lastExecution = now;
  }

  private void checkServerConfig() {
    // NO NODES CONFIGURED: CHECK IF THERE IS ANY MISCONFIGURATION BY CHECKING THE DATABASE STATUSES
    OrientDBDistributed context = (OrientDBDistributed) manager.getServerInstance().getDatabases();
    for (String databaseName : manager.getDatabases()) {
      if (manager.isSyncronizing(databaseName)) {
        continue;
      }
      final ODistributedConfiguration cfg = context.getDistributedConfiguration(databaseName);
      if (cfg == null) {
        continue;
      }
      final Set<String> confServers = cfg.getServers(null);
      for (String s : manager.getActiveServers()) {
        if (manager.isNodeAvailable(s, databaseName) && !confServers.contains(s)) {
          final Set<String> nodes = manager.getAvailableNodeNames(databaseName);

          // THE SERVERS HAS THE DATABASE ONLINE BUT IT IS NOT IN THE CFG. DETERMINE THE MOST UPD
          // CFG
          try {
            final ODistributedResponse response =
                manager.sendRequest(
                    databaseName, nodes, new ORequestDatabaseConfigurationTask(databaseName));

            final Object payload = response != null ? response.getPayload() : null;
            if (payload instanceof Map) {
              String mostUpdatedServer = null;
              int mostUpdatedServerVersion = -1;

              final Map<String, Object> responses = (Map<String, Object>) payload;
              for (Map.Entry<String, Object> r : responses.entrySet()) {
                if (r.getValue() instanceof ODocument) {
                  final ODocument doc = (ODocument) r.getValue();
                  int v = doc.field("version");
                  if (v > mostUpdatedServerVersion) {
                    mostUpdatedServerVersion = v;
                    mostUpdatedServer = r.getKey();
                  }
                }
              }

              if (cfg.getVersion() < mostUpdatedServerVersion) {
                // OVERWRITE DB VERSION
                context.setDistributedConfiguration(
                    databaseName,
                    new OModifiableDistributedConfiguration(
                        (ODocument) responses.get(mostUpdatedServer)));
              }
            }

          } catch (ODistributedOperationException e) {
            // NO SERVER RESPONDED, THE SERVER COULD BE ISOLATED: SET ALL THE SERVER AS OFFLINE
          }
        }
      }
    }
  }

  private void checkServerList() {
    final Set<String> activeServers = manager.getActiveServers();
    for (String server : activeServers) {
      int id = manager.getNodeIdByName(server);
      if (id == -1) {
        logger.infoNode(
            manager.getLocalNodeName(),
            "Server '%s' was not found in the list of registered servers. Reloading configuration"
                + " from cluster...",
            server);

        ((ODistributedPlugin) manager).reloadRegisteredNodes();
        id = manager.getNodeIdByName(server);
        if (id == -1) {
          if (server.equals(manager.getLocalNodeName())) {
            // LOCAL NODE
            logger.warnNode(
                manager.getLocalNodeName(),
                "Local server was not found in the list of registered servers after the update",
                server);

          } else {
            // REMOTE NODE
            logger.warnNode(
                manager.getLocalNodeName(),
                "Server '%s' was not found in the list of registered servers after the update,"
                    + " restarting the server...",
                server);

            try {
              ((ODistributedPlugin) manager).restartNode(server);
            } catch (IOException e) {
              logger.warnNode(
                  manager.getLocalNodeName(),
                  "Error on restarting server '%s' (error=%s)",
                  server,
                  e);
            }
          }
        }
        break;
      }
    }
  }

  private void checkServerStatus() {
    if (manager.getNodeStatus() != ODistributedServerManager.NODE_STATUS.ONLINE)
      // ONLY ONLINE NODE CAN TRY TO RECOVER FOR SINGLE DB STATUS
      return;

    OServer server = manager.getServerInstance();
    if (!server.isActive()) return;

    OrientDBDistributed context = (OrientDBDistributed) server.getDatabases();
    for (String dbName : manager.getDatabases()) {
      if (manager.isSyncronizing(dbName)) {
        continue;
      }
      final ODistributedServerManager.DB_STATUS localNodeStatus =
          manager.getDatabaseStatus(manager.getLocalNodeName(), dbName);
      if (localNodeStatus != ODistributedServerManager.DB_STATUS.NOT_AVAILABLE)
        // ONLY NOT_AVAILABLE NODE/DB CAN BE RECOVERED
        continue;

      final Set<String> servers = manager.getAvailableNodeNames(dbName);
      servers.remove(manager.getLocalNodeName());

      if (servers.isEmpty()) {
        logger.infoNode(
            manager.getLocalNodeName(),
            "No server are ONLINE for database '%s'. Considering local copy of database as the good"
                + " one. Setting status=ONLINE...",
            dbName);

        context.distributedSetOnline(dbName);

      } else {
        logger.infoNode(
            manager.getLocalNodeName(),
            "Trying to recover current server for database '%s'...",
            dbName);

        if (manager.getNodeStatus() != ODistributedServerManager.NODE_STATUS.ONLINE)
          // ONLY ONLINE NODE CAN TRY TO RECOVER FOR SINGLE DB STATUS
          return;

        final ODistributedConfiguration dCfg = context.getDistributedConfiguration(dbName);
        if (dCfg != null) {
          final boolean result = manager.installDatabase(true, dbName, false, true);

          if (result)
            logger.infoNode(
                manager.getLocalNodeName(), "Recover complete for database '%s'", dbName);
          else
            logger.infoNode(
                manager.getLocalNodeName(),
                "Recover cannot be completed for database '%s'",
                dbName);
        }
      }
    }
  }

  private void checkServerInStall() {
    if (manager.getNodeStatus() != ODistributedServerManager.NODE_STATUS.ONLINE)
      // ONLY ONLINE NODE CAN CHECK FOR OTHERS
      return;

    for (String dbName : manager.getDatabases()) {
      if (manager.isSyncronizing(dbName)) {
        continue;
      }
      final ODistributedServerManager.DB_STATUS localNodeStatus =
          manager.getDatabaseStatus(manager.getLocalNodeName(), dbName);
      if (localNodeStatus != ODistributedServerManager.DB_STATUS.ONLINE)
        // ONLY ONLINE NODE/DB CAN CHECK FOR OTHERS
        continue;

      final Set<String> servers = manager.getAvailableNodeNotLocalNames(dbName);

      if (servers.isEmpty()) continue;

      try {
        final ODistributedResponse response =
            manager.sendRequest(dbName, servers, new OGossipTask());

        final Object payload = response != null ? response.getPayload() : null;
        if (payload instanceof Map) {
          final Map<String, Object> responses = (Map<String, Object>) payload;
          servers.removeAll(responses.keySet());
        }
      } catch (ODistributedException e) {
        // NO SERVER RESPONDED, THE SERVER COULD BE ISOLATED: SET ALL THE SERVER AS OFFLINE
        logger.debugNode(
            manager.getLocalNodeName(), "Error on sending request for cluster health check", e);
      } catch (ODistributedOperationException e) {
        // NO SERVER RESPONDED, THE SERVER COULD BE ISOLATED: SET ALL THE SERVER AS OFFLINE
        logger.debugNode(
            manager.getLocalNodeName(), "Error on sending request for cluster health check", e);
      }

      for (String server : servers) {
        setDatabaseOffline(dbName, server);
      }
    }
  }

  private void notifyDatabaseSequenceStatus() {
    if (manager.getNodeStatus() != ODistributedServerManager.NODE_STATUS.ONLINE)
      // ONLY ONLINE NODE CAN TRY TO RECOVER FOR SINGLE DB STATUS
      return;

    if (!manager.getServerInstance().isActive()) return;

    for (String dbName : manager.getDatabases()) {
      if (manager.isSyncronizing(dbName)) {
        continue;
      }
      final ODistributedServerManager.DB_STATUS localNodeStatus =
          manager.getDatabaseStatus(manager.getLocalNodeName(), dbName);
      if (localNodeStatus != ODistributedServerManager.DB_STATUS.ONLINE)
        // ONLY NOT_AVAILABLE NODE/DB CAN BE RECOVERED
        continue;

      final List<String> servers = manager.getOnlineNodesNotLocal(dbName);

      if (servers.isEmpty()) continue;

      try {
        ODistributedDatabase sharedDb = manager.getDatabase(dbName);
        Optional<OTransactionSequenceStatus> status = sharedDb.status();
        if (status.isPresent()) {
          ORemoteTask task = new OUpdateDatabaseSequenceStatusTask(dbName, status.get());

          final ODistributedResponse response = manager.sendRequest(dbName, servers, task);
        }
      } catch (ODistributedException e) {
        // NO SERVER RESPONDED, THE SERVER COULD BE ISOLATED: SET ALL THE SERVER AS OFFLINE
        logger.debugNode(
            manager.getLocalNodeName(), "Error on sending request for cluster health check", e);
      } catch (ODistributedOperationException e) {
        // NO SERVER RESPONDED, THE SERVER COULD BE ISOLATED: SET ALL THE SERVER AS OFFLINE
        logger.debugNode(
            manager.getLocalNodeName(), "Error on sending request for cluster health check", e);
      }
    }
  }

  private void setDatabaseOffline(final String dbName, final String server) {
    if (manager.getDatabaseStatus(server, dbName) != ODistributedServerManager.DB_STATUS.ONLINE)
      return;

    if (OGlobalConfiguration.DISTRIBUTED_CHECK_HEALTH_CAN_OFFLINE_SERVER.getValueAsBoolean()) {
      logger.warnOut(
          manager.getLocalNodeName(),
          server,
          "Server '%s' did not respond to the gossip message (db=%s, timeout=%dms). Setting the"
              + " database as NOT_AVAILABLE",
          server,
          dbName,
          OGlobalConfiguration.DISTRIBUTED_HEARTBEAT_TIMEOUT.getValueAsLong());

      manager.setDatabaseStatus(server, dbName, ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);

    } else {

      logger.warnOut(
          manager.getLocalNodeName(),
          server,
          "Server '%s' did not respond to the gossip message (db=%s, timeout=%dms), but cannot be"
              + " set OFFLINE by configuration",
          server,
          dbName,
          OGlobalConfiguration.DISTRIBUTED_HEARTBEAT_TIMEOUT.getValueAsLong());
    }
  }
}
