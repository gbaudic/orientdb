package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public class ORecreateIndexesTask implements Runnable {
  private static final OLogger logger = OLogManager.instance().logger(ORecreateIndexesTask.class);

  /** */
  private final OIndexManagerShared indexManager;

  private final OSharedContext ctx;
  private int ok;
  private int errors;

  public ORecreateIndexesTask(OIndexManagerShared indexManager, OSharedContext ctx) {
    this.indexManager = indexManager;
    this.ctx = ctx;
  }

  @Override
  public void run() {
    try {
      final ODatabaseDocumentEmbedded newDb =
          new ODatabaseDocumentEmbedded((OStorage) ctx.getStorage());
      newDb.activateOnCurrentThread();
      newDb.init(null, ctx);
      newDb.internalOpen("admin", "nopass", false);

      final Collection<ODocument> indexesToRebuild;
      indexManager.acquireExclusiveLock();
      try {
        final Collection<ODocument> knownIndexes =
            indexManager.getDocument().field(OIndexManagerShared.CONFIG_INDEXES);
        if (knownIndexes == null) {
          logger.warn("List of indexes is empty");
          indexesToRebuild = Collections.emptyList();
        } else {
          indexesToRebuild = new ArrayList<>();
          for (ODocument index : knownIndexes)
            indexesToRebuild.add(index.copy()); // make copies to safely iterate them later
        }
      } finally {
        indexManager.releaseExclusiveLock();
      }

      try {
        recreateIndexes(indexesToRebuild, newDb);
      } finally {
        indexManager.storage.synch();
        newDb.close();
      }

    } catch (Exception e) {
      logger.error("Error when attempt to restore indexes after crash was performed", e);
    }
  }

  private void recreateIndexes(
      Collection<ODocument> indexesToRebuild, ODatabaseDocumentEmbedded db) {
    ok = 0;
    errors = 0;
    for (ODocument index : indexesToRebuild) {
      try {
        recreateIndex(index, db);
      } catch (RuntimeException e) {
        logger.error("Error during addition of index '%s'", e, index);
        errors++;
      }
    }

    ((OIndexManagerShared) db.getMetadata().getIndexManagerInternal()).save(db);

    indexManager.rebuildCompleted = true;

    logger.info("%d indexes were restored successfully, %d errors", ok, errors);
  }

  private void recreateIndex(ODocument indexDocument, ODatabaseDocumentEmbedded db) {
    final OIndexInternal index = createIndex(indexDocument);
    final OIndexMetadata indexMetadata = index.loadMetadata(indexDocument);
    final OIndexDefinition indexDefinition = indexMetadata.getIndexDefinition();

    final boolean automatic = indexDefinition != null && indexDefinition.isAutomatic();
    // XXX: At this moment Lucene-based indexes are not durable, so we still need to rebuild them.
    final boolean durable = !"LUCENE".equalsIgnoreCase(indexMetadata.getAlgorithm());

    // The database and its index manager are in a special half-open state now, the index manager
    // is created, but not populated
    // with the index metadata, we have to rebuild the whole index list manually and insert it
    // into the index manager.

    if (automatic) {
      if (durable) {
        logger.info(
            "Index '%s' is a durable automatic index and will be added as is without"
                + " rebuilding",
            indexMetadata.getName());
        addIndexAsIs(indexDocument, index, db);
      } else {
        logger.info(
            "Index '%s' is a non-durable automatic index and must be rebuilt",
            indexMetadata.getName());
        rebuildNonDurableAutomaticIndex(indexDocument, index, indexMetadata, indexDefinition);
      }
    } else {
      if (durable) {
        logger.info(
            "Index '%s' is a durable non-automatic index and will be added as is without"
                + " rebuilding",
            indexMetadata.getName());
        addIndexAsIs(indexDocument, index, db);
      } else {
        logger.info(
            "Index '%s' is a non-durable non-automatic index and will be added as is without"
                + " rebuilding",
            indexMetadata.getName());
        addIndexAsIs(indexDocument, index, db);
      }
    }
  }

  private void rebuildNonDurableAutomaticIndex(
      ODocument indexDocument,
      OIndexInternal index,
      OIndexMetadata indexMetadata,
      OIndexDefinition indexDefinition) {
    index.loadFromConfiguration(indexDocument);
    index.delete();

    final String indexName = indexMetadata.getName();
    final Set<String> clusters = indexMetadata.getClustersToIndex();
    final String type = indexMetadata.getType();

    if (indexName != null && clusters != null && !clusters.isEmpty() && type != null) {
      logger.info("Start creation of index '%s'", indexName);
      index.create(indexMetadata, false, new OIndexRebuildOutputListener(index));

      indexManager.addIndexInternal(index);

      logger.info(
          "Index '%s' was successfully created and rebuild is going to be started", indexName);

      index.rebuild(new OIndexRebuildOutputListener(index));

      ok++;

      logger.info("Rebuild of '%s index was successfully finished", indexName);
    } else {
      errors++;
      logger.error(
          "Information about index was restored incorrectly, following data were loaded : "
              + "index name '%s', index definition '%s', clusters %s, type %s",
          null, indexName, indexDefinition, clusters, type);
    }
  }

  private void addIndexAsIs(
      ODocument indexDocument, OIndexInternal index, ODatabaseDocumentEmbedded database) {
    if (index.loadFromConfiguration(indexDocument)) {
      indexManager.addIndexInternal(index);

      ok++;
      logger.info("Index '%s' was added in DB index list", index.getName());
    } else {
      try {
        logger.error("Index '%s' can't be restored and will be deleted", null, index.getName());
        index.delete();
      } catch (Exception e) {
        logger.error("Error while deleting index '%s'", e, index.getName());
      }
      errors++;
    }
  }

  private OIndexInternal createIndex(ODocument idx) {
    final String indexType = idx.field(OIndexInternal.CONFIG_TYPE);

    if (indexType == null) {
      logger.error("Index type is null, will process other record", null);
      throw new OIndexException(
          "Index type is null, will process other record. Index configuration: " + idx.toString());
    }
    OIndexMetadata m = OIndexAbstract.loadMetadataFromDoc(idx);
    return OIndexes.createIndex(indexManager.storage, m);
  }
}
