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
package com.orientechnologies.orient.core.iterator;

import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Iterator to browse multiple clusters forward and backward. Once browsed in a direction, the
 * iterator cannot change it. This iterator with "live updates" set is able to catch updates to the
 * cluster sizes while browsing. This is the case when concurrent clients/threads insert and remove
 * item in any cluster the iterator is browsing. If the cluster are hot removed by from the database
 * the iterator could be invalid and throw exception of cluster not found.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ORecordIteratorClusters<REC extends ORecord> extends OIdentifiableIterator<REC> {
  private static final OLogger logger =
      OLogManager.instance().logger(ORecordIteratorClusters.class);
  protected int[] clusterIds;
  protected int currentClusterIdx;
  protected ORecord currentRecord;
  protected ORID beginRange;
  protected ORID endRange;

  public ORecordIteratorClusters(
      final ODatabaseDocumentInternal iDatabase, final int[] iClusterIds) {
    this(iDatabase, iClusterIds, OStorage.LOCKING_STRATEGY.NONE);
  }

  @Deprecated
  public ORecordIteratorClusters(
      final ODatabaseDocumentInternal iDatabase,
      final int[] iClusterIds,
      final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    super(iDatabase, iLockingStrategy);

    checkForSystemClusters(iDatabase, iClusterIds);

    clusterIds = iClusterIds;

    Arrays.sort(clusterIds);

    config();
  }

  @Deprecated
  protected ORecordIteratorClusters(
      final ODatabaseDocumentInternal iDatabase, final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    super(iDatabase, iLockingStrategy);
  }

  public ORecordIteratorClusters<REC> setRange(final ORID iBegin, final ORID iEnd) {
    final ORID oldBegin = beginRange;
    final ORID oldEnd = endRange;

    beginRange = iBegin;
    endRange = iEnd;

    if ((oldBegin == null ? iBegin == null : oldBegin.equals(iBegin))
        && (oldEnd == null ? iEnd == null : oldEnd.equals(iEnd))) return this;

    if (currentRecord != null && outsideOfTheRange(currentRecord.getIdentity())) {
      currentRecord = null;
    }

    begin();
    return this;
  }

  @Override
  public boolean hasPrevious() {
    checkDirection(false);

    if (currentRecord != null) return true;

    if (limit > -1 && browsedRecords >= limit)
      // LIMIT REACHED
      return false;

    if (browsedRecords >= totalAvailableRecords) return false;

    if (liveUpdated) updateClusterRange();

    ORecord record = getRecord();

    // ITERATE UNTIL THE PREVIOUS GOOD RECORD
    while (currentClusterIdx > -1) {
      while (prevPosition()) {
        currentRecord = readCurrentRecord(record, 0);

        if (currentRecord != null)
          if (include(currentRecord))
            // FOUND
            return true;
      }

      // CLUSTER EXHAUSTED, TRY WITH THE PREVIOUS ONE
      currentClusterIdx--;

      if (currentClusterIdx < 0) break;

      updateClusterRange();
    }

    if (txEntries != null && txEntries.size() - (currentTxEntryPosition + 1) > 0) return true;

    currentRecord = null;
    return false;
  }

  public boolean hasNext() {
    checkDirection(true);

    if (Thread.interrupted())
      // INTERRUPTED
      return false;

    if (currentRecord != null) return true;

    if (limit > -1 && browsedRecords >= limit)
      // LIMIT REACHED
      return false;

    if (browsedRecords >= totalAvailableRecords) return false;

    // COMPUTE THE NUMBER OF RECORDS TO BROWSE
    if (liveUpdated) updateClusterRange();

    ORecord record = getRecord();

    // ITERATE UNTIL THE NEXT GOOD RECORD
    while (currentClusterIdx < clusterIds.length) {
      while (nextPosition()) {
        if (outsideOfTheRange(current)) continue;

        try {
          currentRecord = readCurrentRecord(record, 0);
        } catch (Exception e) {
          if ((e instanceof RuntimeException) && (e instanceof OHighLevelException))
            throw (RuntimeException) e;

          logger.error("Error during read of record", e);

          currentRecord = null;
        }

        if (currentRecord != null)
          if (include(currentRecord))
            // FOUND
            return true;
      }

      // CLUSTER EXHAUSTED, TRY WITH THE NEXT ONE
      currentClusterIdx++;
      if (currentClusterIdx >= clusterIds.length) break;

      updateClusterRange();
    }

    // CHECK IN TX IF ANY
    if (txEntries != null && txEntries.size() - (currentTxEntryPosition + 1) > 0) return true;

    currentRecord = null;
    return false;
  }

  /**
   * Return the element at the current position and move forward the stream to the next position
   * available.
   *
   * @return the next record found, otherwise the NoSuchElementException exception is thrown when no
   *     more records are found.
   */
  @SuppressWarnings("unchecked")
  public REC next() {
    checkDirection(true);

    if (currentRecord != null)
      try {
        // RETURN LAST LOADED RECORD
        return (REC) currentRecord;
      } finally {
        currentRecord = null;
      }

    ORecord record;

    // MOVE FORWARD IN THE CURRENT CLUSTER
    while (hasNext()) {
      if (currentRecord != null)
        try {
          // RETURN LAST LOADED RECORD
          return (REC) currentRecord;
        } finally {
          currentRecord = null;
        }

      record = getTransactionEntry();
      if (record == null) record = readCurrentRecord(null, +1);

      if (record != null)
        // FOUND
        if (include(record)) return (REC) record;
    }

    record = getTransactionEntry();
    if (record != null) return (REC) record;

    throw new NoSuchElementException(
        "Direction: forward, last position was: "
            + current
            + ", range: "
            + beginRange
            + "-"
            + endRange);
  }

  /**
   * Return the element at the current position and move backward the stream to the previous
   * position available.
   *
   * @return the previous record found, otherwise the NoSuchElementException exception is thrown
   *     when no more records are found.
   */
  @SuppressWarnings("unchecked")
  @Override
  public REC previous() {
    checkDirection(false);

    if (currentRecord != null)
      try {
        // RETURN LAST LOADED RECORD
        return (REC) currentRecord;
      } finally {
        currentRecord = null;
      }

    ORecord record = getRecord();

    // MOVE BACKWARD IN THE CURRENT CLUSTER
    while (hasPrevious()) {
      if (currentRecord != null)
        try {
          // RETURN LAST LOADED RECORD
          return (REC) currentRecord;
        } finally {
          currentRecord = null;
        }

      record = getTransactionEntry();
      if (record == null) record = readCurrentRecord(null, -1);

      if (record != null)
        // FOUND
        if (include(record)) return (REC) record;
    }

    record = getTransactionEntry();
    if (record != null) return (REC) record;

    return null;
  }

  /**
   * Move the iterator to the begin of the range. If no range was specified move to the first record
   * of the cluster.
   *
   * @return The object itself
   */
  @Override
  public ORecordIteratorClusters<REC> begin() {
    if (clusterIds.length == 0) return this;

    browsedRecords = 0;
    currentClusterIdx = 0;
    current.setClusterId(clusterIds[currentClusterIdx]);

    updateClusterRange();

    resetCurrentPosition();
    nextPosition();

    final ORecord record = getRecord();
    currentRecord = readCurrentRecord(record, 0);

    if (currentRecord != null && !include(currentRecord)) {
      currentRecord = null;
      hasNext();
    }

    return this;
  }

  /**
   * Move the iterator to the end of the range. If no range was specified move to the last record of
   * the cluster.
   *
   * @return The object itself
   */
  @Override
  public ORecordIteratorClusters<REC> last() {
    if (clusterIds.length == 0) return this;

    browsedRecords = 0;
    currentClusterIdx = clusterIds.length - 1;

    updateClusterRange();

    current.setClusterId(clusterIds[currentClusterIdx]);

    resetCurrentPosition();
    prevPosition();

    final ORecord record = getRecord();
    currentRecord = readCurrentRecord(record, 0);

    if (currentRecord != null && !include(currentRecord)) {
      currentRecord = null;
      hasPrevious();
    }

    return this;
  }

  /**
   * Tell to the iterator that the upper limit must be checked at every cycle. Useful when
   * concurrent deletes or additions change the size of the cluster while you're browsing it.
   * Default is false.
   *
   * @param iLiveUpdated True to activate it, otherwise false (default)
   * @see #isLiveUpdated()
   */
  @Override
  public ORecordIteratorClusters<REC> setLiveUpdated(boolean iLiveUpdated) {
    super.setLiveUpdated(iLiveUpdated);

    if (iLiveUpdated) {
      firstClusterEntry = 0;
      lastClusterEntry = Long.MAX_VALUE;
    } else {
      updateClusterRange();
    }

    return this;
  }

  public ORID getBeginRange() {
    return beginRange;
  }

  public ORID getEndRange() {
    return endRange;
  }

  public int[] getClusterIds() {
    return clusterIds;
  }

  @Override
  public String toString() {
    return String.format(
        "ORecordIteratorCluster.clusters(%s).currentRecord(%s).range(%s-%s)",
        Arrays.toString(clusterIds), currentRecord, beginRange, endRange);
  }

  protected boolean include(final ORecord iRecord) {
    return true;
  }

  protected void updateClusterRange() {
    if (clusterIds.length == 0) return;

    // ADJUST IDX CHECKING BOUNDARIES
    if (currentClusterIdx >= clusterIds.length) currentClusterIdx = clusterIds.length - 1;
    else if (currentClusterIdx < 0) currentClusterIdx = 0;

    current.setClusterId(clusterIds[currentClusterIdx]);
    final long[] range = database.getClusterDataRange(current.getClusterId());

    if (beginRange != null
        && beginRange.getClusterId() == current.getClusterId()
        && beginRange.getClusterPosition() > range[0])
      firstClusterEntry = beginRange.getClusterPosition();
    else firstClusterEntry = range[0];

    if (endRange != null
        && endRange.getClusterId() == current.getClusterId()
        && endRange.getClusterPosition() < range[1])
      lastClusterEntry = endRange.getClusterPosition();
    else lastClusterEntry = range[1];

    resetCurrentPosition();
  }

  protected void config() {
    if (clusterIds.length == 0) return;

    currentClusterIdx = 0; // START FROM THE FIRST CLUSTER

    updateClusterRange();

    totalAvailableRecords = database.countClusterElements(clusterIds);

    txEntries = database.getTransaction().getNewRecordEntriesByClusterIds(clusterIds);

    if (txEntries != null)
      // ADJUST TOTAL ELEMENT BASED ON CURRENT TRANSACTION'S ENTRIES
      for (ORecordOperation entry : txEntries) {
        if (!entry.getRecord().getIdentity().isPersistent()
            && entry.type != ORecordOperation.DELETED) totalAvailableRecords++;
        else if (entry.type == ORecordOperation.DELETED) totalAvailableRecords--;
      }

    begin();
  }

  private boolean outsideOfTheRange(ORID orid) {
    if (beginRange != null && orid.compareTo(beginRange) < 0) return true;

    if (endRange != null && orid.compareTo(endRange) > 0) return true;

    return false;
  }
}
