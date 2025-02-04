package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;

/** @Internal */
public interface RecordReader {
  public interface RecordFetchMode {

    OStorageOperationResult<ORawBuffer> readRecord(
        ORecordId iRid, String iFetchPlan, boolean iIgnoreCache, boolean prefetchRecords);

    OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(
        ORecordId rid, String fetchPlan, boolean ignoreCache, int recordVersion)
        throws ORecordNotFoundException;
  }

  ORawBuffer readRecord(
      RecordFetchMode storage,
      ORecordId rid,
      String fetchPlan,
      boolean ignoreCache,
      final int recordVersion)
      throws ORecordNotFoundException;
}
