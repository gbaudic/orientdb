package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORawBuffer;

/** @Internal */
public final class LatestVersionRecordReader implements RecordReader {
  @Override
  public ORawBuffer readRecord(
      RecordFetchMode storage,
      ORecordId rid,
      String fetchPlan,
      boolean ignoreCache,
      final int recordVersion)
      throws ORecordNotFoundException {
    return storage
        .readRecordIfVersionIsNotLatest(rid, fetchPlan, ignoreCache, recordVersion)
        .getResult();
  }
}
