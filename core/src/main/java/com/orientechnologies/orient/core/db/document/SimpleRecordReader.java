package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORawBuffer;

/** @Internal */
public final class SimpleRecordReader implements RecordReader {
  private final boolean prefetchRecords;

  public SimpleRecordReader(boolean prefetchRecords) {
    this.prefetchRecords = prefetchRecords;
  }

  @Override
  public ORawBuffer readRecord(
      RecordFetchMode storage,
      ORecordId rid,
      String fetchPlan,
      boolean ignoreCache,
      final int recordVersion)
      throws ORecordNotFoundException {
    return storage.readRecord(rid, fetchPlan, ignoreCache, prefetchRecords).getResult();
  }
}
