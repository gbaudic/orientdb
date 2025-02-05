package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORawBuffer;

/** @Internal */
public interface RecordReader {

  ORawBuffer readRecord(
      ORecordId rid, String fetchPlan, boolean ignoreCache, final int recordVersion)
      throws ORecordNotFoundException;
}
