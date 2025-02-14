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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;

/**
 * Progress listener for index rebuild.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexRebuildOutputListener implements OProgressListener {
  private static final OLogger logger =
      OLogManager.instance().logger(OIndexRebuildOutputListener.class);
  private long startTime;
  private long lastDump;
  private long lastCounter = 0;
  private boolean rebuild = false;

  private final OIndex idx;

  public OIndexRebuildOutputListener(OIndex idx) {
    this.idx = idx;
  }

  @Override
  public void onBegin(final Object iTask, final long iTotal, final Object iRebuild) {
    startTime = System.currentTimeMillis();
    lastDump = startTime;

    rebuild = (Boolean) iRebuild;
    if (iTotal > 0)
      if (rebuild)
        logger.info(
            "- Rebuilding index %s.%s (estimated %,d items)...",
            idx.getDatabaseName(), idx.getName(), iTotal);
      else
        logger.debug(
            "- Building index %s.%s (estimated %,d items)...",
            idx.getDatabaseName(), idx.getName(), iTotal);
  }

  @Override
  public boolean onProgress(final Object iTask, final long iCounter, final float iPercent) {
    final long now = System.currentTimeMillis();
    if (now - lastDump > 10000) {
      // DUMP EVERY 5 SECONDS FOR LARGE INDEXES
      if (rebuild)
        logger.info(
            "--> %3.2f%% progress, %,d indexed so far (%,d items/sec)",
            iPercent, iCounter, ((iCounter - lastCounter) / 10));
      else
        logger.info(
            "--> %3.2f%% progress, %,d indexed so far (%,d items/sec)",
            iPercent, iCounter, ((iCounter - lastCounter) / 10));
      lastDump = now;
      lastCounter = iCounter;
    }
    return true;
  }

  @Override
  public void onCompletition(final Object iTask, final boolean iSucceed) {
    final long idxSize = idx.getInternal().size();

    if (idxSize > 0)
      if (rebuild)
        logger.info(
            "--> OK, indexed %,d items in %,d ms",
            idxSize, (System.currentTimeMillis() - startTime));
      else
        logger.debug(
            "--> OK, indexed %,d items in %,d ms",
            idxSize, (System.currentTimeMillis() - startTime));
  }
}
