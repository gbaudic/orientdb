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
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.Comparator;

/**
 * Base interface for identifiable objects. This abstraction is required to use ORID and ORecord in
 * many points.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OIdentifiable extends Comparable<OIdentifiable>, Comparator<OIdentifiable> {
  /**
   * Returns the record identity.
   *
   * @return ORID instance
   */
  ORID getIdentity();

  /**
   * Returns the record instance.
   *
   * @return ORecord instance
   */
  <T extends ORecord> T getRecord();

  /**
   * <p> Use {@link ODatabaseSession#lock(ORID)} instead.</p>
   */
  @Deprecated(forRemoval = true)
  void lock(boolean iExclusive);

  @Deprecated(forRemoval = true)
  boolean isLocked();

  @Deprecated(forRemoval = true)
  OStorage.LOCKING_STRATEGY lockingStrategy();

  /**
   * <p> Use {@link ODatabaseSession#unlock(ORID)} instead.</p>
   */
  @Deprecated(forRemoval = true)
  void unlock();
}
