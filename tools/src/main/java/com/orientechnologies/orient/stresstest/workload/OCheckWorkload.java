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
package com.orientechnologies.orient.stresstest.workload;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.stresstest.OStressTesterSettings;

/**
 * Supports checking of the workload.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OCheckWorkload {
  void check(OStressTesterSettings settings, OrientDB context);
}
