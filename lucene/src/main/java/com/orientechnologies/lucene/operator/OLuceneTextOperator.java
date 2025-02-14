/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene.operator;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.sql.operator.OQueryTargetOperator;

public class OLuceneTextOperator extends OQueryTargetOperator {
  private static final OLogger logger = OLogManager.instance().logger(OLuceneTextOperator.class);
  public static final String MEMORY_INDEX = "_memoryIndex";

  public OLuceneTextOperator() {
    this("LUCENE", 5, false);
  }

  public OLuceneTextOperator(String iKeyword, int iPrecedence, boolean iLogical) {
    super(iKeyword, iPrecedence, iLogical);
  }
}
