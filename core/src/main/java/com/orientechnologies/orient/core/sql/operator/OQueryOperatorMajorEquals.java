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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OBinaryField;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * MAJOR EQUALS operator.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OQueryOperatorMajorEquals extends OQueryOperatorEqualityNotNulls {

  private boolean binaryEvaluate = false;

  public OQueryOperatorMajorEquals() {
    super(">=", 5, false);
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) binaryEvaluate = db.getSerializer().getSupportBinaryEvaluate();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(
      final OIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext) {
    final Object right = OType.convert(iRight, iLeft.getClass());
    if (right == null) return false;
    return ((Comparable<Object>) iLeft).compareTo(right) >= 0;
  }

  @Override
  public boolean evaluate(
      final OBinaryField iFirstField,
      final OBinaryField iSecondField,
      OCommandContext iContext,
      final ODocumentSerializer serializer) {
    return serializer.getComparator().compare(iFirstField, iSecondField) >= 0;
  }

  @Override
  public boolean isSupportingBinaryEvaluate() {
    return binaryEvaluate;
  }
}
