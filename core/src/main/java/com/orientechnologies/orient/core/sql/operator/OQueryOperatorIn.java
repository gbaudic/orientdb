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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemParameter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * IN operator.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OQueryOperatorIn extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorIn() {
    super("IN", 5, false);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(
      final OIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext) {
    if (OMultiValue.isMultiValue(iLeft)) {
      if (iRight instanceof Collection<?>) {
        // AGAINST COLLECTION OF ITEMS
        final Collection<Object> collectionToMatch = (Collection<Object>) iRight;

        boolean found = false;
        for (final Object o1 : OMultiValue.getMultiValueIterable(iLeft, false)) {
          for (final Object o2 : collectionToMatch) {
            if (OQueryOperatorEquals.equals(o1, o2)) {
              found = true;
              break;
            }
          }
        }
        return found;
      } else {
        // AGAINST SINGLE ITEM
        if (iLeft instanceof Set<?>) return ((Set) iLeft).contains(iRight);

        for (final Object o : OMultiValue.getMultiValueIterable(iLeft, false)) {
          if (OQueryOperatorEquals.equals(iRight, o)) return true;
        }
      }
    } else if (OMultiValue.isMultiValue(iRight)) {

      if (iRight instanceof Set<?>) return ((Set) iRight).contains(iLeft);

      for (final Object o : OMultiValue.getMultiValueIterable(iRight, false)) {
        if (OQueryOperatorEquals.equals(iLeft, o)) return true;
      }
    } else if (iLeft.getClass().isArray()) {

      for (final Object o : (Object[]) iLeft) {
        if (OQueryOperatorEquals.equals(iRight, o)) return true;
      }
    } else if (iRight.getClass().isArray()) {

      for (final Object o : (Object[]) iRight) {
        if (OQueryOperatorEquals.equals(iLeft, o)) return true;
      }
    }

    return iLeft.equals(iRight);
  }

  protected List<ORID> addRangeResults(final Iterable<?> ridCollection, final int ridSize) {
    if (ridCollection == null) return null;

    List<ORID> rids = null;
    for (Object rid : ridCollection) {
      if (rid instanceof OSQLFilterItemParameter)
        rid = ((OSQLFilterItemParameter) rid).getValue(null, null, null);

      if (rid instanceof OIdentifiable) {
        final ORID r = ((OIdentifiable) rid).getIdentity();
        if (r.isPersistent()) {
          if (rids == null)
            // LAZY CREATE IT
            rids = new ArrayList<ORID>(ridSize);
          rids.add(r);
        }
      }
    }
    return rids;
  }
}
