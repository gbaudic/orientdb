/* Generated By:JJTree: Do not edit this line. OInOperator.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class OInOperator extends SimpleNode implements OBinaryCompareOperator {
  public OInOperator(int id) {
    super(id);
  }

  public OInOperator(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public boolean execute(Object left, Object right, OCommandContext ctx) {
    if (left == null) {
      return false;
    }
    if (right instanceof Collection) {
      if (left instanceof Collection) {
        return ((Collection) right).containsAll((Collection) left);
      }
      if (left instanceof Iterable) {
        left = ((Iterable) left).iterator();
      }
      if (left instanceof Iterator) {
        Iterator iterator = (Iterator) left;
        while (iterator.hasNext()) {
          Object next = iterator.next();
          if (!((Collection) right).contains(next)) {
            return false;
          }
        }
      }
      return ((Collection) right).contains(left);
    }
    if (right instanceof Iterable) {
      right = ((Iterable) right).iterator();
    }
    if (right instanceof Iterator) {
      if (left instanceof Iterable) {
        left = ((Iterable) left).iterator();
      }
      Iterator leftIterator = (Iterator) left;
      Iterator rightIterator = (Iterator) right;
      while (leftIterator.hasNext()) {
        Object leftItem = leftIterator.next();
        boolean found = false;
        while (rightIterator.hasNext()) {
          Object rightItem = rightIterator.next();
          if (leftItem != null && leftItem.equals(rightItem)) {
            found = true;
            break;
          }
        }
        if (!found) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("IN");
  }

  public void toGenericStatement(StringBuilder builder) {
    builder.append("IN");
  }

  @Override
  public boolean supportsBasicCalculation() {
    return true;
  }

  @Override
  public OInOperator copy() {
    return this;
  }

  @Override
  public Operation getOperation() {
    return Operation.Eq;
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj.getClass().equals(this.getClass());
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public boolean isInclude() {
    return false;
  }

  @Override
  public boolean isLess() {
    return false;
  }

  @Override
  public boolean isGreater() {
    return false;
  }
}
/* JavaCC - OriginalChecksum=6650a720cb942fa3c4d588ff0f381b3a (do not edit this line) */
