/* Generated By:JJTree: Do not edit this line. OLeOperator.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.Map;

public class OLeOperator extends SimpleNode implements OBinaryCompareOperator {
  private static final OLogger logger = OLogManager.instance().logger(OLeOperator.class);

  public OLeOperator(int id) {
    super(id);
  }

  public OLeOperator(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public boolean execute(Object iLeft, Object iRight, OCommandContext ctx) {
    if (iLeft == iRight) {
      return true;
    }
    if (iLeft == null || iRight == null) {
      return false;
    }
    if (iLeft.getClass() != iRight.getClass()
        && iLeft instanceof Number
        && iRight instanceof Number) {
      Number[] couple = OType.castComparableNumber((Number) iLeft, (Number) iRight);
      iLeft = couple[0];
      iRight = couple[1];
    } else {
      try {
        iRight = OType.convert(iRight, iLeft.getClass());
      } catch (RuntimeException e) {
        iRight = null;
        // Can't convert to the target value do nothing will return false
        logger.warn("Issue converting value to target type, ignoring value", e);
      }
    }
    if (iRight == null) return false;
    return ((Comparable<Object>) iLeft).compareTo(iRight) <= 0;
  }

  @Override
  public String toString() {
    return "<=";
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("<=");
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("<=");
  }

  @Override
  public boolean supportsBasicCalculation() {
    return true;
  }

  @Override
  public OLeOperator copy() {
    return this;
  }

  @Override
  public boolean isRange() {
    return true;
  }

  @Override
  public Operation getOperation() {
    return Operation.Le;
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
    return true;
  }

  @Override
  public boolean isLess() {
    return true;
  }

  @Override
  public boolean isGreater() {
    return false;
  }
}
/* JavaCC - OriginalChecksum=8b3232c970fd654af947274a5f384a93 (do not edit this line) */
