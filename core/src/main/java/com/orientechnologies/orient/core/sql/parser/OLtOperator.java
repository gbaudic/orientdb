/* Generated By:JJTree: Do not edit this line. OLtOperator.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.Map;

public class OLtOperator extends SimpleNode implements OBinaryCompareOperator {
  public OLtOperator(int id) {
    super(id);
  }

  public OLtOperator(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public boolean execute(Object iLeft, Object iRight) {
    if (iLeft == null || iRight == null) {
      return false;
    }
    if (iLeft instanceof Number
        && iRight instanceof Number
        && iLeft.getClass() != iRight.getClass()) {
      Number[] couple = OType.castComparableNumber((Number) iLeft, (Number) iRight);
      iLeft = couple[0];
      iRight = couple[1];
    } else {
      iRight = OType.convert(iRight, iLeft.getClass());
    }
    if (iRight == null) return false;
    return ((Comparable<Object>) iLeft).compareTo(iRight) < 0;
  }

  @Override
  public String toString() {
    return "<";
  }

  @Override
  public void toGenericStatement(Map<Object, Object> params, StringBuilder builder) {
    builder.append("<");
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("<");
  }

  @Override
  public boolean supportsBasicCalculation() {
    return true;
  }

  @Override
  public OLtOperator copy() {
    return this;
  }

  @Override
  public boolean isRangeOperator() {
    return true;
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj.getClass().equals(this.getClass());
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }
}
/* JavaCC - OriginalChecksum=d8e97d52128198b373bb0c272c72de2c (do not edit this line) */
