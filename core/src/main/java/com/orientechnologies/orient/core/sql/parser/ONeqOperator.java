/* Generated By:JJTree: Do not edit this line. ONeqOperator.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquals;
import java.util.Map;

public class ONeqOperator extends SimpleNode implements OBinaryCompareOperator {
  public ONeqOperator(int id) {
    super(id);
  }

  public ONeqOperator(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public boolean execute(Object left, Object right) {
    return !OQueryOperatorEquals.equals(left, right);
  }

  @Override
  public String toString() {
    return "<>";
  }

  @Override
  public void toGenericStatement(Map<Object, Object> params, StringBuilder builder) {
    builder.append("<>");
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("<>");
  }

  @Override
  public boolean supportsBasicCalculation() {
    return true;
  }

  @Override
  public ONeqOperator copy() {
    return this;
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
/* JavaCC - OriginalChecksum=588c4112ae7d2c83239f97ab0d2d5989 (do not edit this line) */
