/* Generated By:JJTree: Do not edit this line. ONearOperator.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorFactory;
import java.util.Iterator;
import java.util.Map;

public class ONearOperator extends SimpleNode implements OBinaryCompareOperator {
  private OQueryOperator lowLevelOperator;

  public ONearOperator(int id) {
    super(id);
    initOperator();
  }

  public ONearOperator(OrientSql p, int id) {
    super(p, id);
    initOperator();
  }

  protected void initOperator() {
    Iterator<OQueryOperatorFactory> factories = OSQLEngine.getOperatorFactories();
    while (factories.hasNext()) {
      OQueryOperatorFactory factory = factories.next();
      for (OQueryOperator op : factory.getOperators()) {
        if ("NEAR".equals(op.getKeyword())) {
          lowLevelOperator = op;
        }
      }
    }
  }

  @Override
  public boolean execute(Object left, Object right, OCommandContext ctx) {
    if (lowLevelOperator == null) {
      throw new UnsupportedOperationException(
          toString() + " operator cannot be evaluated in this context");
    } else {
      return lowLevelOperator.evaluate(left, right, ctx);
    }
  }

  @Override
  public String toString() {
    return "NEAR";
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("NEAR");
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("NEAR");
  }

  @Override
  public Operation getOperation() {
    return Operation.FuzzyEq;
  }

  @Override
  public boolean supportsBasicCalculation() {
    return false;
  }

  @Override
  public ONearOperator copy() {
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
/* JavaCC - OriginalChecksum=a79af9beed70f813658f38a0162320e0 (do not edit this line) */
