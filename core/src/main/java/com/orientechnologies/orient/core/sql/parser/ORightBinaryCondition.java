/* Generated By:JJTree: Do not edit this line. ORightBinaryCondition.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ORightBinaryCondition extends SimpleNode {

  OBinaryCompareOperator operator;

  boolean not = false;
  OInOperator inOperator;

  OExpression right;

  public ORightBinaryCondition(int id) {
    super(id);
  }

  public ORightBinaryCondition(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public ORightBinaryCondition copy() {
    ORightBinaryCondition result = new ORightBinaryCondition(-1);
    result.operator = operator == null ? null : operator.copy();
    result.not = not;
    result.inOperator = inOperator == null ? null : inOperator.copy();
    result.right = right == null ? null : right.copy();
    return result;
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    if (operator != null) {
      builder.append(operator.toString());
      builder.append(" ");
      right.toString(params, builder);
    } else if (inOperator != null) {
      if (not) {
        builder.append("NOT ");
      }
      builder.append("IN ");
      right.toString(params, builder);
    }
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    if (operator != null) {
      if (operator instanceof SimpleNode) {
        ((SimpleNode) operator).toGenericStatement(builder);
      } else {
        builder.append(operator.toString());
      }
      builder.append(" ");
      right.toGenericStatement(builder);
    } else if (inOperator != null) {
      if (not) {
        builder.append("NOT ");
      }
      builder.append("IN ");
      right.toGenericStatement(builder);
    }
  }

  public Object execute(OResult iCurrentRecord, Object elementToFilter, OCommandContext ctx) {
    if (elementToFilter == null) {
      return null;
    }
    Iterator iterator;
    if (elementToFilter instanceof OIdentifiable) {
      iterator = Collections.singleton(elementToFilter).iterator();
    } else if (elementToFilter instanceof Iterable) {
      iterator = ((Iterable) elementToFilter).iterator();
    } else if (elementToFilter instanceof Iterator) {
      iterator = (Iterator) elementToFilter;
    } else {
      iterator = Collections.singleton(elementToFilter).iterator();
    }

    List result = new ArrayList();
    while (iterator.hasNext()) {
      Object element = iterator.next();
      if (matchesFilters(iCurrentRecord, element, ctx)) {
        result.add(element);
      }
    }
    return result;
  }

  private boolean matchesFilters(OResult iCurrentRecord, Object element, OCommandContext ctx) {
    if (operator != null) {
      return operator.execute(element, right.execute(iCurrentRecord, ctx), ctx);
    } else if (inOperator != null) {

      Object rightVal = evaluateRight(iCurrentRecord, ctx);
      if (rightVal == null) {
        return false;
      }
      boolean result = OInCondition.evaluateExpression(element, rightVal);
      if (not) {
        result = !result;
      }
      return result;
    }
    return false;
  }

  public Object evaluateRight(OResult currentRecord, OCommandContext ctx) {
    return right.execute(currentRecord, ctx);
  }

  public boolean needsAliases(Set<String> aliases) {
    if (right != null && right.needsAliases(aliases)) {
      return true;
    }
    return false;
  }

  public void extractSubQueries(SubQueryCollector collector) {
    if (right != null) {
      right.extractSubQueries(collector);
    }
  }

  public boolean refersToParent() {
    if (right != null && right.refersToParent()) {
      return true;
    }
    return false;
  }

  public OResult serialize() {

    OResultInternal result = new OResultInternal();
    result.setProperty("operator", operator.getClass().getName());
    result.setProperty("not", not);
    result.setProperty("in", inOperator != null);
    result.setProperty("right", right.serialize());
    return result;
  }

  public void deserialize(OResult fromResult) {
    try {
      operator =
          (OBinaryCompareOperator)
              Class.forName(String.valueOf(fromResult.getProperty("operator"))).newInstance();
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
    not = fromResult.getProperty("not");
    if (Boolean.TRUE.equals(fromResult.getProperty("in"))) {
      inOperator = new OInOperator(-1);
    }
    right = new OExpression(-1);
    right.deserialize(fromResult.getProperty("right"));
  }

  public void applyRemove(
      Object currentValue, OResultInternal originalRecord, OCommandContext ctx) {
    if (currentValue == null) {
      return;
    }
    if (currentValue instanceof Collection) {
      Iterator it = ((Collection) currentValue).iterator();
      while (it.hasNext()) {
        Object cv = it.next();
        if (this.matchesFilters(originalRecord, cv, ctx)) {
          it.remove();
        }
      }
    } else {
      throw new OCommandExecutionException(
          "Trying to remove elements from "
              + currentValue
              + " ("
              + currentValue.getClass().getSimpleName()
              + ")");
    }
  }
}
/* JavaCC - OriginalChecksum=29d59ae04778eb611547292a27863da4 (do not edit this line) */
