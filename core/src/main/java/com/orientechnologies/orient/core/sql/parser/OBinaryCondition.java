/* Generated By:JJTree: Do not edit this line. OBinaryCondition.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.OIndexSearchInfo;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexCandidate;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder;
import com.orientechnologies.orient.core.sql.executor.metadata.OPath;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class OBinaryCondition extends OBooleanExpression {
  protected OExpression left;
  protected OBinaryCompareOperator operator;
  protected OExpression right;

  public OBinaryCondition(int id) {
    super(id);
  }

  public OBinaryCondition(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public boolean evaluate(OIdentifiable currentRecord, OCommandContext ctx) {
    return operator.execute(left.execute(currentRecord, ctx), right.execute(currentRecord, ctx));
  }

  @Override
  public boolean evaluate(OResult currentRecord, OCommandContext ctx) {
    if (left.isFunctionAny()) {
      return evaluateAny(currentRecord, ctx);
    }

    if (left.isFunctionAll()) {
      return evaluateAllFunction(currentRecord, ctx);
    }
    Object leftVal = left.execute(currentRecord, ctx);
    Object rightVal = right.execute(currentRecord, ctx);
    OCollate collate = left.getCollate(currentRecord, ctx);
    if (collate == null) {
      collate = right.getCollate(currentRecord, ctx);
    }
    if (collate != null) {
      leftVal = collate.transform(leftVal);
      rightVal = collate.transform(rightVal);
    }
    return operator.execute(leftVal, rightVal);
  }

  private boolean evaluateAny(OResult currentRecord, OCommandContext ctx) {
    for (String s : currentRecord.getPropertyNames()) {
      Object leftVal = currentRecord.getProperty(s);
      Object rightVal = right.execute(currentRecord, ctx);

      // TODO collate

      if (operator.execute(leftVal, rightVal)) {
        return true;
      }
    }
    return false;
  }

  private boolean evaluateAllFunction(OResult currentRecord, OCommandContext ctx) {
    for (String s : currentRecord.getPropertyNames()) {
      Object leftVal = currentRecord.getProperty(s);
      Object rightVal = right.execute(currentRecord, ctx);

      // TODO collate

      if (!operator.execute(leftVal, rightVal)) {
        return false;
      }
    }
    return true;
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    left.toString(params, builder);
    builder.append(" ");
    builder.append(operator.toString());
    builder.append(" ");
    right.toString(params, builder);
  }

  public void toGenericStatement(StringBuilder builder) {
    left.toGenericStatement(builder);
    builder.append(" ");
    operator.toGenericStatement(builder);
    builder.append(" ");
    right.toGenericStatement(builder);
  }

  protected boolean supportsBasicCalculation() {
    if (!operator.supportsBasicCalculation()) {
      return false;
    }
    return left.supportsBasicCalculation() && right.supportsBasicCalculation();
  }

  @Override
  protected int getNumberOfExternalCalculations() {
    int total = 0;
    if (!operator.supportsBasicCalculation()) {
      total++;
    }
    if (!left.supportsBasicCalculation()) {
      total++;
    }
    if (!right.supportsBasicCalculation()) {
      total++;
    }
    return total;
  }

  @Override
  protected List<Object> getExternalCalculationConditions() {
    List<Object> result = new ArrayList<Object>();
    if (!operator.supportsBasicCalculation()) {
      result.add(this);
    }
    if (!left.supportsBasicCalculation()) {
      result.add(left);
    }
    if (!right.supportsBasicCalculation()) {
      result.add(right);
    }
    return result;
  }

  public OBinaryCondition isIndexedFunctionCondition(
      OClass iSchemaClass, ODatabaseDocumentInternal database) {
    if (left.isIndexedFunctionCal()) {
      return this;
    }
    return null;
  }

  public long estimateIndexed(OFromClause target, OCommandContext context) {
    return left.estimateIndexedFunction(
        target, context, operator, right.execute((OResult) null, context));
  }

  public Iterable<OIdentifiable> executeIndexedFunction(
      OFromClause target, OCommandContext context) {
    return left.executeIndexedFunction(
        target, context, operator, right.execute((OResult) null, context));
  }

  /**
   * tests if current expression involves an indexed funciton AND that function can also be executed
   * without using the index
   *
   * @param target the query target
   * @param context the execution context
   * @return true if current expression involves an indexed function AND that function can be used
   *     on this target, false otherwise
   */
  public boolean canExecuteIndexedFunctionWithoutIndex(
      OFromClause target, OCommandContext context) {
    return left.canExecuteIndexedFunctionWithoutIndex(
        target, context, operator, right.execute((OResult) null, context));
  }

  /**
   * tests if current expression involves an indexed function AND that function can be used on this
   * target
   *
   * @param target the query target
   * @param context the execution context
   * @return true if current expression involves an indexed function AND that function can be used
   *     on this target, false otherwise
   */
  public boolean allowsIndexedFunctionExecutionOnTarget(
      OFromClause target, OCommandContext context) {
    return left.allowsIndexedFunctionExecutionOnTarget(
        target, context, operator, right.execute((OResult) null, context));
  }

  /**
   * tests if current expression involves an indexed function AND the function has also to be
   * executed after the index search. In some cases, the index search is accurate, so this condition
   * can be excluded from further evaluation. In other cases the result from the index is a superset
   * of the expected result, so the function has to be executed anyway for further filtering
   *
   * @param target the query target
   * @param context the execution context
   * @return true if current expression involves an indexed function AND the function has also to be
   *     executed after the index search.
   */
  public boolean executeIndexedFunctionAfterIndexSearch(
      OFromClause target, OCommandContext context) {
    return left.executeIndexedFunctionAfterIndexSearch(
        target, context, operator, right.execute((OResult) null, context));
  }

  public List<OBinaryCondition> getIndexedFunctionConditions(
      OClass iSchemaClass, ODatabaseDocumentInternal database) {
    if (left.isIndexedFunctionCal()) {
      return Collections.singletonList(this);
    }
    return null;
  }

  @Override
  public boolean needsAliases(Set<String> aliases) {
    if (left.needsAliases(aliases)) {
      return true;
    }
    if (right.needsAliases(aliases)) {
      return true;
    }
    return false;
  }

  @Override
  public OBinaryCondition copy() {
    OBinaryCondition result = new OBinaryCondition(-1);
    result.left = left.copy();
    result.operator = (OBinaryCompareOperator) operator.copy();
    result.right = right.copy();
    return result;
  }

  @Override
  public void extractSubQueries(SubQueryCollector collector) {
    left.extractSubQueries(collector);
    right.extractSubQueries(collector);
  }

  @Override
  public boolean refersToParent() {
    return left.refersToParent() || right.refersToParent();
  }

  @Override
  public Optional<OUpdateItem> transformToUpdateItem() {
    if (!checkCanTransformToUpdate()) {
      return Optional.empty();
    }
    if (operator instanceof OEqualsCompareOperator) {
      OUpdateItem result = new OUpdateItem(-1);
      result.operator = OUpdateItem.OPERATOR_EQ;
      OBaseExpression baseExp = ((OBaseExpression) left.mathExpression);
      result.left = baseExp.getIdentifier().suffix.getIdentifier().copy();
      result.leftModifier = baseExp.modifier == null ? null : baseExp.modifier.copy();
      result.right = right.copy();
      return Optional.of(result);
    }
    return super.transformToUpdateItem();
  }

  private boolean checkCanTransformToUpdate() {
    if (left == null
        || left.mathExpression == null
        || !(left.mathExpression instanceof OBaseExpression)) {
      return false;
    }
    OBaseExpression base = (OBaseExpression) left.mathExpression;
    if (base.getIdentifier() == null
        || base.getIdentifier().suffix == null
        || base.getIdentifier().suffix.getIdentifier() == null) {
      return false;
    }
    return true;
  }

  public OExpression getLeft() {
    return left;
  }

  public OBinaryCompareOperator getOperator() {
    return operator;
  }

  public OExpression getRight() {
    return right;
  }

  public void setLeft(OExpression left) {
    this.left = left;
  }

  public void setOperator(OBinaryCompareOperator operator) {
    this.operator = operator;
  }

  public void setRight(OExpression right) {
    this.right = right;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OBinaryCondition that = (OBinaryCondition) o;

    if (left != null ? !left.equals(that.left) : that.left != null) return false;
    if (operator != null ? !operator.equals(that.operator) : that.operator != null) return false;
    if (right != null ? !right.equals(that.right) : that.right != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = left != null ? left.hashCode() : 0;
    result = 31 * result + (operator != null ? operator.hashCode() : 0);
    result = 31 * result + (right != null ? right.hashCode() : 0);
    return result;
  }

  @Override
  public List<String> getMatchPatternInvolvedAliases() {
    List<String> leftX = left.getMatchPatternInvolvedAliases();
    List<String> rightX = right.getMatchPatternInvolvedAliases();
    if (leftX == null) {
      return rightX;
    }
    if (rightX == null) {
      return leftX;
    }

    List<String> result = new ArrayList<String>();
    result.addAll(leftX);
    result.addAll(rightX);
    return result;
  }

  @Override
  public void translateLuceneOperator() {
    if (operator instanceof OLuceneOperator) {
      OExpression newLeft = new OExpression(-1);
      newLeft.mathExpression = new OBaseExpression(-1);
      OBaseIdentifier identifirer = new OBaseIdentifier(-1);
      ((OBaseExpression) newLeft.mathExpression).setIdentifier(identifirer);
      identifirer.levelZero = new OLevelZeroIdentifier(-1);
      OFunctionCall function = new OFunctionCall(-1);
      identifirer.levelZero.functionCall = function;
      function.name = new OIdentifier("search_fields");
      function.params = new ArrayList<>();
      function.params.add(fieldNamesToStrings(left));
      function.params.add(right);
      left = newLeft;

      operator = new OEqualsCompareOperator(-1);
      right = new OExpression(-1);
      right.booleanValue = true;
    }
  }

  private OExpression fieldNamesToStrings(OExpression left) {
    if (left.isBaseIdentifier()) {
      OIdentifier identifier =
          ((OBaseExpression) left.mathExpression).getIdentifier().suffix.getIdentifier();
      OCollection newColl = new OCollection(-1);
      newColl.expressions = new ArrayList<>();
      newColl.expressions.add(identifierToStringExpr(identifier));
      OExpression result = new OExpression(-1);
      OBaseExpression newBase = new OBaseExpression(-1);
      result.mathExpression = newBase;
      OBaseIdentifier newIdentifier = new OBaseIdentifier(-1);
      newIdentifier.levelZero = new OLevelZeroIdentifier(-1);
      newIdentifier.levelZero.collection = newColl;
      newBase.setIdentifier(newIdentifier);
      return result;
    } else if (left.mathExpression instanceof OBaseExpression) {
      OBaseExpression base = (OBaseExpression) left.mathExpression;
      if (base.getIdentifier() != null
          && base.getIdentifier().levelZero != null
          && base.getIdentifier().levelZero.collection != null) {
        OCollection coll = base.getIdentifier().levelZero.collection;

        OCollection newColl = new OCollection(-1);
        newColl.expressions = new ArrayList<>();

        for (OExpression exp : coll.expressions) {
          if (exp.isBaseIdentifier()) {
            OIdentifier identifier =
                ((OBaseExpression) exp.mathExpression).getIdentifier().suffix.getIdentifier();
            OExpression val = identifierToStringExpr(identifier);
            newColl.expressions.add(val);
          } else {
            throw new OCommandExecutionException(
                "Cannot execute because of invalid LUCENE expression");
          }
        }
        OExpression result = new OExpression(-1);
        OBaseExpression newBase = new OBaseExpression(-1);
        result.mathExpression = newBase;
        OBaseIdentifier newIdentifier = new OBaseIdentifier(-1);
        newIdentifier.levelZero = new OLevelZeroIdentifier(-1);
        newIdentifier.levelZero.collection = newColl;
        newBase.setIdentifier(newIdentifier);
        return result;
      }
    }
    throw new OCommandExecutionException("Cannot execute because of invalid LUCENE expression");
  }

  private OExpression identifierToStringExpr(OIdentifier identifier) {
    OBaseExpression bexp = new OBaseExpression(identifier.getStringValue());

    OExpression result = new OExpression(-1);
    result.mathExpression = bexp;
    return result;
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    result.setProperty("left", left.serialize());
    result.setProperty("operator", operator.getClass().getName());
    result.setProperty("right", right.serialize());
    return result;
  }

  public void deserialize(OResult fromResult) {
    left = new OExpression(-1);
    left.deserialize(fromResult.getProperty("left"));
    try {
      operator =
          (OBinaryCompareOperator)
              Class.forName(String.valueOf(fromResult.getProperty("operator"))).newInstance();
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
    right = new OExpression(-1);
    right.deserialize(fromResult.getProperty("right"));
  }

  @Override
  public boolean isCacheable() {
    return left.isCacheable() && right.isCacheable();
  }

  @Override
  public OBooleanExpression rewriteIndexChainsAsSubqueries(OCommandContext ctx, OClass clazz) {
    if (operator instanceof OEqualsCompareOperator
        && right.isEarlyCalculated(ctx)
        && left.isIndexChain(ctx, clazz)) {
      OInCondition result = new OInCondition(-1);

      result.left = new OExpression(-1);
      OBaseExpression base = new OBaseExpression(-1);
      OBaseIdentifier identifier = new OBaseIdentifier(-1);
      identifier.suffix = new OSuffixIdentifier(-1);
      identifier.suffix.setIdentifier(
          ((OBaseExpression) left.mathExpression).getIdentifier().suffix.getIdentifier());
      base.setIdentifier(identifier);
      result.left.mathExpression = base;

      result.operator = new OInOperator(-1);

      OClass nextClazz =
          clazz
              .getProperty(base.getIdentifier().suffix.getIdentifier().getStringValue())
              .getLinkedClass();
      result.rightStatement =
          indexChainToStatement(
              ((OBaseExpression) left.mathExpression).modifier, nextClazz, right, ctx);
      return result;
    }
    return this;
  }

  public static OSelectStatement indexChainToStatement(
      OModifier modifier, OClass clazz, OExpression right, OCommandContext ctx) {
    OClass queryClass = clazz;

    OSelectStatement result = new OSelectStatement(-1);
    result.target = new OFromClause(-1);
    result.target.setItem(new OFromItem(-1));
    result.target.getItem().identifier = new OIdentifier(queryClass.getName());

    result.whereClause = new OWhereClause(-1);
    OBinaryCondition base = new OBinaryCondition(-1);
    result.whereClause.baseExpression = new ONotBlock(-1);
    ((ONotBlock) result.whereClause.baseExpression).sub = base;
    ((ONotBlock) result.whereClause.baseExpression).negate = false;

    base.left = new OExpression(-1);
    base.left.mathExpression = new OBaseExpression(-1);
    ((OBaseExpression) base.left.mathExpression)
        .setIdentifier(new OBaseIdentifier(modifier.suffix.getIdentifier()));
    ((OBaseExpression) base.left.mathExpression).modifier =
        modifier.next == null ? null : modifier.next.copy();

    base.operator = new OEqualsCompareOperator(-1);
    base.right = right.copy();

    return result;
  }

  public Optional<OIndexCandidate> findIndex(OIndexFinder info, OCommandContext ctx) {
    Optional<OPath> path = left.getPath();
    if (path.isPresent()) {
      OPath p = path.get();
      if (right.isEarlyCalculated(ctx)) {
        Object value = right.execute((OResult) null, ctx);
        if (operator instanceof OEqualsCompareOperator) {
          return info.findExactIndex(p, value, ctx);
        } else if (operator instanceof OContainsKeyOperator) {
          return info.findByKeyIndex(p, value, ctx);
        } else if (operator.isRangeOperator()) {
          return info.findAllowRangeIndex(p, operator.getOperation(), value, ctx);
        }
      }
    }

    return Optional.empty();
  }

  public boolean isIndexAware(OIndexSearchInfo info, OCommandContext ctx) {
    if (left.isBaseIdentifier()) {
      if (info.getField().equals(left.getDefaultAlias().getStringValue())) {
        if (right.isEarlyCalculated(info.getCtx())) {
          if (operator instanceof OEqualsCompareOperator) {
            Object vl = this.right.execute((OResult) null, ctx);
            if (vl instanceof Collection<?>) {
              return !((Collection) vl).isEmpty();
            }
            return true;
          } else if (operator instanceof OContainsKeyOperator
              && info.isMap()
              && info.isIndexByKey()) {
            return true;
          } else if (info.allowsRange() && operator.isRangeOperator()) {
            return true;
          }
          return false;
        }
      }
    }
    return false;
  }

  @Override
  public boolean createRangeWith(OBooleanExpression match) {
    if (!(match instanceof OBinaryCondition)) {
      return false;
    }
    OBinaryCondition metchingCondition = (OBinaryCondition) match;
    if (!metchingCondition.getLeft().equals(this.getLeft())) {
      return false;
    }
    OBinaryCompareOperator leftOperator = metchingCondition.getOperator();
    OBinaryCompareOperator rightOperator = this.getOperator();
    if (leftOperator instanceof OGeOperator || leftOperator instanceof OGtOperator) {
      return rightOperator instanceof OLeOperator || rightOperator instanceof OLtOperator;
    }
    if (leftOperator instanceof OLeOperator || leftOperator instanceof OLtOperator) {
      return rightOperator instanceof OGeOperator || rightOperator instanceof OGtOperator;
    }
    return false;
  }

  @Override
  public OExpression resolveKeyFrom(OBinaryCondition additional) {
    OBinaryCompareOperator operator = getOperator();
    if ((operator instanceof OEqualsCompareOperator)
        || (operator instanceof OGtOperator)
        || (operator instanceof OGeOperator)
        || (operator instanceof OContainsKeyOperator)
        || (operator instanceof OContainsValueOperator)) {
      return getRight();
    } else if (additional != null) {
      return additional.getRight();
    } else {
      return null;
      //      throw new UnsupportedOperationException("Cannot execute index query with " + this);
    }
  }

  @Override
  public OExpression resolveKeyTo(OBinaryCondition additional) {
    OBinaryCompareOperator operator = this.getOperator();
    if ((operator instanceof OEqualsCompareOperator)
        || (operator instanceof OLtOperator)
        || (operator instanceof OLeOperator)
        || (operator instanceof OContainsKeyOperator)
        || (operator instanceof OContainsValueOperator)) {
      return getRight();
    } else if (additional != null) {
      return additional.getRight();
    } else {
      return null;
      //      throw new UnsupportedOperationException("Cannot execute index query with " + this);
    }
  }
}
/* JavaCC - OriginalChecksum=99ed1dd2812eb730de8e1931b1764da5 (do not edit this line) */
