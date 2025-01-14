package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.command.script.OAbstractScriptExecutor;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.executor.ORetryExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OScriptExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.RetryStep;
import com.orientechnologies.orient.core.sql.parser.OBeginStatement;
import com.orientechnologies.orient.core.sql.parser.OCommitStatement;
import com.orientechnologies.orient.core.sql.parser.OLetStatement;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSet;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Created by tglman on 25/01/17. */
public class OSqlScriptExecutor extends OAbstractScriptExecutor {

  public OSqlScriptExecutor() {
    super("SQL");
  }

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Object... args)
      throws OCommandSQLParsingException, OCommandExecutionException {

    if (!script.trim().endsWith(";")) {
      script += ";";
    }
    List<OStatement> statements = OSQLEngine.parseScript(script, database);

    OCommandContext scriptContext = new OBasicCommandContext(database);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    scriptContext.setInputParameters(params);

    return executeInternal(statements, scriptContext);
  }

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Map params) {
    if (!script.trim().endsWith(";")) {
      script += ";";
    }
    List<OStatement> statements = OSQLEngine.parseScript(script, database);

    OCommandContext scriptContext = new OBasicCommandContext(database);

    scriptContext.setInputParameters(params);

    return executeInternal(statements, scriptContext);
  }

  private OResultSet executeInternal(List<OStatement> statements, OCommandContext scriptContext) {
    OScriptExecutionPlan plan = new OScriptExecutionPlan();

    plan.setStatement(
        statements.stream().map(OStatement::toString).collect(Collectors.joining(";")));

    List<OStatement> lastRetryBlock = new ArrayList<>();
    int nestedTxLevel = 0;

    for (OStatement stm : statements) {
      if (stm.getOriginalStatement() == null) {
        stm.setOriginalStatement(stm.toString());
      }
      if (stm instanceof OBeginStatement) {
        nestedTxLevel++;
      }

      if (nestedTxLevel <= 0) {
        plan.chain(stm, false, scriptContext);
      } else {
        lastRetryBlock.add(stm);
      }

      if (stm instanceof OCommitStatement && nestedTxLevel > 0) {
        nestedTxLevel--;
        if (nestedTxLevel == 0) {
          if (((OCommitStatement) stm).getRetry() != null) {
            int nRetries = ((OCommitStatement) stm).getRetry().getValue().intValue();
            if (nRetries <= 0) {
              throw new OCommandExecutionException("Invalid retry number: " + nRetries);
            }

            RetryStep step =
                new RetryStep(
                    lastRetryBlock,
                    nRetries,
                    ((OCommitStatement) stm).getElseStatements(),
                    ((OCommitStatement) stm).getElseFail(),
                    scriptContext,
                    false);
            ORetryExecutionPlan retryPlan = new ORetryExecutionPlan();
            retryPlan.chain(step);
            plan.chain(retryPlan, false, scriptContext);
            lastRetryBlock = new ArrayList<>();
          } else {
            for (OStatement statement : lastRetryBlock) {
              plan.chain(statement, false, scriptContext);
            }
            lastRetryBlock = new ArrayList<>();
          }
        }
      }

      if (stm instanceof OLetStatement) {
        scriptContext.declareScriptVariable(((OLetStatement) stm).getName().getStringValue());
      }
    }
    if (!lastRetryBlock.isEmpty()) {
      for (OStatement statement : lastRetryBlock) {
        plan.chain(statement, false, scriptContext);
      }
    }
    return new OLocalResultSet(plan, scriptContext);
  }

  @Override
  public Object executeFunction(
      OCommandContext context, final String functionName, final Map<Object, Object> iArgs) {

    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) context.getDatabase();

    OFunction function = db.getMetadata().getFunctionLibrary().getFunction(functionName);

    db.checkSecurity(ORule.ResourceGeneric.FUNCTION, ORole.PERMISSION_READ, function.getName());
    final OScriptManager scriptManager = db.getSharedContext().getOrientDB().getScriptManager();
    final Object[] args = iArgs == null ? null : iArgs.values().toArray();
    return execute(db, scriptManager.getFunctionInvoke(function, args), iArgs);
  }
}
