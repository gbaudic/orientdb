/* Generated By:JJTree: Do not edit this line. OHaSyncClusterStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;

public class OHaSyncClusterStatement extends OSimpleExecStatement {

  public OIdentifier clusterName;
  public boolean modeFull = true;
  public boolean modeMerge = false;

  public OHaSyncClusterStatement(int id) {
    super(id);
  }

  public OHaSyncClusterStatement(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public OResultSet executeSimple(OCommandContext ctx) {
    throw new OCommandExecutionException("Cannot execute HA SYNC CLUSTER, not supported anymore");
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("HA SYNC CLUSTER ");
    clusterName.toString(params, builder);
  }

  @Override
  public void toGenericStatement(Map<Object, Object> params, StringBuilder builder) {
    builder.append("HA SYNC CLUSTER ");
    clusterName.toGenericStatement(params, builder);
  }
}
/* JavaCC - OriginalChecksum=fbf0df8004d889ebc80f39be85008720 (do not edit this line) */
