/* Generated By:JJTree: Do not edit this line. OCreateFunctionStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OCreateFunctionStatement extends OSimpleExecStatement {
  protected OIdentifier name;
  protected String codeQuoted;
  protected String code;

  protected List<OIdentifier> parameters;
  protected Boolean idempotent;
  protected OIdentifier language;

  public OCreateFunctionStatement(int id) {
    super(id);
  }

  public OCreateFunctionStatement(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public OResultSet executeSimple(OCommandContext ctx) {
    ODatabase database = ctx.getDatabase();
    final OFunction f =
        database.getMetadata().getFunctionLibrary().createFunction(name.getStringValue());
    f.setCode(code);
    f.setIdempotent(Boolean.TRUE.equals(idempotent));
    if (parameters != null)
      f.setParameters(
          parameters.stream().map(x -> x.getStringValue()).collect(Collectors.toList()));
    if (language != null) f.setLanguage(language.getStringValue());
    f.save();
    ORID functionId = f.getId();
    OResultInternal result = new OResultInternal();
    result.setProperty("operation", "create function");
    result.setProperty("functionName", name.getStringValue());
    result.setProperty("finalId", functionId);

    OInternalResultSet rs = new OInternalResultSet();
    rs.add(result);
    return rs;
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("CREATE FUNCTION ");
    name.toString(params, builder);
    builder.append(" ");
    builder.append(codeQuoted);
    if (parameters != null) {
      boolean first = true;
      builder.append(" PARAMETERS [");
      for (OIdentifier param : parameters) {
        if (!first) {
          builder.append(", ");
        }
        param.toString(params, builder);
        first = false;
      }
      builder.append("]");
    }
    if (idempotent != null) {
      builder.append(" IDEMPOTENT ");
      builder.append(idempotent ? "true" : "false");
    }
    if (language != null) {
      builder.append(" LANGUAGE ");
      language.toString(params, builder);
    }
  }

  @Override
  public void toGenericStatement(Map<Object, Object> params, StringBuilder builder) {
    builder.append("CREATE FUNCTION ");
    name.toGenericStatement(params, builder);
    builder.append(" ");
    builder.append(codeQuoted);
    if (parameters != null) {
      boolean first = true;
      builder.append(" PARAMETERS [");
      for (OIdentifier param : parameters) {
        if (!first) {
          builder.append(", ");
        }
        param.toGenericStatement(params, builder);
        first = false;
      }
      builder.append("]");
    }
    if (idempotent != null) {
      builder.append(" IDEMPOTENT ");
      builder.append(idempotent ? "true" : "false");
    }
    if (language != null) {
      builder.append(" LANGUAGE ");
      language.toGenericStatement(params, builder);
    }
  }

  @Override
  public OCreateFunctionStatement copy() {
    OCreateFunctionStatement result = new OCreateFunctionStatement(-1);
    result.name = name == null ? null : name.copy();
    result.codeQuoted = codeQuoted;
    result.code = code;
    result.parameters =
        parameters == null
            ? null
            : parameters.stream().map(x -> x.copy()).collect(Collectors.toList());
    result.idempotent = idempotent;
    result.language = language == null ? null : language.copy();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OCreateFunctionStatement that = (OCreateFunctionStatement) o;

    if (name != null ? !name.equals(that.name) : that.name != null) return false;
    if (codeQuoted != null ? !codeQuoted.equals(that.codeQuoted) : that.codeQuoted != null)
      return false;
    if (code != null ? !code.equals(that.code) : that.code != null) return false;
    if (parameters != null ? !parameters.equals(that.parameters) : that.parameters != null)
      return false;
    if (idempotent != null ? !idempotent.equals(that.idempotent) : that.idempotent != null)
      return false;
    if (language != null ? !language.equals(that.language) : that.language != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (codeQuoted != null ? codeQuoted.hashCode() : 0);
    result = 31 * result + (code != null ? code.hashCode() : 0);
    result = 31 * result + (parameters != null ? parameters.hashCode() : 0);
    result = 31 * result + (idempotent != null ? idempotent.hashCode() : 0);
    result = 31 * result + (language != null ? language.hashCode() : 0);
    return result;
  }
}
/* JavaCC - OriginalChecksum=bbc914f66e96822dedc7e89e14240872 (do not edit this line) */
