/* Generated By:JJTree: Do not edit this line. OCommandLineOption.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import java.util.Map;

public class OCommandLineOption extends SimpleNode {

  protected OIdentifier name;

  public OCommandLineOption(int id) {
    super(id);
  }

  public OCommandLineOption(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("-");
    name.toString(params, builder);
  }

  @Override
  public void toGenericStatement(Map<Object, Object> params, StringBuilder builder) {
    builder.append("-");
    name.toGenericStatement(params, builder);
  }

  public OCommandLineOption copy() {
    OCommandLineOption result = new OCommandLineOption(-1);
    result.name = name == null ? null : name.copy();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OCommandLineOption that = (OCommandLineOption) o;

    if (name != null ? !name.equals(that.name) : that.name != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return name != null ? name.hashCode() : 0;
  }
}
/* JavaCC - OriginalChecksum=7fcb8de8a1f99a2737aac85933d074d9 (do not edit this line) */
