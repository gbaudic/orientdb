/* Generated By:JJTree: Do not edit this line. OCluster.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.Map;

public class OCluster extends SimpleNode {
  protected String clusterName;
  protected Integer clusterNumber;

  public OCluster(String clusterName) {
    super(-1);
    this.clusterName = clusterName;
  }

  public OCluster(int id) {
    super(id);
  }

  public OCluster(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public String toString(String prefix) {
    return super.toString(prefix);
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    if (clusterName != null) {
      builder.append("cluster:" + clusterName);
    } else {
      builder.append("cluster:" + clusterNumber);
    }
  }

  public void toGenericStatement(Map<Object, Object> params, StringBuilder builder) {
    if (clusterName != null) {
      builder.append("cluster:" + clusterName);
    } else {
      builder.append("cluster:" + clusterNumber);
    }
  }

  public String getClusterName() {
    return clusterName;
  }

  public Integer getClusterNumber() {
    return clusterNumber;
  }

  public OCluster copy() {
    OCluster result = new OCluster(-1);
    result.clusterName = clusterName;
    result.clusterNumber = clusterNumber;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OCluster oCluster = (OCluster) o;

    if (clusterName != null
        ? !clusterName.equals(oCluster.clusterName)
        : oCluster.clusterName != null) return false;
    if (clusterNumber != null
        ? !clusterNumber.equals(oCluster.clusterNumber)
        : oCluster.clusterNumber != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = clusterName != null ? clusterName.hashCode() : 0;
    result = 31 * result + (clusterNumber != null ? clusterNumber.hashCode() : 0);
    return result;
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    result.setProperty("clusterName", clusterName);
    result.setProperty("clusterNumber", clusterNumber);
    return result;
  }

  public void deserialize(OResult fromResult) {
    clusterName = fromResult.getProperty("clusterName");
    clusterNumber = fromResult.getProperty("clusterNumber");
  }
}
/* JavaCC - OriginalChecksum=d27abf009fe7db482fbcaac9d52ba192 (do not edit this line) */
