/* Generated By:JJTree: Do not edit this line. SimpleNode.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import java.util.Map;

public abstract class SimpleNode implements Node {
  public static final String PARAMETER_PLACEHOLDER = "?";
  protected Node parent;
  protected Node[] children;
  protected int id;
  protected Object value;
  protected OrientSql parser;
  protected Token firstToken;
  protected Token lastToken;

  public SimpleNode() {
    id = -1;
  }

  public SimpleNode(int i) {
    id = i;
  }

  public SimpleNode(OrientSql p, int i) {
    this(i);
    parser = p;
  }

  public void jjtOpen() {}

  public void jjtClose() {}

  public void jjtSetParent(Node n) {
    parent = n;
  }

  public Node jjtGetParent() {
    return parent;
  }

  public void jjtAddChild(Node n, int i) {
    if (children == null) {
      children = new Node[i + 1];
    } else if (i >= children.length) {
      Node c[] = new Node[i + 1];
      System.arraycopy(children, 0, c, 0, children.length);
      children = c;
    }
    children[i] = n;
  }

  public Node jjtGetChild(int i) {
    return children[i];
  }

  public int jjtGetNumChildren() {
    return (children == null) ? 0 : children.length;
  }

  public void jjtSetValue(Object value) {
    this.value = value;
  }

  public Object jjtGetValue() {
    return value;
  }

  public Token jjtGetFirstToken() {
    return firstToken;
  }

  public void jjtSetFirstToken(Token token) {
    this.firstToken = token;
  }

  public Token jjtGetLastToken() {
    return lastToken;
  }

  public void jjtSetLastToken(Token token) {
    this.lastToken = token;
  }

  /** Accept the visitor. */
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  /** Accept the visitor. */
  public Object childrenAccept(OrientSqlVisitor visitor, Object data) {
    if (children != null) {
      for (int i = 0; i < children.length; ++i) {
        children[i].jjtAccept(visitor, data);
      }
    }
    return data;
  }

  /*
   * You can override these two methods in subclasses of SimpleNode to customize the way the node appears when the tree is dumped.
   * If your output uses more than one line you should override toString(String), otherwise overriding toString() is probably all
   * you need to do.
   */

  public String toString() {
    StringBuilder result = new StringBuilder();
    toString(null, result);
    return result.toString();
  }

  public String toString(String prefix) {
    return prefix + toString();
  }

  /*
   * Override this method if you want to customize how the node dumps out its children.
   */

  public void dump(String prefix) {
    if (children != null) {
      for (int i = 0; i < children.length; ++i) {
        SimpleNode n = (SimpleNode) children[i];
        if (n != null) {
          n.dump(prefix + " ");
        }
      }
    }
  }

  public static ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  public abstract void toString(Map<Object, Object> params, StringBuilder builder);

  public abstract void toGenericStatement(Map<Object, Object> params, StringBuilder builder);

  public String toGenericStatement() {
    StringBuilder builder = new StringBuilder();
    toGenericStatement(null, builder);
    return builder.toString();
  }

  public Object getValue() {
    return value;
  }

  public SimpleNode copy() {
    throw new UnsupportedOperationException();
  }
}

/* JavaCC - OriginalChecksum=d5ed710e8a3f29d574adbb1d37e08f3b (do not edit this line) */
