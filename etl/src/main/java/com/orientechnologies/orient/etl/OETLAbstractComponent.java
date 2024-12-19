/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl;

import static com.orientechnologies.common.parser.OSystemVariableResolver.VAR_BEGIN;
import static com.orientechnologies.common.parser.OSystemVariableResolver.VAR_END;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.parser.OOrBlock;
import com.orientechnologies.orient.etl.context.OETLContext;

/** ETL abstract component. */
public abstract class OETLAbstractComponent implements OETLComponent {
  private static OLogger logger = OLogManager.instance().logger(OETLAbstractComponent.class);
  protected OETLProcessor processor;
  protected OCommandContext context;
  protected String output;
  protected String ifExpression;
  protected ODocument configuration;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + "]");
  }

  @Override
  public void configure(final ODocument iConfiguration, final OCommandContext iContext) {
    context = iContext;
    configuration = iConfiguration;
    ifExpression = iConfiguration.field("if");
  }

  @Override
  public void begin(ODatabaseDocument db) {
    if (configuration.containsField("output")) output = configuration.field("output");
  }

  @Override
  public void end() {}

  @Override
  public OETLContext getContext() {
    return getProcessor().getContext();
  }

  @Override
  public OETLProcessor getProcessor() {
    return processor;
  }

  @Override
  public void setProcessor(OETLProcessor processor) {
    this.processor = processor;
  }

  protected String getCommonConfigurationParameters() {
    return "{log:{optional:true,description:'Can be any of [NONE, ERROR, INFO, DEBUG]. Default is"
        + " INFO'}},{if:{optional:true,description:'Conditional expression. If true, the"
        + " block is executed, otherwise is"
        + " skipped'}},{output:{optional:true,description:'Variable name to store the"
        + " transformer output. If null, the output will be passed to the pipeline as input"
        + " for the next component.'}}";
  }

  @Override
  public String toString() {
    return getName();
  }

  protected boolean skip(final Object input) {
    final OOrBlock ifFilter = getIfFilter();
    if (ifFilter != null) {
      final ODocument doc =
          input instanceof OIdentifiable ? (ODocument) ((OIdentifiable) input).getRecord() : null;

      debug("Evaluating conditional expression if=%s...", ifFilter);

      final Object result = ifFilter.evaluate(doc, context);
      if (!(result instanceof Boolean))
        throw new OConfigurationException(
            "'if' expression in Transformer "
                + getName()
                + " returned '"
                + result
                + "' instead of boolean");

      return !(Boolean) result;
    }
    return false;
  }

  protected OOrBlock getIfFilter() {
    if (ifExpression != null) return OSQLEngine.parsePredicate(ifExpression);
    return null;
  }

  protected void error(String iText, final Object... iArgs) {
    logger.error(format(iText), null, iArgs);
  }

  protected void error(String iText, Throwable exception, final Object... iArgs) {
    logger.error(format(iText), exception, iArgs);
  }

  protected void debug(String iText, final Object... iArgs) {
    logger.debug(format(iText), null, iArgs);
  }

  protected void info(String iText, final Object... iArgs) {
    logger.info(format(iText), null, iArgs);
  }

  protected void warn(String iText, final Object... iArgs) {
    logger.warn(format(iText), null, iArgs);
  }

  protected String format(String iText) {
    final Long extractedNum = context != null ? (Long) context.getVariable("extractedNum") : null;
    if (extractedNum != null) {
      return "[" + extractedNum + ":" + getName() + "]  " + iText;
    } else {
      return "[" + getName() + "] " + iText;
    }
  }

  protected String stringArray2Json(final Object[] iObject) {
    final StringBuilder buffer = new StringBuilder(256);
    buffer.append('[');
    for (int i = 0; i < iObject.length; ++i) {
      if (i > 0) buffer.append(',');

      final Object value = iObject[i];
      if (value != null) {
        buffer.append("'");
        buffer.append(value.toString());
        buffer.append("'");
      }
    }
    buffer.append(']');
    return buffer.toString();
  }

  protected Object resolve(final Object content) {
    if (context == null || content == null) return content;

    Object value;
    if (content instanceof String) {
      String contentAsString = (String) content;
      if (contentAsString.startsWith("$") && !contentAsString.startsWith(VAR_BEGIN)) {
        value = context.getVariable(content.toString());
      } else {
        value =
            OVariableParser.resolveVariables(
                contentAsString, VAR_BEGIN, VAR_END, variable -> context.getVariable(variable));
      }
    } else {
      value = content;
    }
    if (value instanceof String) {
      value =
          OVariableParser.resolveVariables(
              (String) value,
              "={",
              "}",
              variable -> {
                return OSQLEngine.parseExpression(variable).execute((OIdentifiable) null, context);
              });
    }
    return value;
  }
}
