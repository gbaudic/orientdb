/* Generated By:JJTree: Do not edit this line. OAlterPropertyStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

public class OAlterPropertyStatement extends ODDLStatement {

  OIdentifier className;

  OIdentifier propertyName;
  OIdentifier customPropertyName;
  OExpression customPropertyValue;

  OIdentifier settingName;
  public OExpression settingValue;

  public OAlterPropertyStatement(int id) {
    super(id);
  }

  public OAlterPropertyStatement(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public OExecutionStream executeDDL(OCommandContext ctx) {
    ODatabaseSession db = ctx.getDatabase();
    OClass clazz = db.getMetadata().getSchema().getClass(className.getStringValue());

    if (clazz == null) {
      throw new OCommandExecutionException("Invalid class name or class not found: " + clazz);
    }

    OProperty property = clazz.getProperty(propertyName.getStringValue());
    if (property == null) {
      throw new OCommandExecutionException(
          "Property " + propertyName.getStringValue() + " not found on class " + clazz);
    }

    OResultInternal result = new OResultInternal();
    result.setProperty("class", className.getStringValue());
    result.setProperty("property", propertyName.getStringValue());

    if (customPropertyName != null) {
      String customName = customPropertyName.getStringValue();
      Object oldValue = property.getCustom(customName);
      Object finalValue = customPropertyValue.execute((OResult) null, ctx);
      property.setCustom(customName, finalValue == null ? null : "" + finalValue);

      result.setProperty("operation", "alter property custom");
      result.setProperty("customAttribute", customPropertyName.getStringValue());
      result.setProperty("oldValue", oldValue != null ? oldValue.toString() : null);
      result.setProperty("newValue", finalValue != null ? finalValue.toString() : null);
    } else {
      String setting = settingName.getStringValue();
      boolean isCollate = setting.equalsIgnoreCase("collate");
      Object finalValue = settingValue.execute((OResult) null, ctx);
      if (finalValue == null
          && (setting.equalsIgnoreCase("name")
              || setting.equalsIgnoreCase("shortname")
              || setting.equalsIgnoreCase("type")
              || isCollate)) {
        finalValue = settingValue.toString();
        String stringFinalValue = (String) finalValue;
        if (stringFinalValue.startsWith("`")
            && stringFinalValue.endsWith("`")
            && stringFinalValue.length() > 2) {
          stringFinalValue = stringFinalValue.substring(1, stringFinalValue.length() - 1);
          finalValue = stringFinalValue;
        }
      }
      OProperty.ATTRIBUTES attribute;
      try {
        attribute = OProperty.ATTRIBUTES.valueOf(setting.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        throw OException.wrapException(
            new OCommandExecutionException(
                "Unknown property attribute '"
                    + setting
                    + "'. Supported attributes are: "
                    + Arrays.toString(OProperty.ATTRIBUTES.values())),
            e);
      }
      Object oldValue = property.get(attribute);
      property.set(attribute, finalValue);
      finalValue = property.get(attribute); // it makes some conversions...

      result.setProperty("operation", "alter property");
      result.setProperty("attribute", setting);
      result.setProperty("oldValue", oldValue != null ? oldValue.toString() : null);
      result.setProperty("newValue", finalValue != null ? finalValue.toString() : null);
    }
    return OExecutionStream.singleton(result);
  }

  @Override
  public void validate() throws OCommandSQLParsingException {
    super.validate(); // TODO
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("ALTER PROPERTY ");
    className.toString(params, builder);
    builder.append(".");
    propertyName.toString(params, builder);
    if (customPropertyName != null) {
      builder.append(" CUSTOM ");
      customPropertyName.toString(params, builder);
      builder.append(" = ");
      customPropertyValue.toString(params, builder);
    } else {
      builder.append(" ");
      settingName.toString(params, builder);
      builder.append(" ");
      settingValue.toString(params, builder);
    }
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("ALTER PROPERTY ");
    className.toGenericStatement(builder);
    builder.append(".");
    propertyName.toGenericStatement(builder);
    if (customPropertyName != null) {
      builder.append(" CUSTOM ");
      customPropertyName.toGenericStatement(builder);
      builder.append(" = ");
      customPropertyValue.toGenericStatement(builder);
    } else {
      builder.append(" ");
      settingName.toGenericStatement(builder);
      builder.append(" ");
      settingValue.toGenericStatement(builder);
    }
  }

  @Override
  public OAlterPropertyStatement copy() {
    OAlterPropertyStatement result = new OAlterPropertyStatement(-1);
    result.className = className == null ? null : className.copy();
    result.propertyName = propertyName == null ? null : propertyName.copy();
    result.customPropertyName = customPropertyName == null ? null : customPropertyName.copy();
    result.customPropertyValue = customPropertyValue == null ? null : customPropertyValue.copy();
    result.settingName = settingName == null ? null : settingName.copy();
    result.settingValue = settingValue == null ? null : settingValue.copy();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OAlterPropertyStatement that = (OAlterPropertyStatement) o;

    if (className != null ? !className.equals(that.className) : that.className != null)
      return false;
    if (propertyName != null ? !propertyName.equals(that.propertyName) : that.propertyName != null)
      return false;
    if (customPropertyName != null
        ? !customPropertyName.equals(that.customPropertyName)
        : that.customPropertyName != null) return false;
    if (customPropertyValue != null
        ? !customPropertyValue.equals(that.customPropertyValue)
        : that.customPropertyValue != null) return false;
    if (settingName != null ? !settingName.equals(that.settingName) : that.settingName != null)
      return false;
    if (settingValue != null ? !settingValue.equals(that.settingValue) : that.settingValue != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = className != null ? className.hashCode() : 0;
    result = 31 * result + (propertyName != null ? propertyName.hashCode() : 0);
    result = 31 * result + (customPropertyName != null ? customPropertyName.hashCode() : 0);
    result = 31 * result + (customPropertyValue != null ? customPropertyValue.hashCode() : 0);
    result = 31 * result + (settingName != null ? settingName.hashCode() : 0);
    result = 31 * result + (settingValue != null ? settingValue.hashCode() : 0);
    return result;
  }
}
/* JavaCC - OriginalChecksum=2421f6ad3b5f1f8e18149650ff80f1e7 (do not edit this line) */
