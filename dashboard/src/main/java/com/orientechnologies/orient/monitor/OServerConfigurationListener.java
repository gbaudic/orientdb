package com.orientechnologies.orient.monitor;

import com.orientechnologies.orient.core.record.impl.ODocument;

public interface OServerConfigurationListener {

  public void onConfigurationChange(final ODocument iServer, final String iFieldName, final Object value);

}
