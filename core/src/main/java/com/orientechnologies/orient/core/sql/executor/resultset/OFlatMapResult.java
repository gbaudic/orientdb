package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public interface OFlatMapResult {

  OExecutionStream flatMap(OResult next, OCommandContext ctx);
}
