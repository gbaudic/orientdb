package com.orientechnologies.orient.core.sql.parser;

import org.junit.Assert;
import org.junit.Test;

public class OStatementCacheTest {

  @Test
  public void testInIsNotAReservedWord() {
    OStatementCache cache = new OStatementCache(2);
    cache.getStatement("select from foo", null);
    cache.getStatement("select from bar", null);
    cache.getStatement("select from baz", null);

    Assert.assertTrue(cache.contains("select from bar"));
    Assert.assertTrue(cache.contains("select from baz"));
    Assert.assertFalse(cache.contains("select from foo"));

    cache.getStatement("select from bar", null);
    cache.getStatement("select from foo", null);

    Assert.assertTrue(cache.contains("select from bar"));
    Assert.assertTrue(cache.contains("select from foo"));
    Assert.assertFalse(cache.contains("select from baz"));
  }
}
