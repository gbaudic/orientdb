/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql.parser.operators;

import com.orientechnologies.orient.core.sql.parser.OInOperator;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OInOperatorTest {
  @Test
  public void test() {
    OInOperator op = new OInOperator(-1);

    Assert.assertFalse(op.execute(null, null, null));
    Assert.assertFalse(op.execute(null, "foo", null));
    Assert.assertFalse(op.execute("foo", null, null));
    Assert.assertFalse(op.execute("foo", "foo", null));

    List<Object> list1 = new ArrayList<Object>();
    Assert.assertFalse(op.execute("foo", list1, null));
    Assert.assertFalse(op.execute(null, list1, null));
    Assert.assertTrue(op.execute(list1, list1, null));

    list1.add("a");
    list1.add(1);

    Assert.assertFalse(op.execute("foo", list1, null));
    Assert.assertTrue(op.execute("a", list1, null));
    Assert.assertTrue(op.execute(1, list1, null));

    // TODO
  }
}
