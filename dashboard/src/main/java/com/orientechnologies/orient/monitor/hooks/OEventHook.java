/*
 * Copyright 2010-2013 Orient Technologies LTD
 * All Rights Reserved. Commercial License.
 *
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 */

package com.orientechnologies.orient.monitor.hooks;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.monitor.event.OEventController;
import com.orientechnologies.orient.monitor.event.OEventExecutor;

public class OEventHook extends ORecordHookAbstract {

	@Override
	public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void onRecordAfterCreate(ORecord<?> iiRecord) {

		ODocument doc = (ODocument) iiRecord;
		List<ODocument> triggers = new ArrayList<ODocument>();

		if (doc.getClassName().equalsIgnoreCase("Log")) {
			triggers = doc.getDatabase().query(
					new OSQLSynchQuery<Object>(
							"select from Event where when.type = "
									+ doc.field("level") + ""));
		} else {
			
			
//			doc.field("name");
			triggers = doc.getDatabase().query(
					new OSQLSynchQuery<Object>(
							"select from Event where when.name = '"
									+ doc.field("name") + "'"));
		}
		for (ODocument oDocument : triggers) {

			ODocument when = oDocument.field("when");
			ODocument what = oDocument.field("what");
			String classWhen = when.field("@class");
			String classWhat = what.field("@class");
			OEventExecutor executor = OEventController.getInstance()
					.getExecutor(classWhen, classWhat);
			executor.execute(doc, when, what);

		}

	}
}
