package com.orientechnologies.orient.monitor.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.monitor.OMonitorPlugin;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandServerProfiler extends OServerCommandAuthenticatedDbAbstract {

	private static final String[]	NAMES	= { "GET|profiler/*", "POST|profiler/*" };

	private OMonitorPlugin				monitor;

	public OServerCommandServerProfiler(final OServerCommandConfiguration iConfiguration) {
	}

	public OServerCommandServerProfiler() {

	}

	@Override
	public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

		if (monitor == null)
			monitor = OServerMain.server().getPluginByClass(OMonitorPlugin.class);
		final String[] parts = checkSyntax(iRequest.url, 2, "Syntax error: profiler/monitor/<server>/<command>");
		
		String server =  parts[2];
		ODocument doc = monitor.getMonitoredServer(server).getConfiguration();
		
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}

}
