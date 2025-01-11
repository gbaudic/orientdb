package com.orientechnologies.security.kerberos;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.security.AbstractSecurityTest;
import java.io.IOException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author sdipro
 * @since 10/06/16
 */
@Ignore
public class KerberosClientTest extends AbstractSecurityTest {

  private static final String kerbServer = "kerby.odbrealm.com";
  private static final String testDB = "TestDB";
  private static final String kerbUser = "orientdb@ODBREALM.COM";
  private static final String spn = "OrientDB/kerby.odbrealm.com";
  private static final String ccache = "/home/jenkins/ccache";

  @BeforeClass
  public static void beforeClass() throws Exception {
    OrientDB remote =
        new OrientDB("remote:" + kerbServer, "root", "password", OrientDBConfig.defaultConfig());

    if (!remote.exists(testDB)) {
      remote.create(testDB, ODatabaseType.MEMORY);

      // orientdb@ODBREALM.COM
      ODatabaseDocument db = remote.open(testDB, "root", "password");

      try {
        // Create the Kerberos client user.
        final String sql =
            String.format(
                "create user %s identified by %s role %s", kerbUser, "notneeded", "admin");
        db.command(sql).close();
      } finally {
        if (db != null) db.close();
      }
    }

    remote.close();

    OGlobalConfiguration.CLIENT_CREDENTIAL_INTERCEPTOR.setValue(
        "com.orientechnologies.orient.core.security.kerberos.OKerberosCredentialInterceptor");
    OGlobalConfiguration.CLIENT_KRB5_CONFIG.setValue("/etc/krb5.conf");
  }

  @AfterClass
  public static void afterClass() {}

  @Test
  public void defaultSPNTest() throws InterruptedException, IOException {
    shellCommand(String.format("echo password | kinit -c %s orientdb", ccache));

    Assert.assertTrue(fileExists(ccache));

    OGlobalConfiguration.CLIENT_KRB5_CCNAME.setValue(ccache);
    OrientDB remote = new OrientDB("remote:" + kerbServer, OrientDBConfig.defaultConfig());

    ODatabaseDocument db = remote.open(testDB, kerbUser, "");

    db.close();
    remote.close();
  }

  @Test
  public void explicitSPNTest() throws InterruptedException, IOException {
    shellCommand(String.format("echo password | kinit -c %s orientdb", ccache));

    Assert.assertTrue(fileExists(ccache));

    OGlobalConfiguration.CLIENT_KRB5_CCNAME.setValue(ccache);
    OrientDB remote = new OrientDB("remote:" + kerbServer, OrientDBConfig.defaultConfig());
    ODatabaseDocument db = remote.open(testDB, kerbUser, spn);

    db.close();
    remote.close();
  }

  @Test(expected = OSecurityException.class)
  public void shouldFailAuthenticationTest() throws InterruptedException, IOException {
    final String wrongcache = "/home/jenkins/wrongcache";

    shellCommand(String.format("echo password | kinit -c %s orientdb", ccache));

    OGlobalConfiguration.CLIENT_KRB5_CCNAME.setValue(wrongcache);
    OrientDB remote = new OrientDB("remote:" + kerbServer, OrientDBConfig.defaultConfig());
    ODatabaseDocument db = remote.open(testDB, kerbUser, spn);

    db.close();
    remote.close();
  }
}
