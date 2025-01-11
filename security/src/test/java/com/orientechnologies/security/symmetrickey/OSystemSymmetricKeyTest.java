package com.orientechnologies.security.symmetrickey;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.security.symmetrickey.OSymmetricKey;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.security.AbstractSecurityTest;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author S. Colin Leister */
public class OSystemSymmetricKeyTest extends AbstractSecurityTest {

  private static final String TESTDB = "OSystemSymmetricKeyTestDB";
  private static final String DATABASE_URL = "remote:localhost/" + TESTDB;

  private static OServer server;
  private static OrientDB remote;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cleanup("OSystem");
    setup(TESTDB);

    createFile(
        SERVER_DIRECTORY + "/config/orientdb-server-config.xml",
        OSystemSymmetricKeyTest.class.getResourceAsStream(
            "/com/orientechnologies/security/symmetrickey/orientdb-server-config.xml"));
    createFile(
        SERVER_DIRECTORY + "/config/security.json",
        OSystemSymmetricKeyTest.class.getResourceAsStream(
            "/com/orientechnologies/security/symmetrickey/security.json"));

    // Create a default AES 128-bit key.
    OSymmetricKey sk = new OSymmetricKey("AES", "AES/CBC/PKCS5Padding", 128);
    sk.saveToKeystore(
        new FileOutputStream(SERVER_DIRECTORY + "/config/test.jks"),
        "password",
        "keyAlias",
        "password");
    sk.saveToStream(new FileOutputStream(SERVER_DIRECTORY + "/config/AES.key"));

    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(new File(SERVER_DIRECTORY + "/config/orientdb-server-config.xml"));
    server.activate();

    remote =
        new OrientDB(
            "remote:localhost",
            "root",
            "D2AFD02F20640EC8B7A5140F34FCA49D2289DB1F0D0598BB9DE8AAA75A0792F3",
            OrientDBConfig.defaultConfig());
    remote
        .execute(
            "create database "
                + TESTDB
                + " plocal users(admin identified by 'adminpwd' role admin)")
        .close();

    server
        .getSystemDatabase()
        .execute(null, "UPDATE ORole set dbFilter = ['*'] WHERE name = ?", "admin");
  }

  @AfterClass
  public static void afterClass() {
    server.shutdown();

    Orient.instance().shutdown();
    Orient.instance().startup();

    cleanup(TESTDB);
  }

  @Test
  public void shouldTestSystemUserWithKey() throws Exception {

    final String sysuser = "sysuser";

    server
        .getSystemDatabase()
        .execute(
            null,
            "insert into OUser set name=?, password='password', status='ACTIVE', roles=(SELECT FROM"
                + " ORole WHERE name = ?)",
            sysuser,
            "root");
    server
        .getSystemDatabase()
        .execute(
            null,
            "update OUser set properties={'@type':'d',"
                + " 'key':'8BC7LeGkFbmHEYNTz5GwDw==','keyAlgorithm':'AES'} where name = ?",
            sysuser);

    OSymmetricKey sk = new OSymmetricKey("AES", "8BC7LeGkFbmHEYNTz5GwDw==");

    OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
    // "sysuser" is the username.  We just created it in OSystem.
    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    ODatabaseDocument db =
        remote.open(TESTDB, sysuser, sk.encrypt("AES/CBC/PKCS5Padding", sysuser));
    db.close();
    remote.close();
  }

  @Test
  public void shouldTestSystemUserWithKeyFile() throws Exception {

    final String sysuser = "sysuser2";

    server
        .getSystemDatabase()
        .execute(
            null,
            "insert into OUser set name=?, password='password', status='ACTIVE', roles=(SELECT FROM"
                + " ORole WHERE name = ?)",
            sysuser,
            "root");
    server
        .getSystemDatabase()
        .execute(
            null,
            "update OUser set properties={'@type':'d',"
                + " 'keyFile':'${ORIENTDB_HOME}/config/AES.key','keyAlgorithm':'AES'} where name ="
                + " ?",
            sysuser);

    OSymmetricKey sk =
        OSymmetricKey.fromStream("AES", new FileInputStream(SERVER_DIRECTORY + "/config/AES.key"));

    OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
    // "sysuser" is the username.  We just created it in OSystem.
    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    ODatabaseDocument db =
        remote.open(TESTDB, sysuser, sk.encrypt("AES/CBC/PKCS5Padding", sysuser));
    db.close();
    remote.close();
  }

  @Test
  public void shouldTestSystemUserWithKeystore() throws Exception {

    final String sysuser = "sysuser3";

    server
        .getSystemDatabase()
        .execute(
            null,
            "insert into OUser set name=?, password='password', status='ACTIVE', roles=(SELECT FROM"
                + " ORole WHERE name = ?)",
            sysuser,
            "root");
    server
        .getSystemDatabase()
        .execute(
            null,
            "update OUser set properties={'@type':'d',"
                + " 'keyStore':{'file':'${ORIENTDB_HOME}/config/test.jks','password':'password','keyAlias':'keyAlias','keyPassword':'password'}}"
                + " where name = ?",
            sysuser);

    OSymmetricKey sk =
        OSymmetricKey.fromKeystore(
            new FileInputStream(SERVER_DIRECTORY + "/config/test.jks"),
            "password",
            "keyAlias",
            "password");

    OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
    // "sysuser" is the username.  We just created it in OSystem.
    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    ODatabaseDocument db =
        remote.open(TESTDB, sysuser, sk.encrypt("AES/CBC/PKCS5Padding", sysuser));
    db.close();
    remote.close();
  }
}
