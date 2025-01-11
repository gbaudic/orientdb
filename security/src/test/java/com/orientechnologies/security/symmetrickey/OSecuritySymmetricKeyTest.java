package com.orientechnologies.security.symmetrickey;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
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
public class OSecuritySymmetricKeyTest extends AbstractSecurityTest {

  private static final String TESTDB = "SecuritySymmetricKeyTestDB";
  private static final String DATABASE_URL = "remote:localhost/" + TESTDB;

  private static OServer server;

  @BeforeClass
  public static void beforeClass() throws Exception {
    setup(TESTDB);

    createFile(
        SERVER_DIRECTORY + "/config/orientdb-server-config.xml",
        OSecuritySymmetricKeyTest.class.getResourceAsStream(
            "/com/orientechnologies/security/symmetrickey/orientdb-server-config.xml"));
    createFile(
        SERVER_DIRECTORY + "/config/security.json",
        OSecuritySymmetricKeyTest.class.getResourceAsStream(
            "/com/orientechnologies/security/symmetrickey/security.json"));

    // Create a default AES 128-bit key.
    OSymmetricKey sk = new OSymmetricKey("AES", "AES/CBC/PKCS5Padding", 128);
    sk.saveToKeystore(
        new FileOutputStream(SERVER_DIRECTORY + "/config/test.jks"),
        "password",
        "keyAlias",
        "password");
    sk.saveToStream(new FileOutputStream(SERVER_DIRECTORY + "/config/AES.key"));

    //  	 createFile(SERVER_DIRECTORY + "/config/test.jks",
    // OSymmetricKeyTest.class.getResourceAsStream("/com/orientechnologies/security/symmetrickey/test.jks"));

    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(new File(SERVER_DIRECTORY + "/config/orientdb-server-config.xml"));
    server.activate();

    OrientDB orientDB =
        new OrientDB(
            "remote:localhost",
            "root",
            "D2AFD02F20640EC8B7A5140F34FCA49D2289DB1F0D0598BB9DE8AAA75A0792F3",
            OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database "
            + TESTDB
            + " plocal users(admin identified by 'admin' role admin,reader identified by 'reader'"
            + " role reader,writer identified by 'writer' role writer )");
    orientDB.close();
  }

  @AfterClass
  public static void afterClass() {
    server.shutdown();

    Orient.instance().shutdown();
    Orient.instance().startup();

    cleanup(TESTDB);
  }

  @Test(expected = OSecurityAccessException.class)
  public void shouldTestSpecificAESKeyFailure() throws Exception {
    OSymmetricKey sk = new OSymmetricKey("AES", "AAC7LeGkFbmHEYNTz5GwDw==");

    OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
    // "test" is the username.  It's specified in the security.json resource file.
    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    ODatabaseDocument db = remote.open(TESTDB, "test", sk.encrypt("AES/CBC/PKCS5Padding", "test"));
    db.close();
    remote.close();
  }

  @Test
  public void shouldTestSpecificAESKey() throws Exception {
    // This key is specified in the security.json resource file.
    OSymmetricKey sk = new OSymmetricKey("AES", "8BC7LeGkFbmHEYNTz5GwDw==");

    OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
    // "test" is the username.  It's specified in the security.json resource file.
    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    ODatabaseDocument db = remote.open(TESTDB, "test", sk.encrypt("AES/CBC/PKCS5Padding", "test"));
    db.close();
    remote.close();
  }

  @Test
  public void shouldTestKeyFile() throws Exception {
    OSymmetricKey sk =
        OSymmetricKey.fromStream("AES", new FileInputStream(SERVER_DIRECTORY + "/config/AES.key"));

    OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
    // "test2" is the username.  It's specified in the security.json resource file.
    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    ODatabaseDocument db =
        remote.open(TESTDB, "test2", sk.encrypt("AES/CBC/PKCS5Padding", "test2"));
    db.close();
    remote.close();
  }

  @Test
  public void shouldTestKeystore() throws Exception {
    OSymmetricKey sk =
        OSymmetricKey.fromKeystore(
            new FileInputStream(SERVER_DIRECTORY + "/config/test.jks"),
            "password",
            "keyAlias",
            "password");

    OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
    // "test3" is the username.  It's specified in the security.json resource file.
    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    ODatabaseDocument db =
        remote.open(TESTDB, "test3", sk.encrypt("AES/CBC/PKCS5Padding", "test3"));
    db.close();
    remote.close();
  }
}
