package com.orientechnologies.security.password;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.security.OInvalidPasswordException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.security.AbstractSecurityTest;
import java.io.File;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author sdipro
 * @since 10/06/16
 *     <p>Creates SERVER_DIRECTORY and a 'config' directory. Loads customized
 *     'orientdb-server-config.xml' and 'security.json' files under config. Launches a new OServer
 *     (using the security.json resource). Uses OServerAdmin to create a new 'TestDB' database.
 *     Tests using a UUID. Tests the minimum count of total characters (5). Tests the minimum count
 *     of number characters (2). Tests the minimum count of special characters (2). Tests the
 *     minimum count of uppercase characters (3). Tests using a valid UUID as a password. Tests a
 *     valid password meeting all criteria.
 */
public class PasswordValidatorTest extends AbstractSecurityTest {

  private static final String TESTDB = "PasswordValidatorTestDB";

  private static OServer server;

  @BeforeClass
  public static void beforeClass() throws Exception {
    setup(TESTDB);

    createFile(
        SERVER_DIRECTORY + "/config/orientdb-server-config.xml",
        PasswordValidatorTest.class.getResourceAsStream(
            "/com/orientechnologies/security/password/orientdb-server-config.xml"));
    createFile(
        SERVER_DIRECTORY + "/config/security.json",
        PasswordValidatorTest.class.getResourceAsStream(
            "/com/orientechnologies/security/password/security.json"));

    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(new File(SERVER_DIRECTORY + "/config/orientdb-server-config.xml"));
    server.activate();

    server.getContext().create(TESTDB, ODatabaseType.PLOCAL);
  }

  @AfterClass
  public static void afterClass() {
    server.shutdown();

    cleanup(TESTDB);

    Orient.instance().shutdown();
    Orient.instance().startup();
  }

  @Test
  public void minCharacterTest() {
    try (OrientDB orientDb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession db = orientDb.open(TESTDB, "root", ROOT_PASSWORD)) {
        final String sql =
            String.format("create user %s identified by %s role %s", "testuser", "pass", "admin");
        db.command(sql).close();
      } catch (Exception ex) {

        assertThat(ex).isInstanceOf(OInvalidPasswordException.class);
      }
    }
  }

  @Test
  public void minNumberTest() {
    try (OrientDB orientDb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession db = orientDb.open(TESTDB, "root", ROOT_PASSWORD)) {
        final String sql =
            String.format("create user %s identified by %s role %s", "testuser", "passw", "admin");
        db.command(sql).close();
      } catch (Exception ex) {
        assertThat(ex).isInstanceOf(OInvalidPasswordException.class);
      }
    }
  }

  @Test
  public void minSpecialTest() {
    try (OrientDB orientDb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession db = orientDb.open(TESTDB, "root", ROOT_PASSWORD)) {

        final String sql =
            String.format(
                "create user %s identified by %s role %s", "testuser", "passw12", "admin");
        db.command(sql).close();
      } catch (Exception ex) {
        assertThat(ex).isInstanceOf(OInvalidPasswordException.class);
      }
    }
  }

  @Test
  public void minUppercaseTest() {
    try (OrientDB orientDb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession db = orientDb.open(TESTDB, "root", ROOT_PASSWORD)) {

        final String sql =
            String.format(
                "create user %s identified by \"%s\" role %s", "testuser", "passw12$$", "admin");
        db.command(sql).close();
      } catch (Exception ex) {
        assertThat(ex).isInstanceOf(OInvalidPasswordException.class);
      }
    }
  }

  @Test
  public void uuidTest() {
    try (OrientDB orientDb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession db = orientDb.open(TESTDB, "root", ROOT_PASSWORD)) {

        final String sql =
            String.format(
                "create user %s identified by '%s' role %s",
                "uuiduser", java.util.UUID.randomUUID().toString(), "admin");
        db.command(sql).close();
      }
    }
  }

  @Test
  public void validTest() {
    try (OrientDB orientDb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession db = orientDb.open(TESTDB, "root", ROOT_PASSWORD)) {

        final String sql =
            String.format(
                "create user %s identified by '%s' role %s", "testuser", "PASsw12$$", "admin");
        db.command(sql).close();
      }
    }
  }
}
