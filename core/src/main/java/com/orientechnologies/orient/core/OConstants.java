package com.orientechnologies.orient.core;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class OConstants {
  private static final OLogger logger = OLogManager.instance().logger(OConstants.class);
  public static final String ORIENT_URL = "https://www.orientdb.com";
  public static final String COPYRIGHT = "Copyrights (c) 2017 OrientDB LTD";

  private static final Properties properties = new Properties();

  static {
    try (final InputStream inputStream =
        OConstants.class.getResourceAsStream("/com/orientechnologies/orientdb.properties")) {
      if (inputStream != null) {
        properties.load(inputStream);
      }
    } catch (IOException e) {
      logger.errorNoDb("Failed to load OrientDB properties", e);
    }
  }

  /** @return Major part of OrientDB version */
  public static int getVersionMajor() {
    final String[] versions = properties.getProperty("version").split("\\.");
    if (versions.length == 0) {
      logger.errorNoDb("Can not retrieve version information for this build", null);
      return -1;
    }

    try {
      return Integer.parseInt(versions[0]);
    } catch (NumberFormatException nfe) {
      logger.errorNoDb("Can not retrieve major version information for this build", nfe);
      return -1;
    }
  }

  /** @return Minor part of OrientDB version */
  public static int getVersionMinor() {
    final String[] versions = properties.getProperty("version").split("\\.");
    if (versions.length < 2) {
      logger.errorNoDb("Can not retrieve minor version information for this build", null);
      return -1;
    }

    try {
      return Integer.parseInt(versions[1]);
    } catch (NumberFormatException nfe) {
      logger.errorNoDb("Can not retrieve minor version information for this build", nfe);
      return -1;
    }
  }

  /** @return Hotfix part of OrientDB version */
  @SuppressWarnings("unused")
  public static int getVersionHotfix() {
    final String[] versions = properties.getProperty("version").split("\\.");
    if (versions.length < 3) {
      return 0;
    }

    try {
      String hotfix = versions[2];
      int snapshotIndex = hotfix.indexOf("-SNAPSHOT");

      if (snapshotIndex != -1) {
        hotfix = hotfix.substring(0, snapshotIndex);
      }

      return Integer.parseInt(hotfix);
    } catch (NumberFormatException nfe) {
      logger.errorNoDb("Can not retrieve hotfix version information for this build", nfe);
      return -1;
    }
  }

  /** @return Returns only current version without build number and etc. */
  public static String getRawVersion() {
    return properties.getProperty("version");
  }

  /** Returns the complete text of the current OrientDB version. */
  public static String getVersion() {
    return properties.getProperty("version")
        + " (build "
        + properties.getProperty("revision")
        + ", branch "
        + properties.getProperty("branch")
        + ")";
  }

  /** Returns true if current OrientDB version is a snapshot. */
  public static boolean isSnapshot() {
    return properties.getProperty("version").endsWith("SNAPSHOT");
  }

  /** @return the build number if any. */
  public static String getBuildNumber() {
    return properties.getProperty("revision");
  }
}
