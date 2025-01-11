/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Disabled for now. Re-enable it when this is implemented
 * https://github.com/orientechnologies/orientdb/issues/5958
 *
 * @author Enrico Risa <e.risa@orientdb.com>.
 * @since 4/4/2016
 */
public class StorageBackupTestWithLuceneIndex {
  private String buildDirectory;

  private OrientDB ctx;
  private ODatabaseDocumentInternal db;
  private String dbDirectory;
  private String dbName;
  private String dbBackupName;
  private String backedUpDbDirectory;

  @Before
  public void before() {
    buildDirectory = System.getProperty("buildDirectory", "./target");
    dbDirectory = buildDirectory + "/backup-tests";
    dbName = StorageBackupTestWithLuceneIndex.class.getSimpleName();
    dbBackupName = StorageBackupTestWithLuceneIndex.class.getSimpleName() + "BackUp";

    OFileUtils.deleteRecursively(new File(dbDirectory));
    ctx = new OrientDB("embedded:" + dbDirectory, OrientDBConfig.defaultConfig());
    ctx.execute(
            "create database "
                + dbName
                + " plocal users(admin identified by 'adminpwd' role admin)")
        .close();
    db = (ODatabaseDocumentInternal) ctx.open(dbName, "admin", "adminpwd");

    backedUpDbDirectory =
        buildDirectory
            + File.separator
            + StorageBackupTestWithLuceneIndex.class.getSimpleName()
            + "BackUp";
  }

  @After
  public void after() {
    if (ctx.exists(dbName)) {
      ctx.drop(dbName);
    }
    if (ctx.exists(dbBackupName)) {
      ctx.drop(dbBackupName);
    }

    OFileUtils.deleteRecursively(new File(dbDirectory));
    OFileUtils.deleteRecursively(new File(buildDirectory, "backupDir"));
  }

  @Test
  @Ignore
  public void testSingeThreadFullBackup() throws IOException {

    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", OType.INTEGER);
    backupClass.createProperty("name", OType.STRING);

    backupClass.createIndex(
        "backupLuceneIndex",
        OClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE",
        new String[] {"name"});

    final ODocument document = new ODocument("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage");
    db.save(document);

    final File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) Assert.assertTrue(backupDir.mkdirs());

    db.incrementalBackup(backupDir.getAbsolutePath());
    db.close();

    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    OrientDBInternal internal = OrientDBInternal.extract(ctx);
    internal.restore(
        dbBackupName,
        null,
        null,
        ODatabaseType.PLOCAL,
        backupDir.getAbsolutePath(),
        OrientDBConfig.defaultConfig());

    final ODatabaseDocumentInternal backedUpDb =
        (ODatabaseDocumentInternal) ctx.open(dbBackupName, "admin", "adminpwd");
    db = (ODatabaseDocumentInternal) ctx.open(dbName, "admin", "adminpwd");

    final ODatabaseCompare compare =
        new ODatabaseCompare(
            db,
            backedUpDb,
            new OCommandOutputListener() {
              @Override
              public void onMessage(String iText) {
                System.out.println(iText);
              }
            });

    Assert.assertTrue(compare.compare());
  }

  @Test
  @Ignore
  public void testSingeThreadIncrementalBackup() throws IOException {

    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", OType.INTEGER);
    backupClass.createProperty("name", OType.STRING);

    backupClass.createIndex(
        "backupLuceneIndex",
        OClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE",
        new String[] {"name"});

    final File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) Assert.assertTrue(backupDir.mkdirs());

    ODocument document = new ODocument("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage");
    db.save(document);

    db.incrementalBackup(backupDir.getAbsolutePath());

    document = new ODocument("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage1");
    db.save(document);

    db.incrementalBackup(backupDir.getAbsolutePath());
    db.close();

    final String backedUpDbDirectory =
        buildDirectory
            + File.separator
            + StorageBackupTestWithLuceneIndex.class.getSimpleName()
            + "BackUp";
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    OrientDBInternal internal = OrientDBInternal.extract(ctx);
    internal.restore(
        dbBackupName,
        null,
        null,
        ODatabaseType.PLOCAL,
        backupDir.getAbsolutePath(),
        OrientDBConfig.defaultConfig());
    final ODatabaseDocumentInternal backedUpDb =
        (ODatabaseDocumentInternal) ctx.open(dbBackupName, "admin", "adminpwd");
    db = (ODatabaseDocumentInternal) ctx.open(dbName, "admin", "adminpwd");

    final ODatabaseCompare compare =
        new ODatabaseCompare(
            db,
            backedUpDb,
            new OCommandOutputListener() {
              @Override
              public void onMessage(String iText) {
                System.out.println(iText);
              }
            });

    Assert.assertTrue(compare.compare());
  }
}
