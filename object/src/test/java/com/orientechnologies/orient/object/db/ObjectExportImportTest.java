package com.orientechnologies.orient.object.db;

import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

/** Created by tglman on 23/12/15. */
public class ObjectExportImportTest extends BaseObjectTest {

  @Test
  public void testExportImport() throws IOException {

    try {
      database.setAutomaticSchemaGeneration(true);
      database.getMetadata().getSchema().synchronizeSchema();

      assertNotNull(database.getMetadata().getSchema().getClass("OIdentity"));
      byte[] bytes;
      ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
      new ODatabaseExport(
              (ODatabaseDocumentInternal) database.getUnderlying(),
              byteOutputStream,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              })
          .exportDatabase()
          .close();
      database.close();
      bytes = byteOutputStream.toByteArray();
      ctx.execute(
          "create database "
              + getDatabaseName()
              + "1 memory users(admin identified by 'adminpwd' role admin)");
      ODatabaseObject db1 = ctx.open(getDatabaseName() + "1", "admin", "adminpwd");
      db1.setAutomaticSchemaGeneration(true);
      db1.getMetadata().getSchema().synchronizeSchema();
      InputStream input = new ByteArrayInputStream(bytes);
      new ODatabaseImport(
              (ODatabaseDocumentInternal) db1.getUnderlying(),
              input,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              })
          .importDatabase()
          .close();
      assertNotNull(db1.getMetadata().getSchema().getClass("OIdentity"));
      db1.close();
    } finally {
      ctx.drop(getDatabaseName() + "1");
    }
  }
}
