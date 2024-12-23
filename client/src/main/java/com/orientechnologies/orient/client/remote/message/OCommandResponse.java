/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class OCommandResponse implements OBinaryResponse {
  private static final OLogger logger = OLogManager.instance().logger(OCommandResponse.class);
  private final ODatabaseDocumentInternal database;
  private boolean live;
  private Object result;
  private boolean isRecordResultSet;

  public OCommandResponse(
      Object result, boolean isRecordResultSet, ODatabaseDocumentInternal database) {
    this.result = result;
    this.isRecordResultSet = isRecordResultSet;
    this.database = database;
  }

  public OCommandResponse(ODatabaseDocumentInternal database, boolean live) {
    this.database = database;
    this.live = live;
  }

  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {

    serializeValue(channel, result, false, isRecordResultSet, protocolVersion, serializer);
    channel.writeByte((byte) 0); // NO MORE RECORDS
  }

  public void serializeValue(
      OChannelDataOutput channel,
      Object result,
      boolean load,
      boolean isRecordResultSet,
      int protocolVersion,
      ORecordSerializer recordSerializer)
      throws IOException {
    if (result == null) {
      // NULL VALUE
      channel.writeByte((byte) 'n');
    } else if (result instanceof OIdentifiable) {
      // RECORD
      channel.writeByte((byte) 'r');
      if (load && result instanceof ORecordId) result = ((ORecordId) result).getRecord();

      OMessageHelper.writeIdentifiable(channel, (OIdentifiable) result, recordSerializer);
    } else if (!isRecordResultSet) {
      writeSimpleValue(channel, result, protocolVersion, recordSerializer);
    } else if (OMultiValue.isMultiValue(result)) {
      final byte collectionType = result instanceof Set ? (byte) 's' : (byte) 'l';
      channel.writeByte(collectionType);
      channel.writeInt(OMultiValue.getSize(result));
      for (Object o : OMultiValue.getMultiValueIterable(result, false)) {
        try {
          if (load && o instanceof ORecordId) o = ((ORecordId) o).getRecord();

          OMessageHelper.writeIdentifiable(channel, (OIdentifiable) o, recordSerializer);
        } catch (Exception e) {
          logger.warn("Cannot serialize record: %s", e, o);
          OMessageHelper.writeIdentifiable(channel, null, recordSerializer);
          // WRITE NULL RECORD TO AVOID BREAKING PROTOCOL
        }
      }
    } else if (OMultiValue.isIterable(result)) {
      if (protocolVersion >= OChannelBinaryProtocol.PROTOCOL_VERSION_32) {
        channel.writeByte((byte) 'i');
        for (Object o : OMultiValue.getMultiValueIterable(result)) {
          try {
            if (load && o instanceof ORecordId) o = ((ORecordId) o).getRecord();

            channel.writeByte((byte) 1); // ONE MORE RECORD
            OMessageHelper.writeIdentifiable(channel, (OIdentifiable) o, recordSerializer);
          } catch (Exception e) {
            logger.warn("Cannot serialize record: %s", e, o);
          }
        }
        channel.writeByte((byte) 0); // NO MORE RECORD
      } else {
        // OLD RELEASES: TRANSFORM IN A COLLECTION
        final byte collectionType = result instanceof Set ? (byte) 's' : (byte) 'l';
        channel.writeByte(collectionType);
        channel.writeInt(OMultiValue.getSize(result));
        for (Object o : OMultiValue.getMultiValueIterable(result)) {
          try {
            if (load && o instanceof ORecordId) o = ((ORecordId) o).getRecord();

            OMessageHelper.writeIdentifiable(channel, (OIdentifiable) o, recordSerializer);
          } catch (Exception e) {
            logger.warn("Cannot serialize record:%s ", e, o);
          }
        }
      }

    } else {
      // ANY OTHER (INCLUDING LITERALS)
      writeSimpleValue(channel, result, protocolVersion, recordSerializer);
    }
  }

  private void writeSimpleValue(
      OChannelDataOutput channel,
      Object result,
      int protocolVersion,
      ORecordSerializer recordSerializer)
      throws IOException {

    if (protocolVersion >= OChannelBinaryProtocol.PROTOCOL_VERSION_35) {
      channel.writeByte((byte) 'w');
      ODocument document = new ODocument();
      document.field("result", result);
      OMessageHelper.writeIdentifiable(channel, document, recordSerializer);
    } else {
      channel.writeByte((byte) 'a');
      final StringBuilder value = new StringBuilder(64);
      ORecordSerializerStringAbstract.fieldTypeToString(
          value, OType.getTypeByClass(result.getClass()), result);
      channel.writeString(value.toString());
    }
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    try {
      // Collection of prefetched temporary record (nested projection record), to refer for avoid
      // garbage collection.
      List<ORecord> temporaryResults = new ArrayList<ORecord>();

      result = readSynchResult(network, database, temporaryResults);
      if (live) {
        final ODocument doc = ((List<ODocument>) result).get(0);
        final Integer token = doc.field("token");
        final Boolean unsubscribe = doc.field("unsubscribe");
        if (token != null) {
        } else {
          throw new OStorageException("Cannot execute live query, returned null token");
        }
      }
    } finally {
      // TODO: this is here because we allow query in end listener.
      session.commandExecuting = false;
    }
  }

  protected Object readSynchResult(
      final OChannelDataInput network,
      final ODatabaseDocument database,
      List<ORecord> temporaryResults)
      throws IOException {
    ORecordSerializer serializer = ORecordSerializerNetworkV37Client.INSTANCE;
    final Object result;

    final byte type = network.readByte();
    switch (type) {
      case 'n':
        result = null;
        break;

      case 'r':
        result = OMessageHelper.readIdentifiable(network, serializer);
        if (result instanceof ORecord) database.getLocalCache().updateRecord((ORecord) result);
        break;

      case 'l':
      case 's':
        final int tot = network.readInt();
        final Collection<OIdentifiable> coll;

        coll = new HashSet<OIdentifiable>(tot);
        for (int i = 0; i < tot; ++i) {
          final OIdentifiable resultItem = OMessageHelper.readIdentifiable(network, serializer);
          if (resultItem instanceof ORecord)
            database.getLocalCache().updateRecord((ORecord) resultItem);
          coll.add(resultItem);
        }

        result = coll;
        break;
      case 'i':
        coll = new HashSet<OIdentifiable>();
        byte status;
        while ((status = network.readByte()) > 0) {
          final OIdentifiable record = OMessageHelper.readIdentifiable(network, serializer);
          if (record == null) continue;
          if (status == 1) {
            if (record instanceof ORecord) database.getLocalCache().updateRecord((ORecord) record);
            coll.add(record);
          }
        }
        result = coll;
        break;
      case 'w':
        final OIdentifiable record = OMessageHelper.readIdentifiable(network, serializer);
        // ((ODocument) record).setLazyLoad(false);
        result = ((ODocument) record).field("result");
        break;

      default:
        logger.warn("Received unexpected result from query: %d", type);
        result = null;
    }

    // LOAD THE FETCHED RECORDS IN CACHE
    byte status;
    while ((status = network.readByte()) > 0) {
      final ORecord record = (ORecord) OMessageHelper.readIdentifiable(network, serializer);
      if (record != null && status == 2) {
        // PUT IN THE CLIENT LOCAL CACHE
        database.getLocalCache().updateRecord(record);
        if (record.getIdentity().getClusterId() == -2) temporaryResults.add(record);
      }
    }

    return result;
  }

  public Object getResult() {
    return result;
  }
}
