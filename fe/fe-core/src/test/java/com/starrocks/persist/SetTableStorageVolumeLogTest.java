// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.persist;

import com.starrocks.common.io.Writable;
import com.starrocks.journal.JournalEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class SetTableStorageVolumeLogTest {

    @Test
    public void testSerializeDeserialize() throws IOException {
        long tableId = 12345L;
        String svId = "sv-uuid-001";

        SetTableStorageVolumeLog log = new SetTableStorageVolumeLog(tableId, svId);

        // Serialize
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        log.write(dataOutputStream);

        // Deserialize
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        SetTableStorageVolumeLog deserialized = SetTableStorageVolumeLog.read(dataInputStream);

        Assertions.assertEquals(tableId, deserialized.getTableId());
        Assertions.assertEquals(svId, deserialized.getStorageVolumeId());
    }

    @Test
    public void testJournalEntityRoundTrip() throws IOException {
        long tableId = 99999L;
        String svId = "composite-sv-id-abc";

        SetTableStorageVolumeLog log = new SetTableStorageVolumeLog(tableId, svId);
        JournalEntity journalEntity = new JournalEntity(OperationType.OP_SET_TABLE_STORAGE_VOLUME, log);

        // Serialize as JournalEntity
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        dataOutputStream.writeShort(journalEntity.opCode());
        journalEntity.data().write(dataOutputStream);

        // Deserialize via EditLogDeserializer
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);

        short opCode = dataInputStream.readShort();
        Writable writable = EditLogDeserializer.deserialize(opCode, dataInputStream);
        JournalEntity replayed = new JournalEntity(opCode, writable);

        Assertions.assertEquals(OperationType.OP_SET_TABLE_STORAGE_VOLUME, replayed.opCode());
        Assertions.assertInstanceOf(SetTableStorageVolumeLog.class, replayed.data());

        SetTableStorageVolumeLog replayedLog = (SetTableStorageVolumeLog) replayed.data();
        Assertions.assertEquals(tableId, replayedLog.getTableId());
        Assertions.assertEquals(svId, replayedLog.getStorageVolumeId());
    }
}
