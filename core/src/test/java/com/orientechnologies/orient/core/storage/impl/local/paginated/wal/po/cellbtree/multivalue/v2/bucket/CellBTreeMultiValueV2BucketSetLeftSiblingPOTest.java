package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2Bucket;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class CellBTreeMultiValueV2BucketSetLeftSiblingPOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false, Intention.TEST);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false, null);

      CellBTreeMultiValueV2Bucket<Byte> bucket = new CellBTreeMultiValueV2Bucket<>(entry);
      bucket.init(true);

      bucket.setLeftSibling(42);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false, Intention.TEST);
      final OCachePointer restoredCachePointer =
          new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry =
          new OCacheEntryImpl(0, 0, restoredCachePointer, false, null);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.setLeftSibling(24);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2BucketSetLeftSiblingPO);

      final CellBTreeMultiValueV2BucketSetLeftSiblingPO pageOperation =
          (CellBTreeMultiValueV2BucketSetLeftSiblingPO) operations.get(0);

      CellBTreeMultiValueV2Bucket<Byte> restoredBucket =
          new CellBTreeMultiValueV2Bucket<>(restoredCacheEntry);

      Assert.assertEquals(42, restoredBucket.getLeftSibling());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(24, restoredBucket.getLeftSibling());

      byteBufferPool.release(pointer);
      byteBufferPool.release(restoredPointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testUndo() {
    final int pageSize = 64 * 1024;

    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false, Intention.TEST);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false, null);

      CellBTreeMultiValueV2Bucket<Byte> bucket = new CellBTreeMultiValueV2Bucket<>(entry);
      bucket.init(true);

      bucket.setLeftSibling(42);

      entry.clearPageOperations();

      bucket.setLeftSibling(24);

      final List<PageOperationRecord> operations = entry.getPageOperations();

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2BucketSetLeftSiblingPO);
      final CellBTreeMultiValueV2BucketSetLeftSiblingPO pageOperation =
          (CellBTreeMultiValueV2BucketSetLeftSiblingPO) operations.get(0);

      final CellBTreeMultiValueV2Bucket<Byte> restoredBucket =
          new CellBTreeMultiValueV2Bucket<>(entry);

      Assert.assertEquals(24, restoredBucket.getLeftSibling());

      pageOperation.undo(entry);

      Assert.assertEquals(42, restoredBucket.getLeftSibling());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    CellBTreeMultiValueV2BucketSetLeftSiblingPO operation =
        new CellBTreeMultiValueV2BucketSetLeftSiblingPO(12, 21);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeMultiValueV2BucketSetLeftSiblingPO restoredOperation =
        new CellBTreeMultiValueV2BucketSetLeftSiblingPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(12, restoredOperation.getSibling());
    Assert.assertEquals(21, restoredOperation.getPrevSibling());
  }
}
