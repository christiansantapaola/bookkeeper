package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.impl.ByteBufAllocatorBuilderImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class WriteCacheMultiplePutTest {
    private WriteCache wr;
    private static ByteBufAllocatorBuilder builder = new ByteBufAllocatorBuilderImpl();
    private static int maxSegmentsSize = 64;
    private static int maxCacheSize = 2 * maxSegmentsSize - 1;
    private long ledgerId_1;
    private long entryId_1;
    private ByteBuf entry_1;
    private long ledgerId_2;
    private long entryId_2;
    private ByteBuf entry_2;
    private boolean expectedResult_1;
    private boolean expectedResult_2;
    private boolean expectedException;

    public WriteCacheMultiplePutTest(long ledgerId_1, long entryId_1, ByteBuf entry_1, long ledgerId_2, long entryId_2, ByteBuf entry_2, boolean expectedResult_1, boolean expectedResult_2, boolean expectedException) {
        this.ledgerId_1 = ledgerId_1;
        this.entryId_1 = entryId_1;
        this.entry_1 = entry_1;
        this.ledgerId_2 = ledgerId_2;
        this.entryId_2 = entryId_2;
        this.entry_2 = entry_2;
        this.expectedResult_1 = expectedResult_1;
        this.expectedResult_2 = expectedResult_2;
        this.expectedException = expectedException;
    }

    private static ByteBuf getByteBufOfLen(int len) {
        ByteBuf byteBuf = builder.build().heapBuffer(len);
        for (int i = 0; i < len; i++) {
            byteBuf.writeByte(1);
        }
        return byteBuf;
    }



    @Parameterized.Parameters
    public static Collection<Object[]> testingSet() {
        return Arrays.asList(new Object[][] {
                // {long ledgerId_1, long entryId_1, ByteBuf entry_1,
                // long ledgerId_2, long entryId_2, ByteBuf entry_2,
                // boolean expectedResult_1, boolean expectedResult_2, boolean expectedException}
                // same ledgerId and entryId, expected both to succeed
                {0, 0, getByteBufOfLen(maxSegmentsSize / 2), 0, 0, getByteBufOfLen(maxSegmentsSize / 2 + 1), true, true, false},
                // different ledgerId and same entryId, expected both to succeed
                {0, 1, getByteBufOfLen(maxSegmentsSize / 2), 1, 1, getByteBufOfLen(maxSegmentsSize / 2 + 1), true, true, false},
                // different ledgerId and same entryId, insert out of order, expected both to succeed
                {1, 1, getByteBufOfLen(maxSegmentsSize / 2), 0, 0, getByteBufOfLen(maxSegmentsSize / 2 + 1), true, true, false},
                // same ledgerId, different entry id, insert out of order, expected both to succeed
                {0, 1, getByteBufOfLen(maxSegmentsSize / 2), 0, 0, getByteBufOfLen(maxSegmentsSize / 2 + 1), true, true, false},
                // same ledgerId, different entry id, insert in order, happy path, expected both to succeed
                {0, 0, getByteBufOfLen(maxSegmentsSize / 2), 0, 1, getByteBufOfLen(maxSegmentsSize / 2), true, true, false},
                // same ledgerId, different entry id, insert in order, the two entry have the size equals to maxCacheSize, expected both to succeed
                {0, 0, getByteBufOfLen(maxSegmentsSize), 0, 1, getByteBufOfLen(maxSegmentsSize - 1), true, true, false},
                // different ledgerId, different entry id, insert in order, the two entry have the size equals to maxCacheSize, expected both to succeed
                {0, 0, getByteBufOfLen(maxSegmentsSize), 1, 1, getByteBufOfLen(maxSegmentsSize - 1), true, true, false},
                // different ledgerId, different entry id, insert in order, the two entry have the size equals to maxCacheSize (but the bigger is the last), the second put expected to fail
                {0, 0, getByteBufOfLen(maxSegmentsSize - 1), 0, 1, getByteBufOfLen(maxSegmentsSize), true, false, false},
                // same ledgerId, different entry id, insert in order, the two entry have the size greater than to maxCacheSize, the second put expected to fail
                {0, 0, getByteBufOfLen(maxSegmentsSize), 0, 1, getByteBufOfLen(maxSegmentsSize), true, false, false},
        });
    }

    @Before
    public void setUp() throws Exception {
        wr =  new WriteCache(builder.build(), maxCacheSize, maxSegmentsSize);
    }

    @After
    public void tearDown() throws Exception {
        wr.clear();
        wr.close();
    }

    @Test
    public void put() {
        try {
            assertTrue(wr.isEmpty());
            boolean result_1 = wr.put(ledgerId_1, entryId_1, entry_1);
            assertEquals( "expected result 1: ",expectedResult_1, result_1);
            if (expectedResult_1) {
                assertFalse("isEmpty(): ", wr.isEmpty());
                assertEquals("count(): ", 1, wr.count());
                assertEquals(entry_1.readableBytes(), wr.size());
            }
            boolean result_2 = wr.put(ledgerId_2, entryId_2, entry_2);
            assertEquals(expectedResult_2, result_2);
            if (expectedResult_2) {
                assertFalse("isEmpty():", wr.isEmpty());
                assertEquals("count(): ", 2, wr.count());
                assertEquals("size(): ", entry_1.readableBytes() + entry_2.readableBytes(), wr.size());
            }
            assertFalse(expectedException);
        } catch (NullPointerException | IllegalArgumentException e) {
            assertTrue(expectedException);
        }
    }
}