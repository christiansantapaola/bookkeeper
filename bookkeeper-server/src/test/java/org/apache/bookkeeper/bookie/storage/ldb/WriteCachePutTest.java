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
public class WriteCachePutTest {

    private WriteCache wr;
    private static ByteBufAllocatorBuilder builder = new ByteBufAllocatorBuilderImpl();
    private static int maxSegmentsSize = 64;
    private static int maxCacheSize = 128;
    private long ledgerId;
    private long entryId;
    private ByteBuf entry;
    private boolean expectedResult;
    private boolean expectedException;

    public WriteCachePutTest(long ledgerId, long entryId, ByteBuf entry, boolean expectedResult, boolean expectedException) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.entry = entry;
        this.expectedResult = expectedResult;
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
                // happy path
                {0, 0, getByteBufOfLen(maxSegmentsSize), true, false},
                // ledgerId < 0, expected exception.
                {-1, 0, getByteBufOfLen(maxSegmentsSize), false, true},
                // entryId < 0, expected exception.
                {0, -1, getByteBufOfLen(maxSegmentsSize), false, true},
                // ledgerId and entryId as MAX_VALUE, see if increment create problem, expected to succeed.
                {Long.MAX_VALUE, Long.MAX_VALUE, getByteBufOfLen(maxSegmentsSize), true, false},
                // ledgerId and entryId as MIN_VALUE, see if decrement create problem, expected to succeed.
                {Long.MIN_VALUE, Long.MIN_VALUE, getByteBufOfLen(maxSegmentsSize), false, true},
                // entry is null, expected to fail with exception.
                {0, 0, null, false, true},
                // empty buffer
                {0, 0, getByteBufOfLen(0), true, false},
                // entry with size bigger than maxSegmentSize, expected to fail.
                {0, 0, getByteBufOfLen(maxSegmentsSize + 1), false, false},
        });
    }

    @Before
    public void setUp() throws Exception {
        wr =  new WriteCache(builder.build(), maxCacheSize, maxSegmentsSize);
    }

    @Test
    public void put() {
        try {
            assertTrue(wr.isEmpty());
            assertEquals(0, wr.count());
            assertEquals(0, wr.size());
            boolean result = wr.put(ledgerId, entryId, entry);
            assertEquals(expectedResult, result);
            if (expectedResult) {
                assertEquals(entry.readableBytes(), wr.size());
                assertEquals(1, wr.count());
                assertEquals(entry.readableBytes(), wr.size());
            }
        } catch (NullPointerException | IllegalArgumentException e) {
            assertTrue(expectedException);
        }
    }

    @After
    public void tearDown() throws Exception {
        wr.clear();
        wr.close();
    }


}