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
public class WriteCacheGetTest {
    private WriteCache wr;
    private static ByteBufAllocatorBuilder builder = new ByteBufAllocatorBuilderImpl();
    private static int maxSegmentsSize = 64;
    private static int maxCacheSize = 128;
    private long ledgerId;
    private long entryId;
    private ByteBuf entry;
    private boolean doPut;
    private boolean expectedResult;
    private boolean expectedException;


    public WriteCacheGetTest(long ledgerId, long entryId, ByteBuf entry, boolean doPut, boolean expectedResult, boolean expectedException) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.entry = entry;
        this.doPut = doPut;
        this.expectedResult = expectedResult;
        this.expectedException = expectedException;
    }


    @Parameterized.Parameters
    public static Collection<Object[]> testingSet() {
        return Arrays.asList(new Object[][] {
                // {ledgerId, entryId, ByteBuf, doPut, expectedResult, expectedException}
                // happy path, valid ids, valid entry, insert then retrieve.
                {0, 0, getByteBufOfLen(maxSegmentsSize), true, true, false},
                // entry equal to null, first insert then retrieve, expected NullPointerException in put.
                {0, 0, null, true, true, true},
                // entry equal to null, do not insert and try to retrieve, expected get to return true and null.
                {0, 0, null, false, true, false},
                // insert valid entry of length 0. NB: the put succeed but isEmpty return true.
                {0, 0, getByteBufOfLen(0), true, true, false},
                // try to retrieve something that wasn't inserted before.
                {0, 0, getByteBufOfLen(maxSegmentsSize), false, false, false}
        });
    }

    private static ByteBuf getByteBufOfLen(int len) {
        ByteBuf byteBuf = builder.build().heapBuffer(len);
        for (int i = 0; i < len; i++) {
            byteBuf.writeByte(1);
        }
        return byteBuf;
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
    public void get() {
        try {
            if (doPut) {
                assertTrue(wr.isEmpty());
                boolean result = wr.put(ledgerId, entryId, entry);
                assertTrue(result);
                if (result) {
                    if (entry.readableBytes() != 0) {
                        assertFalse(wr.isEmpty());
                    }
                    assertEquals(1, wr.count());
                    assertEquals(entry.readableBytes(), wr.size());
                } else {
                    assertTrue(wr.isEmpty());
                    assertEquals(0, wr.count());
                    assertEquals(0, wr.size());
                }
            }
            ByteBuf entryResult = wr.get(ledgerId, entryId);
            if (entryResult == null) {
                assertEquals(expectedResult, entry == null);
            } else {
                assertEquals(expectedResult, entryResult.equals(entry));
            }
        } catch (Exception e) {
            assertTrue(expectedException);
        }
    }
}