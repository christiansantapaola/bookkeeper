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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class WriteCacheGetLastEntryTest {
    private WriteCache wr;
    private static ByteBufAllocatorBuilder builder = new ByteBufAllocatorBuilderImpl();
    private static int maxSegmentsSize = 64;
    private static int maxCacheSize = 2 * maxSegmentsSize;
    private long ledgerId;
    private long noEntry;
    private ByteBuf lastEntry;
    private boolean expectedResult;
    private boolean expectedException;


    public WriteCacheGetLastEntryTest(long noEntry ,boolean expectedResult, boolean expectedException) {
        this.noEntry = noEntry;
        this.expectedResult = expectedResult;
        this.expectedException = expectedException;
        this.ledgerId = 0;
    }

    private ByteBuf getByteBufOfLen(int len) {
        ByteBuf byteBuf = builder.build().heapBuffer(len);
        for (int i = 0; i < len; i++) {
            byteBuf.writeByte(1);
        }
        return byteBuf;
    }



    @Parameterized.Parameters
    public static Collection<Object[]> testingSet() {
        return Arrays.asList(new Object[][] {
                // {noEntry, expectedResult, expectedException}
                // no insert, expected to fail
                {0, false, false},
                // 1 insert, expected to succeed
                {1, true, false},
                // 2 insert expected to succeed
                {2, true, false},
                {3, true, false},
        });
    }

    @Before
    public void setUp() throws Exception {
        maxCacheSize = Math.max((int) noEntry * maxSegmentsSize, maxSegmentsSize);
        wr =  new WriteCache(builder.build(), maxCacheSize, maxSegmentsSize);
        int len = 0;
        if (noEntry == 0) {
            len = maxSegmentsSize;
            lastEntry = null;
        } else {
            len = (int) Math.min(Math.max(maxCacheSize, maxSegmentsSize) / noEntry, maxSegmentsSize);
        }
        for (int i = 0; i < noEntry; i++) {
            lastEntry = getByteBufOfLen(len);
            boolean res = wr.put(ledgerId, i, lastEntry);
            assertTrue(res);
        }
        assertEquals(len * noEntry, wr.size());
    }

    @Test
    public void getLastEntry() {
        assertEquals(noEntry, wr.count());
        ByteBuf entry = wr.getLastEntry(ledgerId);
        assertEquals(lastEntry, entry);
    }


    @After
    public void tearDown() throws Exception {
        wr.clear();
        wr.close();
    }




}
