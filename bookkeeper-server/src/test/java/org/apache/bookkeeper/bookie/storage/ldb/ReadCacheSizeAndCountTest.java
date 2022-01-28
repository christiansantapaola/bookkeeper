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
public class ReadCacheSizeAndCountTest {
    private int maxSegmentSize;
    private long maxCacheSize;
    private int entrySize;
    private int numPut;
    private int expectedSize;
    private int expectedCount;
    private boolean expectedException;
    private static ByteBufAllocatorBuilder builder;
    private ReadCache rd;

    public ReadCacheSizeAndCountTest(int maxSegmentSize,
                                     long maxCacheSize,
                                     int entrySize,
                                     int numPut,
                                     int expectedSize,
                                     int expectedCount,
                                     boolean expectedException) {
        this.maxSegmentSize = maxSegmentSize;
        this.maxCacheSize = maxCacheSize;
        this.entrySize = entrySize;
        this.numPut = numPut;
        this.expectedSize = expectedSize;
        this.expectedCount = expectedCount;
        this.expectedException = expectedException;
    }



    private static ByteBuf getByteBufOfLen(int len) {
        ByteBuf byteBuf = builder.build().heapBuffer(len);
        for (int i = 0; i < len; i++) {
            byteBuf.writeByte(1);
        }
        return byteBuf;
    }


    /*
    * int maxSegmentSize, long maxCacheSize, int entrySize, int numPut, int expectedSize, int expectedCount, boolean expectedException
    */

    @Parameterized.Parameters
    public static Collection<Object[]> testingSet() {
        return Arrays.asList(new Object[][] {
                {64, 64 * 2, 64, 2, 64 * 2, 2, false},
                {64, 64 * 2, 32, 4, 64 * 2, 2, false},
                {64, 64 * 2, 64, 10, 64 * 2, 2, false},
                {64, 64 * 2, 0, 2, 0, 2, false},
        });
    }


    @Before
    public void setUp() throws Exception {
        builder = new ByteBufAllocatorBuilderImpl();
        rd = new ReadCache(builder.build(), maxCacheSize, maxSegmentSize);
    }

    @Test
    public void test() {
        try {
            assertTrue(entrySize <= maxSegmentSize);
            assertTrue(maxSegmentSize <= maxCacheSize);
            for (int i = 0; i < numPut; i++) {
                ByteBuf buf = getByteBufOfLen(entrySize);
                rd.put(0, i, buf);
            }
            assertEquals(expectedCount, rd.count());
            assertEquals(expectedSize, rd.size());
        } catch (Exception e) {
            System.err.println(e.getMessage());
            assertTrue(expectedException);
        }
    }


    @After
    public void tearDown() throws Exception {
        rd.close();
    }
}