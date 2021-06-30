package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.impl.ByteBufAllocatorBuilderImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ReadCacheArbitraryPutTest {
    private int maxSegmentSize;
    private long maxCacheSize;
    private int segmentsCount;
    private int segmentSize;
    private static ByteBufAllocatorBuilder builder;
    private ReadCache rd;
    private int ledgerId;
    private int noEntry;
    private int len;
    private boolean expectedException;

    public ReadCacheArbitraryPutTest(int maxSegmentSize, long maxCacheSize,
                                     int ledgerId, int noEntry, int len, boolean expectedException) {
        this.maxSegmentSize = maxSegmentSize;
        this.maxCacheSize = maxCacheSize;
        this.ledgerId = ledgerId;
        this.noEntry = noEntry;
        this.len = len;
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
                // {int maxSegmentSize, long maxCacheSize, int ledgerId, int noEntry, int len boolean expectedException}
                {64, 64 * 10, 0, 10, 64, false},
                {64, 128, 0, 3, 64, false},
                {64, 128, 0, 10, 60, false}

        });
    }


    @Before
    public void setUp() throws Exception {
        builder = new ByteBufAllocatorBuilderImpl();
        segmentsCount = Math.max(2, (int) (maxCacheSize / maxSegmentSize));
        segmentSize = (int) (maxCacheSize / segmentsCount);
        rd = new ReadCache(builder.build(), maxCacheSize, maxSegmentSize);
    }

    @Test
    public void put() {
        try {
            int len = (int) (noEntry > 0 ? Math.min(maxCacheSize / noEntry, segmentSize) : segmentSize);
            for (int i = 0; i < 2 * noEntry; i++) {
                rd.put(ledgerId, i, getByteBufOfLen(len));
            }
            assertNotNull(rd.get(ledgerId, 2 * noEntry - 1));
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