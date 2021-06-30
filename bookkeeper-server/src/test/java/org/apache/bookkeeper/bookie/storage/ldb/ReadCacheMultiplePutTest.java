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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ReadCacheMultiplePutTest {
    private static int maxSegmentSize;
    private static long maxCacheSize;
    private static int segmentsCount;
    private static int segmentSize;
    private static ByteBufAllocatorBuilder builder;
    private ReadCache rd;
    private int ledgerId_1;
    private int entryId_1;
    private ByteBuf entry_1;
    private int ledgerId_2;
    private int entryId_2;
    private ByteBuf entry_2;
    private boolean expectedException;

    public ReadCacheMultiplePutTest(int ledgerId_1, int entryId_1, ByteBuf entry_1,
                                    int ledgerId_2, int entryId_2, ByteBuf entry_2,
                                    boolean expectedException) {
        this.ledgerId_1 = ledgerId_1;
        this.entryId_1 = entryId_1;
        this.entry_1 = entry_1;
        this.ledgerId_2 = ledgerId_2;
        this.entryId_2 = entryId_2;
        this.entry_2 = entry_2;
        this.expectedException = expectedException;
    }

    private static ByteBuf getByteBufOfLen(int len) {
        ByteBuf byteBuf = builder.build().heapBuffer(len);
        for (int i = 0; i < len; i++) {
            byteBuf.writeByte(1);
        }
        return byteBuf;
    }


    @BeforeClass
    public static void setEnv() {
        maxSegmentSize = 64;
        maxCacheSize = maxSegmentSize * 2 - 1;
        builder = new ByteBufAllocatorBuilderImpl();
        segmentsCount = Math.max(2, (int) (maxCacheSize / maxSegmentSize));
        segmentSize = (int) (maxCacheSize / segmentsCount);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testingSet() {
        setEnv();
        return Arrays.asList(new Object[][] {
                // {int ledgerId, int noEntry, boolean expectedException}
                // valid ids, entries of size equal to maxCacheSize, expected to succeed.
                {0, 0, getByteBufOfLen(segmentSize), 0, 1, getByteBufOfLen(segmentSize - 1), false},
                // valid ids, entries of size equal to maxCacheSize, insert out of order, expected to succeed.
                {0, 1, getByteBufOfLen(segmentSize), 0, 0, getByteBufOfLen(segmentSize - 1), false},
                // valid ids, entries of size equal to maxCacheSize, different ledgerId, expected to succeed.
                {0, 0, getByteBufOfLen(segmentSize), 1, 0, getByteBufOfLen(segmentSize - 1), false},
                // valid ids, entries of size bigger than maxCacheSize, expected to fail
                {0, 0, getByteBufOfLen(segmentSize), 0, 1, getByteBufOfLen(segmentSize), true},
                // valid ids, entries of size equal to maxCacheSize, biggest is last, expected to fail.
                {0, 0, getByteBufOfLen(segmentSize - 1), 0, 1, getByteBufOfLen(segmentSize), true},
                {0, 0, getByteBufOfLen(segmentSize / 2), 0, 1, getByteBufOfLen(segmentSize), false},

        });
    }


    @Before
    public void setUp() throws Exception {
        rd = new ReadCache(builder.build(), maxCacheSize, maxSegmentSize);
    }

    @Test
    public void put() {
        try {
            rd.put(ledgerId_1, entryId_1, entry_1);
            rd.put(ledgerId_2, entryId_2, entry_2);
            assertEquals(2, rd.count());
            assertTrue(entry_1.readableBytes() + entry_2.readableBytes() <= rd.size());
            ByteBuf get_entry_1 = rd.get(ledgerId_1, entryId_1);
            ByteBuf get_entry_2 = rd.get(ledgerId_2, entryId_2);
            assertEquals(entry_1, get_entry_1);
            assertEquals(entry_2, get_entry_2);

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