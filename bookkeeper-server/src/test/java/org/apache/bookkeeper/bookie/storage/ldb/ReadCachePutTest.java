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
public class ReadCachePutTest {
    private static int maxSegmentSize;
    private static long maxCacheSize;
    private static int segmentsCount;
    private static int segmentSize;
    private static ByteBufAllocatorBuilder builder;
    private ReadCache rd;
    private long ledgerId;
    private long entryId;
    private ByteBuf entry;
    private boolean expectedException;

    public ReadCachePutTest(long ledgerId, long entryId, ByteBuf entry, boolean expectedException) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.entry = entry;
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
        maxCacheSize = maxSegmentSize * 2;
        builder = new ByteBufAllocatorBuilderImpl();
        segmentsCount = Math.max(2, (int) (maxCacheSize / maxSegmentSize));
        segmentSize = (int) (maxCacheSize / segmentsCount);

    }

    @Parameterized.Parameters
    public static Collection<Object[]> testingSet() {
        setEnv();
        return Arrays.asList(new Object[][] {
                // happy path
                {0, 0, getByteBufOfLen(segmentSize), false},
                // entryId < 0, expected to succeed
                {0, -1, getByteBufOfLen(segmentSize), false},
                // ledgerId < 0 expected to fail.
                {-1, 0, getByteBufOfLen(segmentSize), true},
                // valid Id, entry null, expected exception.
                {0, 0, null, true},
                // valid id, empty buffer, expected to succeed.
                {0, 0, getByteBufOfLen(0), false},
                // valid id, buffer with size bigger than segmentSize, expected to fail.
                {0, 0, getByteBufOfLen(segmentSize + 1), true},
        });
    }


    @Before
    public void setUp() throws Exception {
        rd = new ReadCache(builder.build(), maxCacheSize, maxSegmentSize);
    }

    @Test
    public void put() {
        try {
            rd.put(ledgerId, entryId, entry);
            assertEquals(1, rd.count());
            assertEquals(entry.readableBytes(), rd.size());
            ByteBuf result = rd.get(ledgerId, entryId);
            assertEquals(entry, result);
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