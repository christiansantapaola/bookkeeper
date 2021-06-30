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
public class ReadCacheGetTest {
    private static int maxSegmentSize;
    private static long maxCacheSize;
    private static int segmentsCount;
    private static int segmentSize;
    private static ByteBufAllocatorBuilder builder;
    private ReadCache rd;
    private long ledgerId;
    private long entryId;
    private ByteBuf entry;
    private boolean doInsert;
    private boolean expectedException;

    private ByteBuf getByteBufOfLen(int len) {
        ByteBuf byteBuf = builder.build().heapBuffer(len);
        for (int i = 0; i < len; i++) {
            byteBuf.writeByte(1);
        }
        return byteBuf;
    }


    public ReadCacheGetTest(long ledgerId, long entryId, boolean doInsert, boolean expectedException) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.doInsert = doInsert;
        this.expectedException = expectedException;
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
                // valid Id,  expected exception.
                {0, 0,  true, true},
                // valid id,  no inserted before expected to fail.
                {0, 0, false, false},
        });
    }


    @Before
    public void setUp() throws Exception {
        rd = new ReadCache(builder.build(), maxCacheSize, maxSegmentSize);
        entry = getByteBufOfLen(segmentSize);
    }

    @Test
    public void put() {
        try {
            if (doInsert) {
                rd.put(ledgerId, entryId, entry);
                assertEquals(1, rd.count());
                assertEquals(entry.readableBytes(), rd.size());
            } else {
                entry = null;
            }
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