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
    private ReadCache rd;
    private static int maxSegmentSize;
    private static long maxCacheSize;
    private long ledgerId;
    private long entryId;
    private ByteBuf entry;
    private boolean expectedException;

    private static ByteBuf getByteBufOfLen(int len) {
        ByteBufAllocatorBuilder builder = new ByteBufAllocatorBuilderImpl();
        ByteBuf byteBuf = builder.build().heapBuffer(len);
        for (int i = 0; i < len; i++) {
            byteBuf.writeByte(1);
        }
        return byteBuf;
    }


    @BeforeClass
    public static void setEnv() {
        maxSegmentSize = 64;
        maxCacheSize = 64 * 10;
    }

    public ReadCacheGetTest(long ledgerId, long entryId, ByteBuf entry, boolean expectedException) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.entry = entry;
        this.expectedException = expectedException;
    }



    @Parameterized.Parameters
    public static Collection<Object[]> testingSet() {
        setEnv();
        return Arrays.asList(new Object[][] {
                // happy path
                {0, 0, getByteBufOfLen(maxSegmentSize), false},
                {0, 0, getByteBufOfLen(0), false},
        });
    }


    @Before
    public void setUp() throws Exception {
        ByteBufAllocatorBuilder builder = new ByteBufAllocatorBuilderImpl();
        this.rd = new ReadCache(builder.build(), maxCacheSize, maxSegmentSize);
        this.rd.put(this.ledgerId, this.entryId, this.entry);
    }

    @Test
    public void put() {
        try {
        ByteBuf out = rd.get(this.ledgerId, this.entryId);
        assertEquals(this.entry, out);
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