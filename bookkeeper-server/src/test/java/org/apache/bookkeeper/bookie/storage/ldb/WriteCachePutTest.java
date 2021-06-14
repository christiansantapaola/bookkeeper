package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.impl.ByteBufAllocatorBuilderImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class WriteCachePutTest {

    private WriteCache wr;
    private static ByteBufAllocatorBuilder builder = new ByteBufAllocatorBuilderImpl();;
    private static int maxCacheSize;
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

    @Parameterized.Parameters
    public static Collection<Object[]> testingSet() {
        ByteBufAllocator entryAllocator = builder.build();
        return Arrays.asList(new Object[][] {
                {1, 0, entryAllocator.buffer(), true, false},
                {0, 1, entryAllocator.buffer(), true, false},
                {1, 1, entryAllocator.buffer(), true, false},
                {-1, 0, entryAllocator.buffer(), false, true},
                {0, -1, entryAllocator.buffer(), false, true},
                {-1, -1, entryAllocator.buffer(), false, true},
                {0, 0, null, false, true},
                {0, 0, entryAllocator.ioBuffer(), true, false},
                {0, 0, entryAllocator.compositeBuffer(), true, false},
                {0, 0, entryAllocator.directBuffer(), true, false},
                {0, 0, entryAllocator.heapBuffer(), true, false},
        });
    }

    @BeforeClass
    static public void setEnvUp() throws Exception {
        maxCacheSize = 1024;
    }

    @Before
    public void setUp() throws Exception {
        wr =  new WriteCache(builder.build(), maxCacheSize);
    }

    @After
    public void tearDown() throws Exception {
        wr.clear();
        wr.close();
    }

    @Test
    public void put() {
        try {
            boolean result = wr.put(ledgerId, entryId, entry);
            assertEquals(result, expectedResult);
            assertFalse(expectedException);
        } catch (NullPointerException | IllegalArgumentException e) {
            assertTrue(expectedException);
        }
    }
}