package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.impl.ByteBufAllocatorBuilderImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class WriteCacheGetTest {

    private WriteCache wr;
    private ByteBufAllocator byteBufAllocator;
    @Before
    public void setUp() throws Exception {
        ByteBufAllocatorBuilder builder = new ByteBufAllocatorBuilderImpl();
        byteBufAllocator = builder.build();
        int maxCacheSize = 1024;
        wr =  new WriteCache(byteBufAllocator, maxCacheSize);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void get() {
    }
}