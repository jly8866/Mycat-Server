package io.mycat.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DirectByteBuffer池，可以分配任意指定大小的DirectByteBuffer，用完需要归还
 * @author wuzhih
 * @author zagnix
 */
@SuppressWarnings("restriction")
public class DirectByteBufferPool implements BufferPool{
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectByteBufferPool.class);
    public static final String LOCAL_BUF_THREAD_PREX = "$_";
    private final ByteBufferPage[] allPages;
    private final int chunkSize;
    // private int prevAllocatedPage = 0;
    private final AtomicLong prevAllocatedPage;
    private final  int pageSize;
    private final short pageCount;
    private final int conReadBuferChunk ;
    /**
     * 记录对线程ID->该线程的所使用Direct Buffer的size
     */
    private final ConcurrentHashMap<Long,Long> memoryUsage;

    public DirectByteBufferPool(final int pageSize, final short chunkSize, final short pageCount,final int conReadBuferChunk) {
        allPages = new ByteBufferPage[pageCount];
        this.chunkSize = chunkSize;
        this.pageSize = pageSize;
        this.pageCount = pageCount;
        this.conReadBuferChunk = conReadBuferChunk;
        prevAllocatedPage = new AtomicLong(0);
        for (int i = 0; i < pageCount; i++) {
            allPages[i] = new ByteBufferPage(ByteBuffer.allocateDirect(pageSize), chunkSize);
        }
        memoryUsage = new ConcurrentHashMap<>();
    }

    @Override
    public BufferArray allocateArray() {
        return new BufferArray(this);
    }
    /**
     * TODO 当页不够时，考虑扩展内存池的页的数量...........
     * @param buffer
     * @return
     */
    public  ByteBuffer expandBuffer(final ByteBuffer buffer){
        final int oldCapacity = buffer.capacity();
        final int newCapacity = oldCapacity << 1;
        final ByteBuffer newBuffer = allocate(newCapacity);
        if(newBuffer != null){
            final int newPosition = buffer.position();
            buffer.flip();
            newBuffer.put(buffer);
            newBuffer.position(newPosition);
            recycle(buffer);
            return  newBuffer;
        }
        return null;
    }

    @Override
    public ByteBuffer allocate(final int size) {
        final int theChunkCount = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
        final int selectedPage =  (int)(prevAllocatedPage.incrementAndGet() % allPages.length);
        ByteBuffer byteBuf = allocateBuffer(theChunkCount, 0, selectedPage);
        if (byteBuf == null) {
            byteBuf = allocateBuffer(theChunkCount, selectedPage, allPages.length);
        }
        final long threadId = Thread.currentThread().getId();

        if(byteBuf !=null){
            if (memoryUsage.containsKey(threadId)){
                memoryUsage.put(threadId,memoryUsage.get(threadId)+byteBuf.capacity());
            }else {
                memoryUsage.put(threadId,(long)byteBuf.capacity());
            }
        }
        return byteBuf;
    }

    @Override
    public void recycle(final ByteBuffer theBuf) {
        if(!(theBuf instanceof DirectBuffer)){
            theBuf.clear();
            return;
        }

        final long size = theBuf.capacity();

        boolean recycled = false;
        final DirectBuffer thisNavBuf = (DirectBuffer) theBuf;
        final int chunkCount = theBuf.capacity() / chunkSize;
        final DirectBuffer parentBuf = (DirectBuffer) thisNavBuf.attachment();
        final int startChunk = (int) ((thisNavBuf.address() - parentBuf.address()) / chunkSize);
        for (int i = 0; i < allPages.length; i++) {
            if (recycled = allPages[i].recycleBuffer((ByteBuffer) parentBuf, startChunk, chunkCount) == true) {
                break;
            }
        }
        final long threadId = Thread.currentThread().getId();

        if (memoryUsage.containsKey(threadId)){
            memoryUsage.put(threadId,memoryUsage.get(threadId)-size);
        }
        if (recycled == false) {
            LOGGER.warn("warning ,not recycled buffer " + theBuf);
        }
    }

    private ByteBuffer allocateBuffer(final int theChunkCount, final int startPage, final int endPage) {
        for (int i = startPage; i < endPage; i++) {
            final ByteBuffer buffer = allPages[i].allocatChunk(theChunkCount);
            if (buffer != null) {
                prevAllocatedPage.getAndSet(i);
                return buffer;
            }
        }
        return null;
    }

    @Override
    public int getChunkSize() {
        return chunkSize;
    }

    @Override
    public ConcurrentHashMap<Long,Long> getNetDirectMemoryUsage() {
        return memoryUsage;
    }

    public int getPageSize() {
        return pageSize;
    }

    public short getPageCount() {
        return pageCount;
    }

    //TODO   should  fix it
    @Override
    public long capacity(){
        return size();
    }

    @Override
    public long size(){
        return  (long) pageSize * chunkSize * pageCount;
    }

    //TODO
    @Override
    public  int getSharedOptsCount(){
        return 0;
    }


    @Override
    public int getConReadBuferChunk() {
        return conReadBuferChunk;
    }

}
