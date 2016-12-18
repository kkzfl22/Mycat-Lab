package io.mycat.memory;

import java.nio.ByteBuffer;

import sun.nio.ch.DirectBuffer;

/**
 * DirectByteBuffer池，可以分配任意指定大小的DirectByteBuffer，用完需要归还
 *
 * @author wuzhih
 */
@SuppressWarnings("restriction")
public class DirectByteBufferPool {

    /**
     * 固定的内存页的数组 
    * @字段说明 allPages
    */
    private ByteBufferPage[] allPages;

    /**
     * 每个内存页中数据项的大小
    * @字段说明 chunkSize
    */
    private final int chunkSize;

    private int prevAllocatedPage = 0;

    /**
    * 构造方法
    * @param pageSize 每个内存页的大小
    * @param chunkSize 每个内存页中chunk的大小
    * @param pageCount 总的内存页数
    */
    public DirectByteBufferPool(int pageSize, short chunkSize, short pageCount) {
        // 保存所有的内存页的数组
        allPages = new ByteBufferPage[pageCount];
        // 每个内存页中chunk的大小
        this.chunkSize = chunkSize;
        // 进行初始化每个内存页数据的分配
        for (int i = 0; i < pageCount; i++) {
            allPages[i] = new ByteBufferPage(ByteBuffer.allocateDirect(pageSize), chunkSize);
        }
    }

    /**
     * 构造一个内存的byteBuffer对象
    * 方法描述
    * @param size
    * @return
    * @创建日期 2016年12月18日
    */
    public ByteBuffer allocate(int size) {

        // 计算需要的chunk的数量
        int theChunkCount = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
        // 得到结束的内存页索引号
        int selectedPage = (++prevAllocatedPage) % allPages.length;
        // 进行内存分配
        ByteBuffer byteBuf = allocateBuffer(theChunkCount, 0, selectedPage);
        // 如果内存分配失败，则再进一次内存分配
        if (byteBuf == null) {
            byteBuf = allocateBuffer(theChunkCount, selectedPage, allPages.length);
        }
        return byteBuf;
    }

    /**
     * 进行内存回收
    * 方法描述
    * @param theBuf
    * @创建日期 2016年12月18日
    */
    public void recycle(ByteBuffer theBuf) {
        // 初始化内存回收为false
        boolean recycled = false;
        // 获得内存buffer
        sun.nio.ch.DirectBuffer thisNavBuf = (DirectBuffer) theBuf;
        // 计算得到总的chunk数
        int chunkCount = theBuf.capacity() / chunkSize;
        // attachment对象在buf.slice();的时候将attachment对象设置为总的buff对象
        sun.nio.ch.DirectBuffer parentBuf = (DirectBuffer) thisNavBuf.attachment();
        // 已经使用的地址减去父类最开始的地址，即为所有已经使用的地址，除以chunkSize得到chunk当前开始的地址,得到整块内存开始的地址
        int startChunk = (int) ((thisNavBuf.address() - parentBuf.address()) / this.chunkSize);

        // 进行遍历内存页
        for (int i = 0; i < allPages.length; i++) {
            // 找到内存页进行内存的归还
            if ((recycled = allPages[i].recycleBuffer((ByteBuffer) parentBuf, startChunk, chunkCount) == true)) {
                break;
            }
        }
        if (recycled == false) {
            System.out.println("warning ,not recycled buffer " + theBuf);
        }
    }

    /**
     * 从指定的内存页中获取需要的chunk
    * 方法描述
    * @param theChunkCount 需要的chunk数量
    * @param startPage 开始的内存页号
    * @param endPage 结束的内存页号
    * @return
    * @创建日期 2016年12月18日
    */
    private ByteBuffer allocateBuffer(int theChunkCount, int startPage, int endPage) {
        for (int i = startPage; i < endPage; i++) {
            // 进行指定内存页中的内存空间分配
            ByteBuffer buffer = allPages[i].allocatChunk(theChunkCount);
            if (buffer != null) {
                // 分配成功，将当前的分配标识记录为当前操作的开始页编号
                prevAllocatedPage = i;
                return buffer;
            }
        }
        return null;
    }
}
