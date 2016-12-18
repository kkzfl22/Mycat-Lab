package io.mycat.memory;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;

/*
 * 用来保存一个一个ByteBuffer为底层存储的内存页
 */
@SuppressWarnings("restriction")
public class ByteBufferPage {
    /**
     * 缓冲区信息
    * @字段说明 buf
    */
    private final ByteBuffer buf;

    /**
     * 每个chunk的大小
    * @字段说明 chunkSize
    */
    private final int chunkSize;

    /**
     * chunk的数量
    * @字段说明 chunkCount
    */
    private final int chunkCount;

    /**
     * 用来进行内存位标识是否使用的集合
    * @字段说明 chunkAllocateTrack
    */
    private final BitSet chunkAllocateTrack;

    /**
     * 当前的锁定的状态
    * @字段说明 allocLockStatus
    */
    private final AtomicBoolean allocLockStatus = new AtomicBoolean(false);

    /**
     * 直接内存块的首地址信息
    * @字段说明 startAddress
    */
    private final long startAddress;

    public ByteBufferPage(ByteBuffer buf, int chunkSize) {
        super();
        // 对本页中的chunk的大小进行设置
        this.chunkSize = chunkSize;
        // 计算得到多少个chunk，即总容量除以每个chunk的大小
        chunkCount = buf.capacity() / chunkSize;
        // 大小可动态改变, 取值为true或false的位集合。用于表示一组布尔标志。
        // chunkCount指定此bitSet集合中的大小
        chunkAllocateTrack = new BitSet(chunkCount);
        // 当前的内存缓冲区
        this.buf = buf;
        // 直接内存的开始地址信息
        startAddress = ((sun.nio.ch.DirectBuffer) buf).address();
    }

    /**
     * 分配指定数量的chunk
    * 方法描述
    * @param theChunkCount 需要的chunk数量
    * @return
    * @创建日期 2016年12月18日
    */
    public ByteBuffer allocatChunk(int theChunkCount) {

        // 将当前进lock进行锁定
        if (!allocLockStatus.compareAndSet(false, true)) {
            return null;
        }
        // 开始的 chunk编号
        int startChunk = -1;
        // 继续的chunk的编号
        int contiueCount = 0;
        try {
            // 在一个内存页的chunk数量中进宪遍历
            for (int i = 0; i < chunkCount; i++) {

                // 获取当前内存坏是否已经使用,如果当前未使用
                if (chunkAllocateTrack.get(i) == false) {
                    // 如果当前的chunk地址未使用
                    if (startChunk == -1) {
                        // 设置开始chunk编号为0
                        startChunk = i;
                        // 继续的编号为1
                        contiueCount = 1;

                        // 如果是仅需要分配一个则结束遍历
                        if (theChunkCount == 1) {
                            break;
                        }
                    }
                    // 如果首个地址已经使用,检查分配的chunk能否满足需要的chunk数量
                    else {
                        if (++contiueCount == theChunkCount) {
                            break;
                        }
                    }
                }
                // 如果已经使用，则将标识记录为下一次的开始
                else {
                    startChunk = -1;
                    contiueCount = 0;
                }
            }

            // 如果找到的chunk数量能满足分配 要求
            if (contiueCount == theChunkCount) {
                // 设置开始的chunk编号
                int offStart = startChunk * chunkSize;
                // 设置结束的chunk编号
                int offEnd = offStart + theChunkCount * chunkSize;
                // 将buf中的标识设置为开始的索引号
                buf.position(offStart);
                // 设置结束的索引号
                buf.limit(offEnd);
                ByteBuffer newBuf = buf.slice();
                // sun.nio.ch.DirectBuffer theBuf = (DirectBuffer) newBuf;
                // System.out.println("offAddress " + (theBuf.address() -
                // startAddress));
                // 将内存标识位设置为已经使用
                markChunksUsed(startChunk, theChunkCount);
                return newBuf;
            } else {
                // System.out.println("contiueCount " + contiueCount + "
                // theChunkCount " + theChunkCount);
                return null;
            }
        } finally {
            // 将当前的状态解锁
            allocLockStatus.set(false);
        }
    }

    /**
     * 将bitset中一段标识为已经使用
    * 方法描述
    * @param startChunk
    * @param theChunkCount
    * @创建日期 2016年12月18日
    */
    private void markChunksUsed(int startChunk, int theChunkCount) {
        for (int i = 0; i < theChunkCount; i++) {
            chunkAllocateTrack.set(startChunk + i);
        }
    }

    /**
     * 找到标识信息，行清理操作 
    * 方法描述
    * @param startChunk 开始的内存块
    * @param theChunkCount  清理的内存块数
    * @创建日期 2016年12月18日
    */
    private void markChunksUnused(int startChunk, int theChunkCount) {
        for (int i = 0; i < theChunkCount; i++) {
            chunkAllocateTrack.clear(startChunk + i);
        }
    }

    public boolean recycleBuffer(ByteBuffer parent, int startChunk, int chunkCount) {

        // 检查当前的buffer是否与传递过来的parent对象相同
        if (parent == this.buf) {

            // 对当前的的操作进行加锁操作,如果加锁失败，则执行其他线程
            while (!this.allocLockStatus.compareAndSet(false, true)) {
                Thread.yield();
            }
            try {
                // 进行标识的清理
                markChunksUnused(startChunk, chunkCount);
            } finally {
                // 释放标识
                allocLockStatus.set(false);
            }
            return true;
        }
        return false;
    }
}
