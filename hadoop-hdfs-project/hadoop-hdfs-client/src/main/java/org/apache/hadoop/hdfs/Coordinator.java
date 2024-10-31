package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Coordinator {

    static class MultipleBlockingQueue<T> {
        private final List<BlockingQueue<T>> queues;

        MultipleBlockingQueue(int numQueue, int queueSize) {
            queues = new ArrayList<>(numQueue);
            for (int i = 0; i < numQueue; i++) {
                queues.add(new LinkedBlockingQueue<T>(queueSize));
            }
        }

        void offer(int i, T object) {
            final boolean b = queues.get(i).offer(object);
            Preconditions.checkState(b, "Failed to offer " + object
                    + " to queue, i=" + i);
        }

        T take(int i) throws InterruptedIOException {
            try {
                return queues.get(i).take();
            } catch(InterruptedException ie) {
                throw DFSUtilClient.toInterruptedIOException("take interrupted, i=" + i, ie);
            }
        }

        T takeWithTimeout(int i) throws InterruptedIOException {
            try {
                return queues.get(i).poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw DFSUtilClient.toInterruptedIOException("take interrupted, i=" + i, e);
            }
        }

        T poll(int i) {
            return queues.get(i).poll();
        }

        T peek(int i) {
            return queues.get(i).peek();
        }

        void clear() {
            for (BlockingQueue<T> q : queues) {
                q.clear();
            }
        }
    }
    /**
     * The next internal block to write to for each streamers. The
     * DFSStripedOutputStream makes the {@link ClientProtocol#addBlock} RPC to
     * get a new block group. The block group is split to internal blocks, which
     * are then distributed into the queue for streamers to retrieve.
     */
    private final MultipleBlockingQueue<LocatedBlock> followingBlocks;
    /**
     * Used to sync among all the streamers before allocating a new block. The
     * DFSStripedOutputStream uses this to make sure every streamer has finished
     * writing the previous block.
     */
    public final MultipleBlockingQueue<ExtendedBlock> endBlocks;

    /**
     * The following data structures are used for syncing while handling errors
     */
    private final MultipleBlockingQueue<LocatedBlock> newBlocks;
    public final Map<DataStreamer, Boolean> updateStreamerMap;
    private final MultipleBlockingQueue<Boolean> streamerUpdateResult;

    Coordinator(final int numAllBlocks) {
        followingBlocks = new MultipleBlockingQueue<>(numAllBlocks, 1);
        endBlocks = new MultipleBlockingQueue<>(numAllBlocks, 1);
        newBlocks = new MultipleBlockingQueue<>(numAllBlocks, 1);
        updateStreamerMap = new ConcurrentHashMap<>(numAllBlocks);
        streamerUpdateResult = new MultipleBlockingQueue<>(numAllBlocks, 1);
    }

    MultipleBlockingQueue<LocatedBlock> getFollowingBlocks() {
        return followingBlocks;
    }

    MultipleBlockingQueue<LocatedBlock> getNewBlocks() {
        return newBlocks;
    }

    public void offerEndBlock(int i, ExtendedBlock block) {
        endBlocks.offer(i, block);
    }

    public void offerStreamerUpdateResult(int i, boolean success) {
        streamerUpdateResult.offer(i, success);
    }

    public boolean takeStreamerUpdateResult(int i) throws InterruptedIOException {
        return streamerUpdateResult.take(i);
    }

    public void updateStreamer(DataStreamer streamer,
                        boolean success) {
        assert !updateStreamerMap.containsKey(streamer);
        updateStreamerMap.put(streamer, success);
    }

    public void clearFailureStates() {
        newBlocks.clear();
        updateStreamerMap.clear();
        streamerUpdateResult.clear();
    }
}
