package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks groups that are requested to be transcoded into
 * a new erasure coding scheme. Groups are given priority
 * based on whether the amount of redundancy is increased
 * or decreased and the distance/benefits of transition.
 */
public class TranscodingBlocks implements Iterable<TranscodeInfo> {

  /** The total number of queues */
  static final int LEVEL = 3;

  static final int QUEUE_INCREASING_REDUNDANCY = 0;

  static final int QUEUE_DECREASING_REDUNDANCY = 1;

  static final int QUEUE_CHANGING_CODE = 2;

  private final List<LightWeightLinkedSet<TranscodeInfo>> priorityQueues
      = new ArrayList<>(LEVEL);

  private final LongAdder transcodingBlocks = new LongAdder();
  private final LongAdder incrRedundancyBlocks = new LongAdder();
  private final LongAdder decrRedundancyBlocks = new LongAdder();

  TranscodingBlocks() {
    for (int i = 0; i < LEVEL; i++) {
      // TODO: swap out lwlinkedset with lwheap
      priorityQueues.add(new LightWeightLinkedSet<>());
    }
  }

  synchronized void clear() {
    for (int i = 0; i < LEVEL; i++) {
      priorityQueues.get(i).clear();
    }
    transcodingBlocks.reset();
    incrRedundancyBlocks.reset();
    decrRedundancyBlocks.reset();
  }

  synchronized int size() {
    int size = 0;
    for (int i = 0; i < LEVEL; i++) {
      size += priorityQueues.get(i).size();
    }
    return size;
  }

  synchronized int getTranscodingBlockCount() {
    return size();
  }

  long getIncreasingRedundancyBlockCount() {
    return incrRedundancyBlocks.longValue();
  }

  long getDecreasingRedundancyBlockCount() {
    return decrRedundancyBlocks.longValue();
  }

  synchronized boolean contains(TranscodeInfo block) {
    for (LightWeightLinkedSet<TranscodeInfo> set : priorityQueues) {
      if (set.contains(block)) {
        return true;
      }
    }
    return false;
  }

  static int getPriority(TranscodeInfo block) {
    int oldDataUnits = block.getSchema().getNumDataUnits();
    int oldParityUnits = block.getSchema().getNumParityUnits();
    int newDataUnits = block.getGroup().getDataBlockNum();
    int newParityUnits = block.getGroup().getParityBlockNum();

    if (oldParityUnits < newParityUnits) {
      // assume increasing redundancy
      return QUEUE_INCREASING_REDUNDANCY;
    } else if (oldParityUnits > newParityUnits) {
      // assume decreasing redundancy for storage
      return QUEUE_DECREASING_REDUNDANCY;
    } else {
      if (oldDataUnits < newDataUnits) {
        // going wider
        return QUEUE_DECREASING_REDUNDANCY;
      } else if (oldDataUnits > newDataUnits) {
        // going narrower
        return QUEUE_INCREASING_REDUNDANCY;
      } else {
        // keeping redundancy, may be used to change codes
        return QUEUE_CHANGING_CODE;
      }
    }
  }

  synchronized boolean add(TranscodeInfo block) {
    final int priority = getPriority(block);
    return add(block, priority);
  }

  private boolean add(TranscodeInfo block, int priority) {
    return priorityQueues.get(priority).add(block);
  }

  synchronized boolean remove(TranscodeInfo block) {
    final int priority = getPriority(block);
    return remove(block, priority);
  }

  private boolean remove(TranscodeInfo block, int priority) {
    return priorityQueues.get(priority).remove(block);
  }

  synchronized void update(TranscodeInfo block, int oldPriority, int newPriority) {
    boolean found = remove(block, oldPriority);
    if (found) {
      // unable to find a block to remove
    }
    add(block, newPriority);
  }

  synchronized List<List<TranscodeInfo>> chooseBlocks2Transcode(
      int blocksToProcess) {
    final List<List<TranscodeInfo>> blocksToTranscode = new ArrayList<>(LEVEL);

    for (int count = 0, priority = 0; count < blocksToProcess && priority < LEVEL; priority++) {
      final LightWeightHashSet<TranscodeInfo> infos = priorityQueues.get(priority);
      final List<TranscodeInfo> blocks = infos.pollN(Math.min(blocksToProcess, infos.size()));
      blocksToTranscode.add(blocks);
      count += blocks.size();
    }

    return blocksToTranscode;
  }

  /** Returns an iterator of all blocks in a given priority queue. */
  synchronized Iterator<TranscodeInfo> iterator(int level) {
    return priorityQueues.get(level).iterator();
  }

  /** Return an iterator of all the blocks to be transcoded */
  @Override
  public synchronized Iterator<TranscodeInfo> iterator() {
    final Iterator<LightWeightLinkedSet<TranscodeInfo>> q = priorityQueues.iterator();
    return new Iterator<TranscodeInfo>() {
      private Iterator<TranscodeInfo> b = q.next().iterator();

      @Override
      public TranscodeInfo next() {
        hasNext();
        return b.next();
      }

      @Override
      public boolean hasNext() {
        for(; !b.hasNext() && q.hasNext(); ) {
          b = q.next().iterator();
        }
        return b.hasNext();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

}
