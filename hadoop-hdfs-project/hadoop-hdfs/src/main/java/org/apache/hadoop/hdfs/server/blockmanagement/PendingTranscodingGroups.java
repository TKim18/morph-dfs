package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.*;

/**
 * PendingTranscodingBlocks maintains any groups
 * undergoing transcoding operations and concretely
 * 1. counts the number of newly completed parity blocks per group
 * 2. counts the number of groups in the file that are completed
 */
class PendingTranscodingGroups {

  private final Map<Long, PendingFileTranscodeInfo> pendingGroupsInFile;  // key is block collection id

  private final Map<BlockInfo, PendingGroupTranscodeInfo> pendingBlocksInGroup;


  PendingTranscodingGroups() {
    pendingGroupsInFile = new HashMap<>();
    pendingBlocksInGroup = new HashMap<>();
  }

  /**
   * Track the group as part of its larger file and also the
   * parity targets as part of its larger group.
   * Use this tracking to maintain transcoding state.
   *
   * @param group undergoing transcoding
   * @param targets location of new parity blocks to be reported
   */
  void track(BlockInfo group, DatanodeStorageInfo... targets) {
    final long bcId = group.getBlockCollectionId();

    synchronized (pendingGroupsInFile) {
      PendingFileTranscodeInfo found = pendingGroupsInFile.get(bcId);
      if (found == null) {
        // track the group in its file
        pendingGroupsInFile.put(bcId, new PendingFileTranscodeInfo(group));
      } else {
        found.incrementFile(group);
      }
    }

    synchronized (pendingBlocksInGroup) {
      PendingGroupTranscodeInfo found = pendingBlocksInGroup.get(group);
      if (found == null) {
        // track the parity block targets in its group
        pendingBlocksInGroup.put(group, new PendingGroupTranscodeInfo(targets));
      } else {
        found.incrementGroup(targets);
      }
    }
  }

  boolean register(BlockInfo group, DatanodeStorageInfo target) {
    final long bcId = group.getBlockCollectionId();  // group is actually a parity block
    boolean removed = false;  // indicator of all parities completed for a group
    boolean complete = false; // indicator of all groups completed for a file

    synchronized (pendingBlocksInGroup) {
      PendingGroupTranscodeInfo found = pendingBlocksInGroup.get(group);
      if (found != null) {
        found.decrementGroup(target);
        if (found.getNumTargets() <= 0) {
          // this is the last parity, remove group and decrement file
          pendingBlocksInGroup.remove(group);
          removed = true;
        }
      }
    }

    if (!removed) {
      return false;
    }

    synchronized(pendingGroupsInFile) {
      PendingFileTranscodeInfo found = pendingGroupsInFile.get(bcId);
      if (found != null) {
        found.decrementFile(group);
        if (found.getNumGroups() <= 0) {
          complete = true;
        }
      }
    }

    return complete;
  }

  /**
   * Completes the transcode op for this file.
   * Pops out the newly completed parity blocks for that bcId.
   *
   * @param bcId completed file's bcId id
   * @return new parities
   */
  BlockInfo[] complete(Long bcId) {
    BlockInfo[] parities = null;
    synchronized(pendingGroupsInFile) {
      PendingFileTranscodeInfo found = pendingGroupsInFile.get(bcId);
      if (found != null) {
        parities = found.getNewParities();
        // remove this element from tracking
        pendingGroupsInFile.remove(bcId);
      }
    }
    return parities;
  }

  boolean isTracked(BlockInfo group) {
    // return if a group is being tracked for transcoding
    synchronized (pendingBlocksInGroup) {
      return pendingBlocksInGroup.get(group) != null;
    }
  }

  /**
   * Tracks the number of incomplete target blocks to be returned
   * from this group's transcoding op.
   */
  static class PendingGroupTranscodeInfo {
    private final List<DatanodeStorageInfo> incompleteBlocks;

    PendingGroupTranscodeInfo(DatanodeStorageInfo[] targets) {
      this.incompleteBlocks = targets == null ? new ArrayList<>()
          : new ArrayList<>(Arrays.asList(targets));
    }

    void incrementGroup(DatanodeStorageInfo... newTargets) {
      if (newTargets != null) {
        for (DatanodeStorageInfo newTarget : newTargets) {
          if (!incompleteBlocks.contains(newTarget)) {
            incompleteBlocks.add(newTarget);
          }
        }
      }
    }

    void decrementGroup(DatanodeStorageInfo target) {
        incompleteBlocks.removeIf(next -> next.getDatanodeDescriptor() == target.getDatanodeDescriptor());
    }

    int getNumTargets() {
      return incompleteBlocks.size();
    }
  }

  /**
   * Tracks the number of incomplete target groups to be returned
   * from this file's transcoding op.
   */
  static class PendingFileTranscodeInfo {
    private final List<BlockInfo> incompleteGroups;
    /** track new parity blocks for completed files to be used after transcode completion */
    private final List<BlockInfo> completeParities;

    PendingFileTranscodeInfo(BlockInfo firstGroup) {
      this.incompleteGroups = new LinkedList<>();
      this.completeParities = new LinkedList<>();
      this.incompleteGroups.add(firstGroup);
    }

    void incrementFile(BlockInfo newGroup) {
      if (!incompleteGroups.contains(newGroup)) {
        incompleteGroups.add(newGroup);
      }
    }

    void decrementFile(BlockInfo completedParity) {
      incompleteGroups.remove(completedParity);
      completeParities.add(completedParity);
    }

    int getNumGroups() {
      return incompleteGroups.size();
    }

    BlockInfo[] getNewParities() {
      BlockInfo[] parities = new BlockInfo[completeParities.size()];
      for (int i = 0; i < completeParities.size(); i++) {
        parities[i] = completeParities.get(i);
      }
      return parities;
    }
  }

}
