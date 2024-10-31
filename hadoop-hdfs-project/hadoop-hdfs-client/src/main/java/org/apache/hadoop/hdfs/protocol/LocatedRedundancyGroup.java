package org.apache.hadoop.hdfs.protocol;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;

import java.util.*;

/**
 * Tracks separate metadata of parity block striping as opposed to logical block
 * data striping which occurs after an ec-transition.
 *
 * For example: a striped block group with DN0, DN1 maybe get grouped together with another
 * striped block group with DN2, DN3 and DN4, DN5 such that there are two new LocatedRedundancyGroups:
 * DN0 + DN1 + DN2 | DN3 + DN4 + DN5. We need to create a new structure to be able to compute
 * these new datanode locations lazily.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedRedundancyGroup {

    // max capacity of the group: k2 (should be k2 + p2 later)
    private final int capacity;

    // field to track blocks in group
    private LocatedBlock[] blocks;

    // track the current position of the group, useful when constructing
    private int pointer;

    private int size() {
        return pointer;
    }

    public LocatedRedundancyGroup(int capacity) {
        this.pointer = 0;
        this.capacity = capacity;
        blocks = new LocatedBlock[capacity];
    }

    private void append(LocatedBlock block) {
        blocks[pointer] = block;
        pointer++;
    }

    /**
     * Wrapper around convertToRedundancyGroup for static unit testing.
     */
    protected List<LocatedRedundancyGroup> convert(LocatedBlocks lbs) {
        return convertToRedundancyGroup(lbs);
    }

    /**
     * Given a file's complete list of logical blocks, convert it into
     * a full list of located redundancy groups for the entire file in
     * sequential order of logical blocks.
     * @param lbs striped block groups of a file
     */
    public static List<LocatedRedundancyGroup> convertToRedundancyGroup(LocatedBlocks lbs) {
        List<LocatedRedundancyGroup> stripedRedundancyGroups = new ArrayList<>();
        List<LocatedBlock> stripedBlockGroups = lbs.getLocatedBlocks();

        // compute dimensions of new striped redundancy groups based on original and current schema
        ErasureCodingPolicy ecPolicy = lbs.getErasureCodingPolicy();
        int k1 = ecPolicy.getStripeWidth();
        int k2 = ecPolicy.getSchema().getNumDataUnits();
        int cellSize = ecPolicy.getCellSize();

        // keep track of the blocks available to us
        int lb = 0;
        LocatedStripedBlock sbg = (LocatedStripedBlock) stripedBlockGroups.get(lb);
        Queue<LocatedBlock> blockQueue = new LinkedList<>(
                Arrays.asList(StripedBlockUtil.parseStripedBlockGroup(sbg, cellSize, k1, 0)));

        // keep track of the current located redundancy group we are building
        LocatedRedundancyGroup lrg = new LocatedRedundancyGroup(k2);
        while (blockQueue.size() > 0) {
            while (lrg.size() < lrg.capacity && blockQueue.size() > 0) {
                LocatedBlock b = blockQueue.poll();
                if (b != null) {
                    lrg.append(b);
                }
            }

            // 1. run out of blocks in block queue
            if (blockQueue.size() == 0) {
                lb++;
                if (lb < stripedBlockGroups.size()) {
                    // still have more blocks to pull from
                    sbg = (LocatedStripedBlock) stripedBlockGroups.get(lb);
                    blockQueue.addAll(Arrays.asList(
                            StripedBlockUtil.parseStripedBlockGroup(sbg, cellSize, k1, 0)));
                } else {
                    // ran out of logical blocks, wrap up this lrg if not already added
                    stripedRedundancyGroups.add(lrg);
                    break;
                }
            }

            // 2. run out of space in lrg
            if (lrg.size() == lrg.capacity) {
                stripedRedundancyGroups.add(lrg);
                lrg = new LocatedRedundancyGroup(k2);
            }
        }

        for (LocatedRedundancyGroup group : stripedRedundancyGroups) {
            System.out.println(group);
        }

        return stripedRedundancyGroups;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LocatedRedundancyGroup: \n" + "{capacity()=")
                .append(capacity)
                .append("; current size ")
                .append(pointer)
                .append("\n");
        for (int i = 0; i < pointer; i++) {
            sb.append(blocks[i].toString()).append("\n");
        }
        return sb.toString();
    }
}
