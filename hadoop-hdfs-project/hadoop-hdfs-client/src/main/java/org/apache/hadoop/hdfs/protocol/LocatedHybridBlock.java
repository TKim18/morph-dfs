package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;

/**
 * {@link LocatedStripedBlock} with hybrid block support. For a hybrid block,
 * all the things are the same as with striped block but includes additional
 * block informations for the replica sets that are protected by the stripe.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedHybridBlock extends LocatedStripedBlock {

    private final ExtendedBlock[] replicas;
    private final ExtendedBlock parity;

    public LocatedHybridBlock(ExtendedBlock b, ExtendedBlock p, ExtendedBlock[] replicas, DatanodeInfo[] locs,
           String[] storageIDs, StorageType[] storageTypes, byte[] indices,
           long startOffset, boolean corrupt, DatanodeInfo[] cachedLocs) {
        super(b, locs, storageIDs, storageTypes, indices, startOffset, corrupt, cachedLocs);
        this.parity = p;
        this.replicas = replicas;
    }

    public ExtendedBlock[] getReplicas() {
        return replicas;
    }

    public ExtendedBlock getParity() {
        return parity;
    }

    @Override
    public BlockType getBlockType() {
        return BlockType.HYBRID;
    }

    @Override
    public boolean isHybrid() {
        return true;
    }

}
