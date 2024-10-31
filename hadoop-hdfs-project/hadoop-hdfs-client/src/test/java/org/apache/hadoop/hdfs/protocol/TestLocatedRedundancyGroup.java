package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestLocatedRedundancyGroup {

    private DatanodeInfo generateInfo(String name) {
        DatanodeInfo.DatanodeInfoBuilder builder = new DatanodeInfo.DatanodeInfoBuilder();
        builder.setIpAddr("ip0");
        builder.setHostName(name);
        builder.setDatanodeUuid("uuid0");
        builder.setXferPort(90);
        builder.setInfoPort(10);
        builder.setInfoSecurePort(30);
        return builder.build();
    }

    @Test
    public void testConstruction() {
        LocatedRedundancyGroup lrg = new LocatedRedundancyGroup(5);
        List<LocatedBlock> blks = new ArrayList<>();

        // transition from 2-of-3 to 3-of-4
        ErasureCodingPolicy ecPolicy = new ErasureCodingPolicy(
                new ECSchema("XOR", 3, 2), 1024);
        ecPolicy.setStripeWidth(2);

        // construct 3 logical blocks, each with 2 internal blocks
        LocatedBlock lb0 = new LocatedStripedBlock(
                new ExtendedBlock("pool", 0),
                new DatanodeInfo[]{generateInfo("host0"), generateInfo("host1"), generateInfo("host9")},
                new String[]{"si0", "si1", "si2"}, new StorageType[]{StorageType.DISK, StorageType.DISK, StorageType.DISK},
                new byte[]{0,1,2}, 0, false, null);
        blks.add(lb0);
        LocatedBlock lb1 = new LocatedStripedBlock(
                new ExtendedBlock("pool", 5),
                new DatanodeInfo[]{generateInfo("host2"), generateInfo("host3"), generateInfo("host4")},
                new String[]{"si0", "si1", "si2"}, new StorageType[]{StorageType.DISK, StorageType.DISK, StorageType.DISK},
                new byte[]{0,1,2}, 0, false, null);
        blks.add(lb1);
        LocatedBlock lb2 = new LocatedStripedBlock(
                new ExtendedBlock("pool", 10),
                new DatanodeInfo[]{generateInfo("host4"), generateInfo("host5")},
                new String[]{"si0", "si1"}, new StorageType[]{StorageType.DISK, StorageType.DISK},
                new byte[]{0,2}, 0, false, null);
        blks.add(lb2);

        // convert the logical blocks into a redundancy group
        LocatedBlocks lbs = new LocatedBlocks(1000, false,
                blks, null, null, true, null, ecPolicy);
        List<LocatedRedundancyGroup> lrgs = lrg.convert(lbs);
    }
}
