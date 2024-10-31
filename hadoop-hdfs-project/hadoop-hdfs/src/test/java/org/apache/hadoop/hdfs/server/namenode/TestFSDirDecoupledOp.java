package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFSDirDecoupledOp {

  DatanodeStorageInfo[] datanodes;

  ErasureCodingPolicy policy_2_1;
  ErasureCodingPolicy policy_3_1;
  ErasureCodingPolicy policy_4_1;
  ErasureCodingPolicy policy_6_1;
  ErasureCodingPolicy policy_7_1;
  ErasureCodingPolicy policy_20_1;

  ErasureCodingPolicy cc_policy_2_1;
  ErasureCodingPolicy cc_policy_4_1;
  int cellSize;
  int bigCellSize;

  FSNamesystem fsnMock;

  @Before
  public void setUp() throws IOException {
    datanodes = new DatanodeStorageInfo[7];
    datanodes[0] = DFSTestUtil.createDatanodeStorageInfo(
      "storageID1", "127.0.0.1");
    datanodes[1] = DFSTestUtil.createDatanodeStorageInfo(
      "storageID2", "127.0.0.2");
    datanodes[2] = DFSTestUtil.createDatanodeStorageInfo(
      "storageID3", "127.0.0.3");
    datanodes[3] = DFSTestUtil.createDatanodeStorageInfo(
      "storageID4", "127.0.0.4");
    datanodes[4] = DFSTestUtil.createDatanodeStorageInfo(
      "storageID5", "127.0.0.5");
    datanodes[5] = DFSTestUtil.createDatanodeStorageInfo(
      "storageID6", "127.0.0.6");
    datanodes[6] = DFSTestUtil.createDatanodeStorageInfo(
      "storageID7", "127.0.0.7");

    cellSize = 2048; // two kilobytes

    policy_2_1 = new ErasureCodingPolicy(
      new ECSchema("cc", 2, 1), cellSize);
    policy_3_1 = new ErasureCodingPolicy(
      new ECSchema("cc", 3, 1), cellSize);
    policy_4_1 = new ErasureCodingPolicy(
      new ECSchema("cc", 4, 1), cellSize);


    cc_policy_2_1 = new ErasureCodingPolicy(
            new ECSchema("cc",2,1), cellSize);
    cc_policy_4_1 = new ErasureCodingPolicy(
            new ECSchema("cc",4,1), cellSize);

    bigCellSize = 1048576; // one megabyte

    policy_6_1 = new ErasureCodingPolicy(
      new ECSchema("XOR", 6, 1), bigCellSize);
    policy_7_1 = new ErasureCodingPolicy(
      new ECSchema("XOR", 7, 1), bigCellSize);
    policy_20_1 = new ErasureCodingPolicy(
      new ECSchema("XOR", 20, 1), bigCellSize);

    fsnMock = mock(FSNamesystem.class);
    when(fsnMock.createNewBlock(BlockType.GROUPED))
      .thenReturn(new Block(toStripedId(32), 0, 0))
      .thenReturn(new Block(toStripedId(64), 0, 0))
      .thenReturn(new Block(toStripedId(96), 0, 0))
      .thenReturn(new Block(toStripedId(128), 0, 0));
  }
@Test
  public void testTranscodeInfoForCCGroups() throws IOException {
    BlockInfoData[] dataStripes = new BlockInfoData[4];
    BlockInfoParity[] oldParities = new BlockInfoParity[4];

    long id0 = toStripedId(32);
    BlockInfoData b1 = new BlockInfoData(new Block(id0), (short) 2, cellSize);
    b1.addStorage(datanodes[0], new Block(id0));
    b1.addStorage(datanodes[1], new Block(id0 + 1));
    b1.setNumBytes(cellSize * 2L);
    dataStripes[0] = b1;

    long id1 = id0 + 32;
    BlockInfoData b2 = new BlockInfoData(new Block(id1), (short) 2, cellSize);
    b2.addStorage(datanodes[3], new Block(id1));
    b2.addStorage(datanodes[4], new Block(id1 + 1));
    b2.setNumBytes(cellSize * 2L);
    dataStripes[1] = b2;

    long id2 = id1 + 32;
    BlockInfoData b3 = new BlockInfoData(new Block(id2), (short) 2, cellSize);
    b3.addStorage(datanodes[1], new Block(id2));
    b3.addStorage(datanodes[3], new Block(id2 + 1));
    b3.setNumBytes(cellSize * 2L);
    dataStripes[2] = b3;

    long id3 = id2 + 32;
    BlockInfoData b4 = new BlockInfoData(new Block(id3), (short) 2, cellSize);
    b4.addStorage(datanodes[2], new Block(id3));
    b4.addStorage(datanodes[4], new Block(id3 + 1));
    b4.setNumBytes(cellSize * 2L);
    dataStripes[3] = b4;

    long id01 = id3 + 32;
    BlockInfoParity b01 = new BlockInfoParity(new Block(id01), (short) 1, cellSize);
    b01.addStorage(datanodes[5], new Block(id01));
    b01.setNumBytes(cellSize * 2L);
    oldParities[0] = b01;

    long id02 = id01 + 32;
    BlockInfoParity b02 = new BlockInfoParity(new Block(id02), (short) 1, cellSize);
    b02.addStorage(datanodes[6], new Block(id02));
    b02.setNumBytes(cellSize * 2L);
    oldParities[1] = b02;

    long id03 = id02 + 32;
    BlockInfoParity b03 = new BlockInfoParity(new Block(id03), (short) 1, cellSize);
    b03.addStorage(datanodes[0], new Block(id03));
    b03.setNumBytes(cellSize * 2L);
    oldParities[2] = b03;

    long id04 = id03 + 32;
    BlockInfoParity b04 = new BlockInfoParity(new Block(id04), (short) 1, cellSize);
    b04.addStorage(datanodes[1], new Block(id04));
    b04.setNumBytes(cellSize * 2L);
    oldParities[3] = b04;





    TranscodeInfo[] transcodeInfos = FSDirDecouplingOp.createNewGeneralizedGroups(fsnMock, dataStripes, oldParities,
            cc_policy_2_1, cc_policy_4_1);

    for(TranscodeInfo tinfo: transcodeInfos){
      System.out.println(tinfo);
      Assert.assertEquals(2, transcodeInfos.length);
      int[] offsetArray = tinfo.getGroupIDOffsets();
      Assert.assertEquals(5, offsetArray.length);
      for(int offset : offsetArray){
        System.out.println(offset);
      }

      Assert.assertEquals(2,tinfo.getParities().length);
      for(BlockInfoParity parity : tinfo.getParities()){
        System.out.println(parity);
      }
    }
  }
@Test
  public void testTranscodeInfoForXORGroups() throws IOException {
    BlockInfoData[] dataStripes = new BlockInfoData[4];
    BlockInfoParity[] oldParities = new BlockInfoParity[4];

    long id0 = toStripedId(32);
    BlockInfoData b1 = new BlockInfoData(new Block(id0), (short) 2, cellSize);
    b1.addStorage(datanodes[0], new Block(id0));
    b1.addStorage(datanodes[1], new Block(id0 + 1));
    b1.setNumBytes(cellSize * 2L);
    dataStripes[0] = b1;

   long id1 = id0 + 32;
    BlockInfoData b2 = new BlockInfoData(new Block(id1), (short) 2, cellSize);
    b2.addStorage(datanodes[3], new Block(id1));
    b2.addStorage(datanodes[4], new Block(id1 + 1));
    b2.setNumBytes(cellSize * 2L);
    dataStripes[1] = b2;

    long id2 = id1 + 32;
    BlockInfoData b3 = new BlockInfoData(new Block(id2), (short) 2, cellSize);
    b3.addStorage(datanodes[1], new Block(id2));
    b3.addStorage(datanodes[3], new Block(id2 + 1));
    b3.setNumBytes(cellSize * 2L);
    dataStripes[2] = b3;

    long id3 = id2 + 32;
    BlockInfoData b4 = new BlockInfoData(new Block(id3), (short) 2, cellSize);
    b4.addStorage(datanodes[2], new Block(id3));
    b4.addStorage(datanodes[4], new Block(id3 + 1));
    b4.setNumBytes(cellSize * 2L);
    dataStripes[3] = b4;

    long id01 = id3 + 32;
    BlockInfoParity b01 = new BlockInfoParity(new Block(id01), (short) 1, cellSize);
    b01.addStorage(datanodes[5], new Block(id01));
    b01.setNumBytes(cellSize * 2L);
    oldParities[0] = b01;

    long id02 = id01 + 32;
    BlockInfoParity b02 = new BlockInfoParity(new Block(id02), (short) 1, cellSize);
    b02.addStorage(datanodes[6], new Block(id02));
    b02.setNumBytes(cellSize * 2L);
    oldParities[1] = b02;

    long id03 = id02 + 32;
    BlockInfoParity b03 = new BlockInfoParity(new Block(id03), (short) 1, cellSize);
    b03.addStorage(datanodes[0], new Block(id03));
    b03.setNumBytes(cellSize * 2L);
    oldParities[2] = b03;

    long id04 = id03 + 32;
    BlockInfoParity b04 = new BlockInfoParity(new Block(id04), (short) 1, cellSize);
    b04.addStorage(datanodes[1], new Block(id04));
    b04.setNumBytes(cellSize * 2L);
    oldParities[3] = b04;





    TranscodeInfo[] transcodeInfos = FSDirDecouplingOp.createNewGeneralizedGroups(fsnMock, dataStripes, oldParities,
          policy_2_1, policy_4_1);
    Assert.assertEquals(2, transcodeInfos.length);

    for(TranscodeInfo tinfo: transcodeInfos){
      System.out.println(tinfo);
      int[] offsetArray = tinfo.getGroupIDOffsets();
      Assert.assertEquals(5, offsetArray.length);

      for(int offset : offsetArray){
        System.out.println(offset);
      }
      Assert.assertEquals(null, tinfo.getParities());

    }


  }

@Test
  public void testCCGroupingForNoDanglingBlocks() throws IOException {
    BlockInfoData[] dataStripes = new BlockInfoData[4];
    BlockInfoParity[] oldParities = new BlockInfoParity[4];

    long id0 = toStripedId(32);
    BlockInfoData b1 = new BlockInfoData(new Block(id0), (short) 2, cellSize);
    b1.addStorage(datanodes[0], new Block(id0));
    b1.addStorage(datanodes[1], new Block(id0 + 1));
    b1.setNumBytes(cellSize * 2L);
    dataStripes[0] = b1;

    long id1 = id0 + 32;
    BlockInfoData b2 = new BlockInfoData(new Block(id1), (short) 2, cellSize);
    b2.addStorage(datanodes[3], new Block(id1));
    b2.addStorage(datanodes[4], new Block(id1 + 1));
    b2.setNumBytes(cellSize * 2L);
    dataStripes[1] = b2;

    long id2 = id1 + 32;
    BlockInfoData b3 = new BlockInfoData(new Block(id2), (short) 2, cellSize);
    b3.addStorage(datanodes[1], new Block(id2));
    b3.addStorage(datanodes[3], new Block(id2 + 1));
    b3.setNumBytes(cellSize * 2L);
    dataStripes[2] = b3;

    long id3 = id2 + 32;
    BlockInfoData b4 = new BlockInfoData(new Block(id3), (short) 2, cellSize);
    b4.addStorage(datanodes[2], new Block(id3));
    b4.addStorage(datanodes[4], new Block(id3 + 1));
    b4.setNumBytes(cellSize * 2L);
    dataStripes[3] = b4;

    long id01 = id3 + 32;
    BlockInfoParity b01 = new BlockInfoParity(new Block(id01), (short) 1, cellSize);
    b01.addStorage(datanodes[5], new Block(id01));
    b01.setNumBytes(cellSize * 2L);
    oldParities[0] = b01;

    long id02 = id01 + 32;
    BlockInfoParity b02 = new BlockInfoParity(new Block(id02), (short) 1, cellSize);
    b02.addStorage(datanodes[6], new Block(id02));
    b02.setNumBytes(cellSize * 2L);
    oldParities[1] = b02;

    long id03 = id02 + 32;
    BlockInfoParity b03 = new BlockInfoParity(new Block(id03), (short) 1, cellSize);
    b03.addStorage(datanodes[0], new Block(id03));
    b03.setNumBytes(cellSize * 2L);
    oldParities[2] = b03;

    long id04 = id03 + 32;
    BlockInfoParity b04 = new BlockInfoParity(new Block(id04), (short) 1, cellSize);
    b04.addStorage(datanodes[1], new Block(id04));
    b04.setNumBytes(cellSize * 2L);
    oldParities[3] = b04;





    BlockInfoGrouped[] groups = FSDirDecouplingOp.createNewGroupsForCC(
          fsnMock, dataStripes, oldParities,
          policy_2_1, policy_4_1);
    Assert.assertEquals(2, groups.length);
    Assert.assertEquals(3, groups[0].getTotalGroupWidth());
    //May need help with understanding expected cell size changes
    //Assert.assertEquals(cellSize * 3, groups[0].getNumBytes());
    Assert.assertEquals(3, groups[1].getTotalGroupWidth());
    //Assert.assertEquals(cellSize * 3, groups[1].getNumBytes());

  }

  @Test
  public void testFromDataOrderingFullStripes() throws IOException {
    BlockInfoData[] dataBlocks = new BlockInfoData[4];
    BlockInfoParity[] parityBlocks = new BlockInfoParity[4];

    long id0 = toStripedId(32);
    BlockInfoData b1 = new BlockInfoData(new Block(id0), (short) 2, cellSize);
    b1.addStorage(datanodes[0], new Block(id0));
    b1.addStorage(datanodes[1], new Block(id0 + 1));
    b1.setNumBytes(cellSize * 2L);
    dataBlocks[0] = b1;

    BlockInfoParity p1 = new BlockInfoParity(new Block(id0 + 16), (short) 1, cellSize);
    p1.addStorage(datanodes[2], new Block(id0 + 16));
    parityBlocks[0] = p1;

    long id1 = id0 + 32;
    BlockInfoData b2 = new BlockInfoData(new Block(id1), (short) 2, cellSize);
    b2.addStorage(datanodes[3], new Block(id1));
    b2.addStorage(datanodes[4], new Block(id1 + 1));
    b2.setNumBytes(cellSize * 2L);
    dataBlocks[1] = b2;

    BlockInfoParity p2 = new BlockInfoParity(new Block(id1 + 16), (short) 1, cellSize);
    p2.addStorage(datanodes[0], new Block(id1 + 16));
    parityBlocks[1] = p2;

    long id2 = id1 + 32;
    BlockInfoData b3 = new BlockInfoData(new Block(id2), (short) 2, cellSize);
    b3.addStorage(datanodes[1], new Block(id2));
    b3.addStorage(datanodes[2], new Block(id2 + 1));
    b3.setNumBytes(cellSize * 2L);
    dataBlocks[2] = b3;

    BlockInfoParity p3 = new BlockInfoParity(new Block(id2 + 16), (short) 1, cellSize);
    p3.addStorage(datanodes[4], new Block(id2 + 16));
    parityBlocks[2] = p3;

    long id3 = id2 + 32;
    BlockInfoData b4 = new BlockInfoData(new Block(id3), (short) 2, cellSize);
    b4.addStorage(datanodes[3], new Block(id3));
    b4.addStorage(datanodes[4], new Block(id3 + 1));
    b4.setNumBytes(cellSize * 2L);
    dataBlocks[3] = b4;

    BlockInfoParity p4 = new BlockInfoParity(new Block(id3 + 16), (short) 1, cellSize);
    p4.addStorage(datanodes[2], new Block(id3 + 16));
    parityBlocks[3] = p4;

    TranscodeInfo[] ti = FSDirDecouplingOp.computeTranscodeOps(fsnMock, dataBlocks, parityBlocks, policy_2_1, policy_4_1);
    Assert.assertEquals(2, ti.length);
  }

  @Test
  public void testFromDataOrderingIncompleteStripe() throws IOException {
    BlockInfoData[] dataBlocks = new BlockInfoData[3];

    long id0 = toStripedId(32);
    BlockInfoData b1 = new BlockInfoData(new Block(id0), (short) 2, cellSize);
    b1.addStorage(datanodes[0], new Block(id0));
    b1.addStorage(datanodes[1], new Block(id0 + 1));
    b1.setNumBytes(cellSize * 2L);
    dataBlocks[0] = b1;

    long id1 = id0 + 32;
    BlockInfoData b2 = new BlockInfoData(new Block(id1), (short) 2, cellSize);
    b2.addStorage(datanodes[3], new Block(id1));
    b2.addStorage(datanodes[4], new Block(id1 + 1));
    b2.setNumBytes(cellSize * 2L);
    dataBlocks[1] = b2;

    long id2 = id1 + 32;
    BlockInfoData b3 = new BlockInfoData(new Block(id2), (short) 2, cellSize);
    b3.addStorage(datanodes[1], new Block(id2));
    b3.setNumBytes(cellSize);
    dataBlocks[2] = b3;

    BlockInfoGrouped[] groups = FSDirDecouplingOp.createNewGroups(fsnMock, dataBlocks, policy_3_1);
    Assert.assertEquals(2, groups.length);
    Assert.assertEquals(4, groups[0].getTotalGroupWidth());
    Assert.assertEquals(cellSize * 3, groups[0].getNumBytes());
    Assert.assertEquals(3, groups[1].getTotalGroupWidth());
    Assert.assertEquals(cellSize * 2, groups[1].getNumBytes());
  }

  @Test
  public void testFromDataOrderingIncompleteStripeBigFileUpTransition() throws IOException {
    BlockInfoData[] dataBlocks = new BlockInfoData[3];

    long id0 = toStripedId(32);
    BlockInfoData b1 = new BlockInfoData(new Block(id0), (short) 6, cellSize);
    b1.addStorage(datanodes[0], new Block(id0));
    b1.addStorage(datanodes[1], new Block(id0 + 1));
    b1.addStorage(datanodes[2], new Block(id0 + 2));
    b1.addStorage(datanodes[3], new Block(id0 + 3));
    b1.addStorage(datanodes[4], new Block(id0 + 4));
    b1.addStorage(datanodes[5], new Block(id0 + 5));
    b1.setNumBytes(bigCellSize * 8 * 6L);
    dataBlocks[0] = b1;

    long id1 = id0 + 32;
    BlockInfoData b2 = new BlockInfoData(new Block(id1), (short) 6, cellSize);
    b2.addStorage(datanodes[3], new Block(id1));
    b2.addStorage(datanodes[4], new Block(id1 + 1));
    b2.addStorage(datanodes[5], new Block(id1 + 2));
    b2.addStorage(datanodes[6], new Block(id1 + 3));
    b2.addStorage(datanodes[0], new Block(id1 + 4));
    b2.addStorage(datanodes[1], new Block(id1 + 5));
    b2.setNumBytes(bigCellSize * 8 * 6L);
    dataBlocks[1] = b2;

    long id2 = id1 + 32;
    BlockInfoData b3 = new BlockInfoData(new Block(id2), (short) 6, cellSize);
    b3.addStorage(datanodes[1], new Block(id2));
    b3.addStorage(datanodes[2], new Block(id2 + 1));
    b3.addStorage(datanodes[3], new Block(id2 + 2));
    b3.addStorage(datanodes[4], new Block(id2 + 3));
    b3.addStorage(datanodes[5], new Block(id2 + 4));
    b3.addStorage(datanodes[6], new Block(id2 + 5));
    b3.setNumBytes(bigCellSize * 8 * 6L);
    dataBlocks[2] = b3;

    BlockInfoGrouped[] groups = FSDirDecouplingOp.createNewGroups(fsnMock, dataBlocks, policy_7_1);
    Assert.assertEquals(3, groups.length);
    Assert.assertEquals(58720256, groups[0].getNumBytes());
    Assert.assertEquals(58720256, groups[1].getNumBytes());
    Assert.assertEquals(33554432, groups[2].getNumBytes());
  }


  @Test
  public void testFromDataOrderingIncompleteStripeBigFileDownTransition() throws IOException {
    BlockInfoData[] dataBlocks = new BlockInfoData[3];

    long id0 = toStripedId(32);
    BlockInfoData b1 = new BlockInfoData(new Block(id0), (short) 6, cellSize);
    b1.addStorage(datanodes[0], new Block(id0));
    b1.addStorage(datanodes[1], new Block(id0 + 1));
    b1.addStorage(datanodes[2], new Block(id0 + 2));
    b1.addStorage(datanodes[3], new Block(id0 + 3));
    b1.addStorage(datanodes[4], new Block(id0 + 4));
    b1.addStorage(datanodes[5], new Block(id0 + 5));
    b1.setNumBytes(bigCellSize * 8 * 6L);
    dataBlocks[0] = b1;

    long id1 = id0 + 32;
    BlockInfoData b2 = new BlockInfoData(new Block(id1), (short) 6, cellSize);
    b2.addStorage(datanodes[3], new Block(id1));
    b2.addStorage(datanodes[4], new Block(id1 + 1));
    b2.addStorage(datanodes[5], new Block(id1 + 2));
    b2.addStorage(datanodes[6], new Block(id1 + 3));
    b2.addStorage(datanodes[0], new Block(id1 + 4));
    b2.addStorage(datanodes[1], new Block(id1 + 5));
    b2.setNumBytes(bigCellSize * 8 * 6L);
    dataBlocks[1] = b2;

    long id2 = id1 + 32;
    BlockInfoData b3 = new BlockInfoData(new Block(id2), (short) 6, cellSize);
    b3.addStorage(datanodes[1], new Block(id2));
    b3.addStorage(datanodes[2], new Block(id2 + 1));
    b3.addStorage(datanodes[3], new Block(id2 + 2));
    b3.addStorage(datanodes[4], new Block(id2 + 3));
    b3.addStorage(datanodes[5], new Block(id2 + 4));
    b3.addStorage(datanodes[6], new Block(id2 + 5));
    b3.setNumBytes(bigCellSize * 8 * 6L);
    dataBlocks[2] = b3;

    BlockInfoGrouped[] groups = FSDirDecouplingOp.createNewGroups(fsnMock, dataBlocks, policy_3_1);
    Assert.assertEquals(6, groups.length);
  }

  @Test
  public void testFromDataOrderingImbalancedStripes() throws IOException {
    BlockInfoData[] dataBlocks = new BlockInfoData[2];

    long id0 = toStripedId(32);
    BlockInfoData b1 = new BlockInfoData(new Block(id0), (short) 2, cellSize);
    b1.addStorage(datanodes[0], new Block(id0));
    b1.addStorage(datanodes[1], new Block(id0 + 1));
    b1.setNumBytes(cellSize * 8L);
    dataBlocks[0] = b1;

    long id1 = id0 + 32;
    BlockInfoData b2 = new BlockInfoData(new Block(id1), (short) 2, cellSize);
    b2.addStorage(datanodes[3], new Block(id1));
    b2.addStorage(datanodes[4], new Block(id1 + 1));
    b2.setNumBytes(cellSize * 2L);
    dataBlocks[1] = b2;

    BlockInfoGrouped[] groups = FSDirDecouplingOp.createNewGroups(fsnMock, dataBlocks, policy_4_1);
    Assert.assertEquals(1, groups.length);
    Assert.assertEquals(5, groups[0].getTotalGroupWidth());
    Assert.assertEquals(cellSize * 10, groups[0].getNumBytes());
  }

  @Test
  public void testFromDataOrderingFromEmpty() throws IOException {
    BlockInfoData[] dataBlocks = new BlockInfoData[2];

    long id0 = toStripedId(32);
    BlockInfoData b1 = new BlockInfoData(new Block(id0), (short) 2, cellSize);
    b1.setNumBytes(cellSize * 8L);
    dataBlocks[0] = b1;

    long id1 = id0 + 32;
    BlockInfoData b2 = new BlockInfoData(new Block(id1), (short) 2, cellSize);
    b2.setNumBytes(cellSize * 8L);
    dataBlocks[1] = b2;

    BlockInfoGrouped[] groups = FSDirDecouplingOp.createNewGroups(fsnMock, dataBlocks, policy_4_1);
    Assert.assertEquals(1, groups.length);
  }


//  @Test
//  public void testFromDataOrderingIncompleteStripeBigFile() {
//    BlockInfoStriped[] dataBlocks = new BlockInfoStriped[3];
//
//    long id0 = toStripedId(32);
//    BlockInfoStriped blockOne = new BlockInfoStriped(new Block(id0), policy_6_1);
//    blockOne.setNumBytes(bigCellSize*8*6);
//    blockOne.addStorage(datanodes[0], new Block(id0));
//    blockOne.addStorage(datanodes[1], new Block(id0 + 1));
//    blockOne.addStorage(datanodes[2], new Block(id0 + 2));
//    blockOne.addStorage(datanodes[3], new Block(id0 + 3));
//    blockOne.addStorage(datanodes[4], new Block(id0 + 4));
//    blockOne.addStorage(datanodes[5], new Block(id0 + 5));
//    blockOne.addStorage(datanodes[6], new Block(id0 + 6));
//    dataBlocks[0] = new BlockInfoData(blockOne, policy_6_0);
//
//    long id1 = id0 + 32;
//    BlockInfoStriped blockTwo = new BlockInfoStriped(new Block(id1), policy_6_1);
//    blockTwo.setNumBytes(10485760*8*6);
//    blockTwo.addStorage(datanodes[3], new Block(id1));
//    blockTwo.addStorage(datanodes[4], new Block(id1 + 1));
//    blockTwo.addStorage(datanodes[5], new Block(id1 + 2));
//    blockTwo.addStorage(datanodes[6], new Block(id1 + 3));
//    blockTwo.addStorage(datanodes[0], new Block(id1 + 4));
//    blockTwo.addStorage(datanodes[1], new Block(id1 + 5));
//    blockTwo.addStorage(datanodes[2], new Block(id1 + 6));
//    dataBlocks[1] = new BlockInfoData(blockTwo, policy_6_0);;
//
//    long id2 = id1 + 32;
//    BlockInfoStriped blockThree = new BlockInfoStriped(new Block(id2), policy_6_1);
//    blockThree.setNumBytes(10485760*8*6);
//    blockThree.addStorage(datanodes[1], new Block(id2));
//    blockThree.addStorage(datanodes[2], new Block(id2 + 1));
//    blockThree.addStorage(datanodes[3], new Block(id2 + 2));
//    blockThree.addStorage(datanodes[4], new Block(id2 + 3));
//    blockThree.addStorage(datanodes[5], new Block(id2 + 4));
//    blockThree.addStorage(datanodes[6], new Block(id2 + 5));
//    blockThree.addStorage(datanodes[0], new Block(id2 + 6));
//    dataBlocks[2] = new BlockInfoData(blockThree, policy_6_0);
//
//    BlockInfoGrouped[] groups = BlockInfoGrouped.determineRedundancyGrouping(
//      dataBlocks, policy_7_1);
//    Assert.assertEquals(2, groups.length);
//    Assert.assertEquals(134217728, groups[0].getNumBytes());
//    Assert.assertEquals(134217728, groups[1].getNumBytes());
//  }
//
//  @Test
//  public void testMatchingParityBlocks() {
//    ECSchema oldSchema = new ECSchema("XOR", 1, 2);
//    ECSchema newSchema = new ECSchema("XOR", 2, 2);
//    ErasureCodingPolicy policy = new ErasureCodingPolicy(newSchema, 2048);
//    policy.setOriginalSchema(oldSchema);
//
//    BlockInfoStriped[] dataLayout = new BlockInfoStriped[3];
//
//    long id0 = toStripedId(32);
//    BlockInfoStriped blockOne = new BlockInfoStriped(
//      new Block(id0), new ErasureCodingPolicy(oldSchema, 2048));
//    blockOne.addStorage(datanodes[0], new Block(id0));
//    blockOne.addStorage(datanodes[1], new Block(id0 + 1));
//    blockOne.addStorage(datanodes[2], new Block(id0 + 2));
//    dataLayout[0] = blockOne;
//
//    long id1 = id0 + 32;
//    BlockInfoStriped blockTwo = new BlockInfoStriped(
//      new Block(id1), new ErasureCodingPolicy(oldSchema, 2048));
//    blockTwo.addStorage(datanodes[3], new Block(id1));
//    blockTwo.addStorage(datanodes[4], new Block(id1 + 1));
//    blockTwo.addStorage(datanodes[0], new Block(id1 + 2));
//    dataLayout[1] = blockTwo;
//
//    long id2 = id1 + 32;
//    BlockInfoStriped blockThree = new BlockInfoStriped(
//      new Block(id2), new ErasureCodingPolicy(oldSchema, 2048));
//    blockThree.addStorage(datanodes[1], new Block(id2));
//    blockThree.addStorage(datanodes[2], new Block(id2 + 1));
//    blockThree.addStorage(datanodes[3], new Block(id2 + 2));
//    dataLayout[2] = blockThree;
//
//    BlockInfoGrouped[] groups = BlockInfoGrouped.determineRedundancyGrouping(
//      dataLayout, policy, map);
//
//    BlockInfoParity[] parities = BlockInfoGrouped.toParityBlocks(groups, policy);
//
//    datanodes[1].addBlock(parities[0], new Block(parities[0].getBlockId() + 1));
//
//    // basically created redundancy groups,
//    // convert to parity groups
//    // then call add storage on blockinfostriped for that new reported block
//    // reported block = new parity block
//    // block group = original parity group
//  }
//
//  @Test
//  public void testFromDataOrderingManyParity() {
//    ECSchema oldSchema = new ECSchema("XOR", 2, 1);
//    ECSchema newSchema = new ECSchema("XOR", 3, 3);
//    ErasureCodingPolicy policy = new ErasureCodingPolicy(newSchema, 2048);
//    policy.setOriginalSchema(oldSchema);
//
//    BlockInfoStriped[] dataLayout = new BlockInfoStriped[3];
//
//    long id0 = toStripedId(32);
//    BlockInfoStriped blockOne = new BlockInfoStriped(
//      new Block(id0), new ErasureCodingPolicy(oldSchema, 2048));
//    blockOne.addStorage(datanodes[0], new Block(id0));
//    blockOne.addStorage(datanodes[1], new Block(id0 + 1));
//    blockOne.addStorage(datanodes[2], new Block(id0 + 2));
//    dataLayout[0] = blockOne;
//
//    long id1 = id0 + 32;
//    BlockInfoStriped blockTwo = new BlockInfoStriped(
//      new Block(id1), new ErasureCodingPolicy(oldSchema, 2048));
//    blockTwo.addStorage(datanodes[3], new Block(id1));
//    blockTwo.addStorage(datanodes[4], new Block(id1 + 1));
//    blockTwo.addStorage(datanodes[0], new Block(id1 + 2));
//    dataLayout[1] = blockTwo;
//
//    long id2 = id1 + 32;
//    BlockInfoStriped blockThree = new BlockInfoStriped(
//      new Block(id2), new ErasureCodingPolicy(oldSchema, 2048));
//    blockThree.addStorage(datanodes[1], new Block(id2));
//    // missing the second data block
//    blockThree.addStorage(datanodes[3], new Block(id2 + 2));
//    dataLayout[2] = blockThree;
//
//    BlockInfoGrouped[] groups = BlockInfoGrouped.determineRedundancyGrouping(
//      dataLayout, policy, map);
//    Assert.assertEquals(2, groups.length);
//    Assert.assertEquals(6, groups[0].getSize());
//    Assert.assertEquals(5, groups[1].getSize());
//    Assert.assertEquals(1, map.get(id0).size());
//    Assert.assertEquals(2, map.get(id1).size());
//    Assert.assertEquals(1, map.get(id2).size());
//  }
//
//  @Test
//  public void testFromDataOrderingOnlyDataBlocks() {
//    ECSchema oldSchema = new ECSchema("XOR", 2, 1);
//    ECSchema newSchema = new ECSchema("XOR", 3, 1);
//    ErasureCodingPolicy oldPolicy = new ErasureCodingPolicy(oldSchema, 2048);
//    ErasureCodingPolicy policy = new ErasureCodingPolicy(newSchema, 2048);
//    policy.setOriginalSchema(oldSchema);
//
//    BlockInfoData[] dataLayout = new BlockInfoData[3];
//
//    long id0 = toStripedId(32);
//    BlockInfoStriped blockOne = new BlockInfoStriped(
//      new Block(id0), new ErasureCodingPolicy(oldSchema, 2048));
//    blockOne.addStorage(datanodes[0], new Block(id0));
//    blockOne.addStorage(datanodes[1], new Block(id0 + 1));
//    blockOne.addStorage(datanodes[2], new Block(id0 + 2));
//    dataLayout[0] = new BlockInfoData(blockOne, oldPolicy);
//
//    long id1 = id0 + 32;
//    BlockInfoStriped blockTwo = new BlockInfoStriped(
//      new Block(id1), new ErasureCodingPolicy(oldSchema, 2048));
//    blockTwo.addStorage(datanodes[3], new Block(id1));
//    blockTwo.addStorage(datanodes[4], new Block(id1 + 1));
//    blockTwo.addStorage(datanodes[0], new Block(id1 + 2));
//    dataLayout[1] = new BlockInfoData(blockTwo, oldPolicy);
//
//    long id2 = id1 + 32;
//    BlockInfoStriped blockThree = new BlockInfoStriped(
//      new Block(id2), new ErasureCodingPolicy(oldSchema, 2048));
//    blockThree.addStorage(datanodes[1], new Block(id2));
//    blockThree.addStorage(datanodes[2], new Block(id2 + 1));
//    blockThree.addStorage(datanodes[3], new Block(id2 + 2));
//    dataLayout[2] = new BlockInfoData(blockThree, oldPolicy);
//
//    BlockInfoGrouped[] groups = BlockInfoGrouped.determineRedundancyGrouping(
//      dataLayout, policy, map);
//    Assert.assertEquals(2, groups.length);
//    Assert.assertEquals(4, groups[0].getSize());
//    Assert.assertEquals(4, groups[1].getSize());
//    Assert.assertEquals(1, map.get(id0).size());
//    Assert.assertEquals(2, map.get(id1).size());
//    Assert.assertEquals(1, map.get(id2).size());
//  }
//
//  @Test
//  public void testFromDataOrderingFullyMissingBlocks() {
//    BlockInfoData[] dataBlocks = new BlockInfoData[3];
//
//    // TODO: pass this test case, despite having no storage or indices, make this algorithm
//    // TODO: spit out the structure as the ones above
//    long id0 = toStripedId(32);
//    BlockInfoStriped blockOne = new BlockInfoStriped(new Block(id0), policy_2_1);
//    dataBlocks[0] = new BlockInfoData(blockOne, policy_2_0);
//
//    long id1 = id0 + 32;
//    BlockInfoStriped blockTwo = new BlockInfoStriped(new Block(id1), policy_2_1);
//    dataBlocks[1] = new BlockInfoData(blockTwo, policy_2_0);
//
//    long id2 = id1 + 32;
//    BlockInfoStriped blockThree = new BlockInfoStriped(new Block(id2), policy_2_1);
//    dataBlocks[2] = new BlockInfoData(blockThree, policy_2_0);
//
//    BlockInfoGrouped[] groups = BlockInfoGrouped.determineRedundancyGrouping(
//      dataBlocks, policy_3_1);
//    Assert.assertEquals(2, groups.length);
//    Assert.assertEquals(4, groups[0].getSize());
//    Assert.assertEquals(4, groups[1].getSize());
//  }
//
//  @Test
//  public void testFromDataOrderingTruncatedLast() {
//    BlockInfoData[] dataBlocks = new BlockInfoData[3];
//
//    long id0 = toStripedId(32);
//    BlockInfoStriped blockOne = new BlockInfoStriped(new Block(id0), policy_2_1);
//    dataBlocks[0] = new BlockInfoData(blockOne, policy_2_0);
//
//    long id1 = id0 + 32;
//    BlockInfoStriped blockTwo = new BlockInfoStriped(new Block(id1), policy_2_1);
//    dataBlocks[1] = new BlockInfoData(blockTwo, policy_2_0);
//
//    long id2 = id1 + 32;
//    BlockInfoStriped blockThree = new BlockInfoStriped(new Block(id2), policy_2_1);
//    dataBlocks[2] = new BlockInfoData(blockThree, policy_1_0);
//
//    BlockInfoGrouped[] groups = BlockInfoGrouped.determineRedundancyGrouping(
//      dataBlocks, policy_3_1);
//    Assert.assertEquals(2, groups.length);
//    Assert.assertEquals(4, groups[0].getSize());
//    Assert.assertEquals(3, groups[1].getSize());
//  }
//
//  @Test
//  public void testLastDataBlockTruncate() {
//    BlockInfoData[] dataBlocks = new BlockInfoData[3];
//
//    long id0 = toStripedId(32);
//    BlockInfoStriped blockOne = new BlockInfoStriped(new Block(id0), policy_2_1);
//    blockOne.addStorage(datanodes[0], new Block(id0));
//    blockOne.addStorage(datanodes[1], new Block(id0 + 1));
//    blockOne.addStorage(datanodes[2], new Block(id0 + 2));
//    blockOne.setNumBytes(2048*2);
//
//    long id1 = id0 + 32;
//    BlockInfoStriped blockTwo = new BlockInfoStriped(new Block(id1), policy_2_1);
//    blockTwo.setNumBytes(2048);
//    dataBlocks[1] = new BlockInfoData(blockTwo, policy_2_0);
//
//    BlockInfoData truncated = new BlockInfoData(dataBlocks[1], new ErasureCodingPolicy(
//      new ECSchema(policy_2_1.getCodecName(),
//        dataBlocks[1].getRealDataBlockNum(),
//        0), policy_2_1.getCellSize()));
//    Assert.assertEquals(1, truncated.getCapacity());
//  }
//
//  @Test
//  public void testSplitDataParity() {
//    ECSchema schema = new ECSchema("XOR", 4, 3);
//    ErasureCodingPolicy policy = new ErasureCodingPolicy(schema, 2048);
//    ErasureCodingPolicy dataPolicy = ErasureCodingPolicy.nullifyParity(policy);
//    ErasureCodingPolicy parityPolicy = ErasureCodingPolicy.nullifyData(policy);
//
//    long id0 = toStripedId(32);
//    BlockInfoStriped block = new BlockInfoStriped(
//      new Block(id0), policy);
//    block.addStorage(datanodes[0], new Block(id0));
//    block.addStorage(datanodes[1], new Block(id0 + 1));
//    block.addStorage(datanodes[2], new Block(id0 + 2));
//    block.addStorage(datanodes[3], new Block(id0 + 3));
//    block.addStorage(datanodes[4], new Block(id0 + 4));
//    block.addStorage(datanodes[5], new Block(id0 + 5));
//    block.addStorage(datanodes[6], new Block(id0 + 6));
//
//    BlockInfoData dataLayout = new BlockInfoData(block, dataPolicy);
//    BlockInfoParity parityBlocks = new BlockInfoParity(block, parityPolicy, policy.getNumDataUnits());
//
//    Assert.assertEquals(4, dataLayout.storages.length);
//    Assert.assertEquals(3, parityBlocks.storages.length);
//  }
//
//  @Test
//  public void testSplitDataParityMissingInStripe() {
//    ECSchema schema = new ECSchema("XOR", 4, 3);
//    ErasureCodingPolicy policy = new ErasureCodingPolicy(schema, 2048);
//    ErasureCodingPolicy dataPolicy = ErasureCodingPolicy.nullifyParity(policy);
//    ErasureCodingPolicy parityPolicy = ErasureCodingPolicy.nullifyData(policy);
//
//    long id0 = toStripedId(32);
//    BlockInfoStriped block = new BlockInfoStriped(
//      new Block(id0), policy);
//    block.addStorage(datanodes[0], new Block(id0));
//    block.addStorage(datanodes[1], new Block(id0 + 1));
//    block.addStorage(datanodes[2], new Block(id0 + 2));
//    block.addStorage(datanodes[4], new Block(id0 + 4));
//    block.addStorage(datanodes[5], new Block(id0 + 5));
//    block.addStorage(datanodes[6], new Block(id0 + 6));
//
//    BlockInfoData dataLayout = new BlockInfoData(block, dataPolicy);
//    BlockInfoParity parityBlocks = new BlockInfoParity(block, parityPolicy, policy.getNumDataUnits());
//
//    Assert.assertEquals(4, dataLayout.storages.length);
//    Assert.assertFalse(dataLayout.isIndexSet(3));
//    Assert.assertEquals(3, parityBlocks.storages.length);
//  }
//
//  @Test
//  public void testToParityBlocks() {
//    ECSchema oldSchema = new ECSchema("XOR", 1, 2);
//    ECSchema newSchema = new ECSchema("XOR", 2, 2);
//    ErasureCodingPolicy policy = new ErasureCodingPolicy(newSchema, 2048);
//    policy.setOriginalSchema(oldSchema);
//
//    BlockInfoStriped[] dataLayout = new BlockInfoStriped[3];
//
//    long id0 = toStripedId(32);
//    BlockInfoStriped blockOne = new BlockInfoStriped(
//      new Block(id0), new ErasureCodingPolicy(oldSchema, 2048));
//    blockOne.addStorage(datanodes[0], new Block(id0));
//    blockOne.addStorage(datanodes[1], new Block(id0 + 1));
//    blockOne.addStorage(datanodes[2], new Block(id0 + 2));
//    dataLayout[0] = blockOne;
//
//    long id1 = id0 + 32;
//    BlockInfoStriped blockTwo = new BlockInfoStriped(
//      new Block(id1), new ErasureCodingPolicy(oldSchema, 2048));
//    blockTwo.addStorage(datanodes[3], new Block(id1));
//    blockTwo.addStorage(datanodes[4], new Block(id1 + 1));
//    blockTwo.addStorage(datanodes[0], new Block(id1 + 2));
//    dataLayout[1] = blockTwo;
//
//    long id2 = id1 + 32;
//    BlockInfoStriped blockThree = new BlockInfoStriped(
//      new Block(id2), new ErasureCodingPolicy(oldSchema, 2048));
//    blockThree.addStorage(datanodes[1], new Block(id2));
//    blockThree.addStorage(datanodes[2], new Block(id2 + 1));
//    blockThree.addStorage(datanodes[3], new Block(id2 + 2));
//    dataLayout[2] = blockThree;
//
//    BlockInfoGrouped[] groups = BlockInfoGrouped.determineRedundancyGrouping(
//      dataLayout, policy, map);
//
//    BlockInfoParity[] parities = BlockInfoGrouped.toParityBlocks(groups, policy);
//    Assert.assertEquals(2, parities.length);
//    Assert.assertEquals(2, parities[0].storages.length);
//  }

  static long toStripedId(long id) {
    final long BLOCK_ID_MASK_STRIPED = 1L << 63;
    return id | (BLOCK_ID_MASK_STRIPED);
  }
}
