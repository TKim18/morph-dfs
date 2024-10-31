package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.ECSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.io.erasurecode.CodecUtil.IO_ERASURECODE_CODEC_CC;

final class FSDirDecouplingOp {

  /**
   * Private constructor for preventing FSDirErasureCodingOp object
   * creation. Static-only class.
   */
  private FSDirDecouplingOp() {}

  static TranscodeInfo[] createNewGeneralizedGroups(
      final FSNamesystem fsn, BlockInfo[] stripes, BlockInfo[] oldParities,
      ErasureCodingPolicy oldPolicy, ErasureCodingPolicy newPolicy) throws IOException {

    int oldParityNum = oldPolicy.getNumParityUnits();
    int newParityNum = newPolicy.getNumParityUnits();
    int oldDataNum = oldPolicy.getNumDataUnits();
    int newDataNum = newPolicy.getNumDataUnits();
    int numStripes = stripes.length;
    int stripeSize = stripes[0].getCapacity();
    int lastStripeBlockNum = ((BlockInfoData)stripes[numStripes-1]).getRealDataBlockNum();
    //Might need a check that stripes isn't empty
    int totalFileBlocks =  stripeSize * (numStripes-1) + lastStripeBlockNum;
    int numDanglingBlocks = totalFileBlocks % newDataNum;
    int numGroupings = (totalFileBlocks - numDanglingBlocks) / newDataNum;
    String codec = newPolicy.getCodecName();

    TranscodeInfo[] transcodeInfos;

    if (numDanglingBlocks != 0) {
      transcodeInfos = new TranscodeInfo[numGroupings + 1];
      BlockInfoGrouped finalBlockGroup = new BlockInfoGrouped(fsn.createNewBlock(BlockType.GROUPED), new ErasureCodingPolicy(
          new ECSchema(newPolicy.getCodecName(), numDanglingBlocks, newParityNum), newPolicy.getCellSize()));

      transcodeInfos[numGroupings] = new TranscodeInfo(finalBlockGroup, null, null);;

    } else {
      transcodeInfos = new TranscodeInfo[numGroupings];
    }

    int overallIndex = 0;
    int groupNum = 0;

    for (int i = 0; i < numGroupings; i++) {

      BlockInfoGrouped group = new BlockInfoGrouped(fsn.createNewBlock(BlockType.GROUPED), newPolicy);

      for (int j = 0; j < newDataNum; j++) {
        int stripeIndex = overallIndex % stripeSize;

        int stripeNum = (overallIndex - stripeIndex) / stripeSize;

        BlockInfo stripe = stripes[stripeNum];

        long blockSize = StripedBlockUtil.getInternalBlockLength(
            stripe.getNumBytes(), oldPolicy.getCellSize(),
            stripe.getReplication(), j);
        long genStamp = stripe.getGenerationStamp();
        group.setGenerationStamp(Math.min(group.getGenerationStamp(), genStamp));


        group.setBlockIdAtIndex(j, stripe.getBlockId() + stripeIndex);
        group.setBlockLengthAtIndex(j, blockSize);
        stripe.storages[stripeIndex].setBlock(group, j);

        overallIndex++;
        int groupIndex = overallIndex % oldDataNum;
        groupNum = (overallIndex - groupIndex) / oldDataNum;
      }

      TranscodeInfo newInfoGroup;

      if (codec.equals("cc") && newDataNum % oldDataNum == 0){

        BlockInfoParity[] relevantParities = new BlockInfoParity[(newDataNum/oldDataNum)];
        int parityPosition = 0;
        for (int k = (groupNum - (newDataNum/oldDataNum)) * oldParityNum; k < groupNum; k++) {
          relevantParities[parityPosition++] = (BlockInfoParity) oldParities[k];
        }

        newInfoGroup = new TranscodeInfo(group, oldPolicy.getSchema(), relevantParities);

      } else {
        newInfoGroup = new TranscodeInfo(group, null, null);

      }

      transcodeInfos[i] = newInfoGroup;

    }

    return transcodeInfos;

  }



  static TranscodeInfo[] computeTranscodeOps(
      final FSNamesystem fsn, BlockInfo[] stripes, BlockInfo[] parities,
      ErasureCodingPolicy oldPolicy, ErasureCodingPolicy newPolicy) throws IOException {

    // TODO: change the name of this
    boolean mergable_cc =
        "cc".equals(oldPolicy.getCodecName())
            && "cc".equals(newPolicy.getCodecName())
            && newPolicy.getNumDataUnits() % oldPolicy.getNumDataUnits() == 0
            && oldPolicy.getNumParityUnits() == newPolicy.getNumParityUnits();

    if (mergable_cc == false) {
      mergable_cc = "cc".equals(oldPolicy.getCodecName())
      && "lrc".equals(newPolicy.getCodecName())
      && newPolicy.getNumDataUnits() % oldPolicy.getNumDataUnits() == 0   // merge regime
      && newPolicy.getNumDataUnits() / oldPolicy.getNumDataUnits() == newPolicy.getNumLocalParityUnits()    // for now only supporting the case when #local parities = #groups being merged (merge factor)
      && newPolicy.getNumParityUnits() == oldPolicy.getNumParityUnits() - 1;    // the first parity of each initial group becomes a local parity in the new group, the rest become global parities.
    }

    // @todo verify conditions of bcc mergable.
    boolean mergable_bcc =
            "bcc".equals(oldPolicy.getCodecName())
                    && "bcc".equals(newPolicy.getCodecName())
                    && newPolicy.getNumDataUnits() % oldPolicy.getNumDataUnits() == 0
                    && newPolicy.getNumParityUnits() > oldPolicy.getNumParityUnits();

    boolean mergable = mergable_cc || mergable_bcc;

    int oldDataNum = oldPolicy.getNumDataUnits();
    int newDataNum = newPolicy.getNumDataUnits();
    int oldParityNum = oldPolicy.getNumParityUnits();
    int newParityNum = newPolicy.getNumParityUnits();

    int numStripes = stripes.length;
    int stripeWidth = stripes[0].getReplication();

    int lastStripeBlockNum = ((BlockInfoData)stripes[numStripes-1]).getRealDataBlockNum();
    // might need a check that stripes isn't empty
    int totalDataBlocks = stripeWidth * (numStripes - 1) + lastStripeBlockNum;

    int danglingGroupLength = totalDataBlocks % newDataNum;
    int numFullGroups = totalDataBlocks / newDataNum;
    int numDanglingGroups = danglingGroupLength > 0 ? 1 : 0;
    int numNewGroups = numFullGroups + numDanglingGroups;

    TranscodeInfo[] transcodeOps = new TranscodeInfo[numNewGroups];

    int stripeIndex = 0, stripePosition = 0, globalIndex = 0, currentGroup = 0, groupPosition;
    BlockInfo stripe = stripes[stripeIndex];
    int currStripeWidth = ((BlockInfoData) stripes[stripeIndex]).getRealDataBlockNum();

    for (int g = 0; g < numNewGroups; g++) {
      BlockInfoGrouped group;
      int dataPerGroup;

      // handle dangling group case
      if (g != numFullGroups) {
        group = new BlockInfoGrouped(fsn.createNewBlock(BlockType.GROUPED), newPolicy);
        dataPerGroup = newDataNum;
      } else {
        group = new BlockInfoGrouped(fsn.createNewBlock(BlockType.GROUPED), new ErasureCodingPolicy(
            new ECSchema(newPolicy.getCodecName(), danglingGroupLength, newParityNum), newPolicy.getCellSize()));
        dataPerGroup = danglingGroupLength;
      }

      if (!mergable) {
        group.setGenerationStamp(Math.min(group.getGenerationStamp(), stripe.getGenerationStamp()));
      }
      group.setBlockCollectionId(stripe.getBlockCollectionId());

      // fill the data grouping
      for (int d = 0; d < dataPerGroup; d++) {
        long blockSize = StripedBlockUtil.getInternalBlockLength(
            stripe.getNumBytes(), oldPolicy.getCellSize(),
            stripe.getReplication(), d);

        group.setBlockIdAtIndex(d, stripe.getBlockId() + stripePosition);
        group.setBlockLengthAtIndex(d, blockSize);

        if (stripe.storages[stripePosition] != null) {
          stripe.storages[stripePosition].setBlock(group, d);
        }

        globalIndex++;
        // pull from a new stripe if out
        if (stripePosition == currStripeWidth - 1) {
          stripePosition = 0;
          if (stripeIndex++ == numStripes - 1) break;
          stripe = stripes[stripeIndex];
          currStripeWidth = ((BlockInfoData) stripes[stripeIndex]).getRealDataBlockNum();
        } else {
          stripePosition++;
        }
      }

      // fill the protecting parities
      for (int p = 0; p < newParityNum; p++) {
        group.setBlockIdAtIndex(dataPerGroup + p, group.getBlockId() + p);
      }

      BlockInfoParity[] mergableParities = null;
      if (mergable) {
        int numGroupsPutTogether = newDataNum / oldDataNum;
        mergableParities = new BlockInfoParity[numGroupsPutTogether];

        // take however many parities we have iterated over thus far
        // and append them into mergable parities
        for (int k = 0; k < numGroupsPutTogether; k++) {
          mergableParities[k] = (BlockInfoParity) parities[k + currentGroup];
        }
        currentGroup += numGroupsPutTogether;
      }

      transcodeOps[g] = new TranscodeInfo(group, oldPolicy.getSchema(), mergableParities);
    }

    return transcodeOps;
  }

  static BlockInfoGrouped[] createNewGroupsForCC(
      final FSNamesystem fsn, BlockInfo[] stripes, BlockInfo[] oldParities,
      ErasureCodingPolicy oldPolicy, ErasureCodingPolicy newPolicy) throws IOException {
    // TODO: implement
    // specifications:
    // given current state of file (data/parities/stripe + encoding scheme)
    // return groupings that can form new parity blocks
    // e.g.: 6-of-3 to 12-of-15, return BlockInfoGrouped[oldp1a, oldp1b, oldp2a, oldp2b, newp1, newp2]
    // where [oldp1a, oldp1b, oldp2a, oldp2b (is the inputs to transition and) newp1, newp2] (are the outputs)
    // within blockinfogrouped, set storage location, block id, block length for inputs
    // when generating new block ids for newp1 and newp2, use fsn.createNewBlock(BlockType.GROUPED)

    int oldParityNum = oldPolicy.getNumParityUnits();
    int newParityNum = newPolicy.getNumParityUnits();
    int oldDataNum = oldPolicy.getNumDataUnits();
    int newDataNum = newPolicy.getNumDataUnits();

    int groupsPerMergeGrouping = newDataNum/oldDataNum;

    int numStripes = stripes.length;
    int lastStripeBlockNum = ((BlockInfoData)stripes[numStripes-1]).getRealDataBlockNum();
    //Might need a check that stripes isn't empty
    int totalFileBlocks = stripes[0].getCapacity() * (numStripes-1) + lastStripeBlockNum;

    int numDanglingBlocks = totalFileBlocks % newDataNum;
    int numMergeGroupings = (totalFileBlocks - numDanglingBlocks) / newDataNum;

    BlockInfoGrouped[] newGroups;

    if (numDanglingBlocks != 0) {
      newGroups = new BlockInfoGrouped[numMergeGroupings + 1];
      BlockInfoGrouped finalBlockGroup = new BlockInfoGrouped(fsn.createNewBlock(BlockType.GROUPED), new ErasureCodingPolicy(
          new ECSchema(newPolicy.getCodecName(), numDanglingBlocks, newParityNum), newPolicy.getCellSize()));

      newGroups[numMergeGroupings] = finalBlockGroup;

    } else {
      newGroups = new BlockInfoGrouped[numMergeGroupings];
    }



    for(int i = 0; i < numMergeGroupings; i++){
      BlockInfoGrouped mergedGroup = new BlockInfoGrouped(fsn.createNewBlock(BlockType.GROUPED), newPolicy);
      int mergedGroupIndex = 0;
      for(int j = i * groupsPerMergeGrouping; j < (i+1) * groupsPerMergeGrouping; j++){
        long paritySetBlockID = oldParities[j].getBlockId();
        System.out.println(paritySetBlockID);
        for(int k = 0; k < oldParityNum; k++){
          mergedGroup.setBlockIdAtIndex(mergedGroupIndex, paritySetBlockID + k);
          oldParities[j].storages[k].setBlock(mergedGroup, mergedGroupIndex);
          mergedGroupIndex++;
        }
      }

      for (int p = 0; p < newParityNum; p++){
        mergedGroup.setBlockIdAtIndex(mergedGroupIndex, fsn.createNewBlock(BlockType.GROUPED).getBlockId());
        mergedGroupIndex++;
      }

      newGroups[i] = mergedGroup;
    }




    return newGroups;
  }

  static BlockInfoGrouped[] createNewGroups(
      final FSNamesystem fsn, BlockInfo[] blocks, ErasureCodingPolicy ecPolicy) throws IOException {
    List<BlockInfoGrouped> groups = new ArrayList<>();

    int newNumData = ecPolicy.getNumDataUnits();
    int newNumParity = ecPolicy.getNumParityUnits();

    BlockInfoGrouped group = null; // current building group
    int pos = 0; // current index in the group
    // current data stripe
    for (BlockInfo stripe : blocks) {
      long id = stripe.getBlockId();
      long genStamp = stripe.getGenerationStamp();
      long dataUnits = ((BlockInfoData) stripe).getRealDataBlockNum();
      for (int d = 0; d < dataUnits; d++) {
        if (group == null) {
          group = new BlockInfoGrouped(fsn.createNewBlock(BlockType.GROUPED), ecPolicy);
          group.setGenerationStamp(Math.min(group.getGenerationStamp(), genStamp));
          group.setBlockCollectionId(stripe.getBlockCollectionId());
        }

        long blockSize = StripedBlockUtil.getInternalBlockLength(
            stripe.getNumBytes(), ecPolicy.getCellSize(),
            stripe.getReplication(), d);

        group.setBlockIdAtIndex(pos, id + d);
        group.setBlockLengthAtIndex(pos, blockSize);

        if (stripe.storages[d] != null) {
          stripe.storages[d].setBlock(group, pos);
        }
        pos++;

        // check if we reached parities in new group
        if (pos == newNumData) {
          // fill the remainder and reset
          for (int p = 0; p < newNumParity; p++) {
            group.setBlockIdAtIndex(pos, group.getBlockId() + p);
            pos++;
            // key is to not set any storage so ec reconstruction happens automatically
          }
          groups.add(group);

          // reset
          group = null;
          pos = 0;
        }
      }
    }

    // fill last group with parity and add if not empty
    if (group != null) {
      // last group is not filled, must shrink its width and ecpolicy
      newNumData = pos;
      BlockInfoGrouped finalGroup = new BlockInfoGrouped(group, new ErasureCodingPolicy(
          new ECSchema(ecPolicy.getCodecName(), newNumData, newNumParity), ecPolicy.getCellSize()));
      finalGroup.copyFrom(group);

      // fill parities from the back
      for (int p = newNumParity - 1; p >= 0; p--) {
        finalGroup.setBlockIdAtIndex(newNumData + p, group.getBlockId() + p);
      }
      groups.add(finalGroup);
    }

    return groups.toArray(new BlockInfoGrouped[0]);
  }

  public static BlockInfoParity[] toParityBlocks(TranscodeInfo[] infos) {
    BlockInfoParity[] parities = new BlockInfoParity[infos.length];
    for (int i = 0; i < infos.length; i++) {
      BlockInfoGrouped group = infos[i].getGroup();
      long paritySize = (group.getNumBytes() / group.getDataBlockNum()) * group.getParityBlockNum();
      parities[i] = new BlockInfoParity(infos[i].getGroup(), infos[i].getGroup().getParityBlockNum() + infos[i].getGroup().getLocalParityBlockNum(), infos[i].getGroup().getCellSize());
      parities[i].setNumBytes(paritySize);
    }
    return parities;
  }
}
