package org.apache.hadoop.hdfs.server.ectransitioner;

import org.apache.commons.cli.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * The ECTransitioner is a utility that transitions a given file from its current
 * erasure-coding scheme to a new specific erasure-coding scheme. It achieves this
 * by retaining the cell striping on as many datablocks as it can, before moving data
 * from cells to the new datablock's cells. It also reflects the movement properly in
 * the metadata such that reads are possible on the transitioned file, though writes,
 * updates, and appends are no longer valid operations.
 */

@InterfaceAudience.Private
public class ECTransitioner {

    static final Logger LOG = LoggerFactory.getLogger(ECTransitioner.class);
    static final Path TRANSITIONER_ID_PATH = new Path("/system/ectrans.id");

    private final NameNodeConnector nnc;
    private final DistributedFileSystem dfs;
    private final DFSClient client;
    private final CellMover mover;
    private final Configuration conf;

    private final String fileName;
    private final ECSchema targetSchema;
    private ErasureCodingPolicy currentPolicy;
    private ErasureCodingPolicy targetPolicy;

    // data unit difference between target and current
    private int dataDiff;

    // specified file's original block configuration
    private List<LocatedBlock> blocks;

    ECTransitioner(NameNodeConnector nnc, ECTransitionerParameters params,
                   Configuration conf) {
        // TODO: add conf params for limits
        LOG.info("Initializing ECTransitioner");
        this.nnc = nnc;
        this.dfs = nnc.getDistributedFileSystem();
        this.client = dfs.getClient();
        this.mover = new CellMover(nnc, conf);
        this.conf = conf;

        // TODO: grab current schema from querying file on namenode
        this.fileName = params.getFileName();
        this.targetSchema = params.getTargetSchema();
    }

    private ExitStatus transition() {
        LOG.info("Executing TRANSITION!");
        LOG.info("FilesInput = " + fileName);
        LOG.info("TargetSchema = " + targetSchema.toString());

        String[] fileNames = fileName.split(",");
        LOG.info("FileNames Length = " + fileNames.length);
        for (String filePath : fileNames) {

            try {
                LOG.info("Transitioning file={}", filePath);
                // get complete file metadata (e.g. ECPolicy, LocatedBlocks)
//                HdfsLocatedFileStatus file = dfs.getClient().getLocatedFileInfo(filePath, true);
//                LOG.info("Original file has stats: {}", file);
//                LOG.info("Current erasure coded policy on the file: {}", file.getErasureCodingPolicy());

                // validate parameters and file setup

//                if (validatePolicy(file) != ExitStatus.SUCCESS
//                        || validateBlock(file) != ExitStatus.SUCCESS) {
//                    return ExitStatus.NO_MOVE_BLOCK;
//                }
                targetPolicy = new ErasureCodingPolicy(targetSchema, 1024*1024);

                LOG.info("Going to target policy = {}", targetPolicy);
                client.getNamenode().setErasureCodingPolicy(filePath, targetPolicy.getName());
            } catch (FileNotFoundException e) {
                LOG.error("Path not found, must specify a file that exists: " + e.getMessage());
                return ExitStatus.ILLEGAL_ARGUMENTS;
            } catch (IOException e) {
                LOG.error("Received IO Exception", e);
                return ExitStatus.IO_EXCEPTION;
            }
        }
        return ExitStatus.SUCCESS;
    }

    private List<DatanodeInfo> pickNewDatanodes(LocatedStripedBlock blockGroup) throws IOException {
        Set<String> currentDns = new HashSet<>();

        // get the current block's datanodes
        for (DatanodeInfoWithStorage currentDn : blockGroup.getLocations()) {
            currentDns.add(currentDn.getXferAddr());
        }

        // get all datanodes in cluster
        List<DatanodeStorageReport> allDns = Arrays.asList(nnc.getLiveDatanodeStorageReport());
        if (allDns.size() < currentDns.size() + dataDiff) {
            // not enough datanodes to move to
            LOG.warn("Difference in dataunits is more than the number of new datanodes available," +
                    " current size, available size, and diff: " + currentDns.size() + allDns.size() + dataDiff);
            return new ArrayList<>();
        }

        // randomize datanode selection for now, in the future make a more educated decision
        Collections.shuffle(allDns);

        // iterate all datanodes and pick diff # of nodes not in current addrs
        List<DatanodeInfo> newDns = new ArrayList<>();
        int remainingDns = dataDiff;
        for (DatanodeStorageReport dn : allDns) {
            final DatanodeInfo info = dn.getDatanodeInfo();
            if (!currentDns.contains(info.getXferAddr())) {
                LOG.info("Picking: " + info + " to move to.");
                newDns.add(info);
                if (--remainingDns == 0) {
                    break;
                }
            }
        }
        LOG.info("Picked: " + newDns.size() + "nodes.");
        return newDns;
    }

    private ExitStatus validatePolicy(HdfsLocatedFileStatus file) {
        // validate ecpolicy schemes
        currentPolicy = file.getErasureCodingPolicy();
        ECSchema currentSchema;
        if (currentPolicy == null) {
            LOG.warn("File is not currently erasure coded, cannot transition non-striped files");
            return ExitStatus.ILLEGAL_ARGUMENTS;
        } else {
            currentSchema = currentPolicy.getSchema();
            LOG.info("CurrentSchema = " + currentSchema.toString());
        }

        dataDiff = targetSchema.getNumDataUnits() - currentSchema.getNumDataUnits();

        if (!(currentSchema.getCodecName()).equalsIgnoreCase(targetSchema.getCodecName())) {
            LOG.warn("File is using a different codec, not exiting but just letting ya know");
        }
        targetPolicy = new ErasureCodingPolicy(targetSchema, currentPolicy.getCellSize());
        return ExitStatus.SUCCESS;
    }

    private ExitStatus validateBlock(HdfsLocatedFileStatus file) {
        // read block-level metadata for the file
        blocks = file.getLocatedBlocks().getLocatedBlocks();
        if (blocks.size() == 0) {
            LOG.warn("No allocated blocks for this file, exiting");
            return ExitStatus.ILLEGAL_ARGUMENTS;
        }

        for (int i = 0; i < blocks.size(); i++) {
            final LocatedBlock block = blocks.get(i);
//            LOG.info(i + ": Block entry = " + block.toString());
        }

        return ExitStatus.SUCCESS;
    }

    static int run(Collection<URI> nns, Collection<String> nsids,
                   ECTransitionerParameters params, Configuration conf) {
        LOG.info("Running ECTransitioner with default sleep time");
        final long sleepTime =
                conf.getTimeDuration(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
                        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT,
                        TimeUnit.SECONDS, TimeUnit.MILLISECONDS) * 2 +
                        conf.getTimeDuration(
                                DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
                                DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_DEFAULT,
                                TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
        List<NameNodeConnector> connectors = Collections.emptyList();
        try {
            connectors = NameNodeConnector.newNameNodeConnectors(nns, nsids,
                    ECTransitioner.class.getSimpleName(), TRANSITIONER_ID_PATH, conf,
                    NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);

            LOG.info("Size of connectors: " + connectors.size());
            while (connectors.size() > 0) {
                Collections.shuffle(connectors);
                Iterator<NameNodeConnector> iter = connectors.iterator();
                while (iter.hasNext()) {
                    LOG.info("Going next in iteration!");
                    NameNodeConnector nnc = iter.next();
                    ECTransitioner ect = new ECTransitioner(nnc, params, conf);

                    final ExitStatus st = ect.transition();

                    if (st == ExitStatus.SUCCESS) {
                        IOUtils.cleanupWithLogger(LOG, nnc);
                        iter.remove();
                    } else if (st != ExitStatus.IN_PROGRESS) {
                        LOG.error("Unable to progress, exiting.");
                        break;
                    }
                }
                Thread.sleep(sleepTime);
            }
        } catch (Exception e) {
            LOG.error("Error while running transition", e);
        } finally {
            for (NameNodeConnector nnc : connectors) {
                IOUtils.cleanupWithLogger(LOG, nnc);
            }
        }
        return ExitStatus.SUCCESS.getExitCode();
    }

    static class Cli extends Configured implements Tool {
        private static final String USAGE = "Usage: hdfs ec transitioner"
                + "\n\t[-fileName <fileName>]\tthe file to transition."
                + "\n\t[-codec <codec>]\tthe code to use."
                + "\n\t[-numDataUnits <numDataUnits>]\tnew number of data nodes."
                + "\n\t[-numParityUnits <numParityUnits>]\tnew number of parity nodes."
                + "\n\t[-numLocalParityUnits <numLocalParityUnits>]\tnew number of local parity nodes (if applicable).";

        private static Options buildCliOptions() {
            Options opts = new Options();
            Option fileName = OptionBuilder.withArgName("fileName").hasArg()
                    .withDescription("the file to transition")
                    .create("fileName");
            Option codec = OptionBuilder.withArgName("codec").hasArg()
              .withDescription("the code to transition to")
              .create("codec");
            Option numDataUnits = OptionBuilder.withArgName("numDataUnits").hasArg()
                    .withDescription("new number of data nodes")
                    .create("numDataUnits");
            Option numParityUnits = OptionBuilder.withArgName("numParityUnits").hasArg()
                    .withDescription("new number of [aroty] nodes")
                    .create("numParityUnits");
            Option numLocalParityUnits = OptionBuilder.withArgName("numLocalParityUnits").hasArg()
                    .withDescription("new number of local [aroty] nodes")
                    .create("numLocalParityUnits");
            OptionGroup fileGroup = new OptionGroup();
            OptionGroup codecGroup = new OptionGroup();
            OptionGroup dataGroup = new OptionGroup();
            OptionGroup parityGroup = new OptionGroup();
            OptionGroup localParityGroup = new OptionGroup();

            fileGroup.addOption(fileName);
            codecGroup.addOption(codec);
            dataGroup.addOption(numDataUnits);
            parityGroup.addOption(numParityUnits);
            localParityGroup.addOption(numLocalParityUnits);

            opts.addOptionGroup(fileGroup);
            opts.addOptionGroup(codecGroup);
            opts.addOptionGroup(dataGroup);
            opts.addOptionGroup(parityGroup);
            opts.addOptionGroup(localParityGroup);
            return opts;
        }

        /**
         * Parse command line arguments, and then execute transition
         * @param args command specific arguments.
         * @return exit code
         */

        @Override
        public int run(String[] args) {
            final long startTime = Time.monotonicNow();
            final Configuration conf = getConf();

            try {
                // TODO: maybe do some validation here
                final Collection<URI> nns = DFSUtil.getInternalNsRpcUris(conf);
                final Collection<String> nnsIds = DFSUtilClient.getNameServiceIds(conf);
                return ECTransitioner.run(nns, nnsIds, parse(args), conf);
            } catch (Exception e) {
                LOG.error("Exception while executing ec transitioner", e);
                return ExitStatus.IO_EXCEPTION.getExitCode();
            } finally {
                LOG.info("EC Transition took " + (Time.monotonicNow() - startTime)/1000);
            }
        }

        static ECTransitionerParameters parse(String[] args) throws Exception {
            final Options opts = buildCliOptions();
            CommandLineParser parser = new GnuParser();
            CommandLine commandline = parser.parse(opts, args, true);
            return buildTransitionParams(commandline);
        }

        private static ECTransitionerParameters buildTransitionParams(CommandLine line) {
            // print all command line options and values
            LOG.info("[LRC] ECTransitionerParameters Options and values:");
            for (Option opt : line.getOptions()) {
                LOG.info("Option: " + opt.getOpt() + " = " + opt.getValue());
            }
            ECTransitionerParameters.Builder b = new ECTransitionerParameters.Builder();

            if (line.hasOption("fileName")) {
                b.setFileName(line.getOptionValue("fileName"));
            }
            String codec = "XOR";
            int numDataUnits = 0;
            int numParityUnits = 0;
            int numLocalParityUnits = 0;
            if (line.hasOption("codec")) {
                codec = line.getOptionValue("codec");
            }
            if (line.hasOption("numDataUnits")) {
                numDataUnits = Integer.parseInt(line.getOptionValue("numDataUnits"));
            }
            if (line.hasOption("numParityUnits")) {
                numParityUnits = Integer.parseInt(line.getOptionValue("numParityUnits"));
            }
            if (line.hasOption("numLocalParityUnits")) {
                LOG.info("[LRC] ECTransitionParameters does have numLocalParityUnits");
                numLocalParityUnits = Integer.parseInt(line.getOptionValue("numLocalParityUnits"));
            }
            b.setTargetSchema(codec, numDataUnits, numLocalParityUnits, numParityUnits);
            return b.build();
        }

    }

    /**
     * Transition EC schemas
     * @param args
     */
    public static void main(String[] args) {
        if (DFSUtil.parseHelpArgument(args, Cli.USAGE, System.out, true)) {
            System.exit(0);
        }

        try {
            LOG.info("Starting ECTransition flow");
            System.exit(ToolRunner.run(new HdfsConfiguration(), new Cli(), args));
        } catch (Throwable e) {
            LOG.error("Exiting ec transitioner due to an exception", e);
            System.exit(-1);
        }
    }

}
