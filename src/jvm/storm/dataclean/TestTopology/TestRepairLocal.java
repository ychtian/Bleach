package storm.dataclean.TestTopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import storm.dataclean.auxiliary.GlobalConstant;
import storm.dataclean.auxiliary.base.*;
import storm.dataclean.auxiliary.detect.DataTuple;
import storm.dataclean.auxiliary.repair.RepairProposal;
import storm.dataclean.auxiliary.repair.coordinator.MergeEQClassProposal;
import storm.dataclean.auxiliary.repair.coordinator.MergeEQClassProposalGroup;
import storm.dataclean.component.bolt.detect.*;
import storm.dataclean.component.bolt.repair.*;
import storm.dataclean.localtest.LocalControlSpout;
import storm.dataclean.localtest.LocalGlobal;
import storm.dataclean.localtest.LocalReadSpout;
import storm.dataclean.localtest.StartEndAnchorLocal;
import storm.dataclean.util.BleachConfig;

import java.util.ArrayList;

/**
 * Created by yongchao on 11/16/15.
 */
public class TestRepairLocal {

    public static int repair_way;


//    public static String input;
//    public static String output;

    public static BleachConfig genConfig(String[] args) {
        String configfile = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-config")) {
                configfile = args[++i];
            }
//            if (args[i].equals("-repair")) {
//                repair_way = Integer.parseInt(args[++i]);
//            }
//            if (args[i].equals("-input")) {
//                input = args[++i];
//            }
//            if (args[i].equals("-output")) {
//                output = args[++i];
//            }

        }

        if (configfile.equals("")) {
            System.out.println("Usage: -config <configfile>");
            System.exit(0);
        }
//        if(repair_way <= 0 || repair_way > 3){
//            System.out.println("There are only 3 repair methods, missing -repair x");
//            System.exit(0);
//        }

        BleachConfig config = BleachConfig.parse(configfile);
//        System.exit(0);
        return config;
    }

    public static void registerSerialization(Config conf){
        conf.registerSerialization(Violation.class);
        conf.registerSerialization(Violation.NullViolation.class);
        conf.registerSerialization(ViolationCause.class);
        conf.registerSerialization(RepairProposal.class);
        conf.registerSerialization(ViolationGroup.class);
        conf.registerSerialization(MergeEQClassProposalGroup.class);
        conf.registerSerialization(MergeEQClassProposal.class);
        conf.registerSerialization(BasicSuperCell.class);
        conf.registerSerialization(WinSuperCell.class);
        conf.registerSerialization(ComWinSuperCell.class);
        conf.registerSerialization(CircularFifoQueue.class);
        conf.registerSerialization(DataTuple.class);
        conf.registerSerialization(ArrayList.class);
    }


    public static void main(String[] args) throws Exception {

        BleachConfig bconfig = genConfig(args);
        TopologyBuilder builder = new TopologyBuilder();

        RepairWorkerBolt rw;
        repair_way = bconfig.getRW();

        if(repair_way == 1){
            rw = new BasicRepairWorkerBolt(bconfig);
        } else if(repair_way == 2){
            rw = new BleachIRRepairWorkerBolt(bconfig);
        } else {
            rw = new BleachDRRepairWorkerBolt(bconfig);
        }




//        builder.setSpout(bconfig.get(BleachConfig.SPOUT_DATA_ID), KafkaSpoutBuilder.buildKafkaDataSpout(bconfig), 1);
        builder.setSpout(bconfig.get(BleachConfig.SPOUT_DATA_ID), new LocalReadSpout(bconfig, bconfig.getInput()), 1);
//        builder.setBolt("test", new LocalBolt(),1).shuffleGrouping(bconfig.get(BleachConfig.SPOUT_DATA_ID));
//        builder.setBolt(bconfig.get(BleachConfig.BOLT_DETECT_INGRESS_REPAIR_EGRESS_ID), new StartEndBolt(bconfig),
//                Integer.parseInt(bconfig.get(BleachConfig.BOLT_DETECT_INGRESS_REPAIR_EGRESS_NUM))).
//                fieldsGrouping(bconfig.get(BleachConfig.SPOUT_DATA_ID), new Fields("kid"));

        builder.setSpout(bconfig.get(BleachConfig.SPOUT_CONTROL_ID), new LocalControlSpout(bconfig),1);






        builder.setBolt(bconfig.get(BleachConfig.BOLT_DETECT_INGRESS_REPAIR_EGRESS_ID), new StartEndAnchorLocal(bconfig, bconfig.getOutput()),
                Integer.parseInt(bconfig.get(BleachConfig.BOLT_DETECT_INGRESS_REPAIR_EGRESS_NUM))).
                fieldsGrouping(bconfig.get(BleachConfig.SPOUT_DATA_ID), new Fields("kid")).
                fieldsGrouping(bconfig.get(BleachConfig.BOLT_REPAIR_AGGREGATOR_ID), GlobalConstant.REPAIR_DATA_STREAM, new Fields("kid")).
                fieldsGrouping(bconfig.get(BleachConfig.BOLT_DETECT_EGRESS_ID), GlobalConstant.REPAIR_CLEAN_NOTIFICATION_STREAM, new Fields("kid"));
        builder.setBolt(bconfig.get(BleachConfig.BOLT_DETECT_INGRESS_ID), new DetectIngressRouterBolt(bconfig), 1).
                shuffleGrouping(bconfig.get(BleachConfig.BOLT_DETECT_INGRESS_REPAIR_EGRESS_ID), GlobalConstant.DETECT_DATA_STREAM).
                allGrouping(bconfig.get(BleachConfig.SPOUT_CONTROL_ID), GlobalConstant.CONTROL_STREAM);
        builder.setBolt(bconfig.get(BleachConfig.BOLT_DETECT_WORKER_ID), new DetectWorkerBolt(bconfig), Integer.parseInt(bconfig.get(BleachConfig.BOLT_DETECT_WORKER_NUM)))
                .directGrouping(bconfig.get(BleachConfig.BOLT_DETECT_INGRESS_ID), GlobalConstant.DETECT_DATA_STREAM).
                allGrouping(bconfig.get(BleachConfig.SPOUT_CONTROL_ID), GlobalConstant.CONTROL_STREAM);
        builder.setBolt(bconfig.get(BleachConfig.BOLT_DETECT_EGRESS_ID), new DetectEgressRouterBolt(bconfig), 1).
                shuffleGrouping(bconfig.get(BleachConfig.BOLT_DETECT_WORKER_ID), GlobalConstant.DETECT_DATA_STREAM).
                allGrouping(bconfig.get(BleachConfig.SPOUT_CONTROL_ID), GlobalConstant.CONTROL_STREAM);
        builder.setBolt(bconfig.get(BleachConfig.BOLT_REPAIR_WORKER_ID), rw,
                Integer.parseInt(bconfig.get(BleachConfig.BOLT_REPAIR_WORKER_NUM))).
                allGrouping(bconfig.get(BleachConfig.BOLT_DETECT_EGRESS_ID), GlobalConstant.REPAIR_DATA_STREAM).
                allGrouping(bconfig.get(BleachConfig.BOLT_REPAIR_COORDINATOR_ID), GlobalConstant.REPAIR_FROM_COORDINATE_STREAM).
                allGrouping(bconfig.get(BleachConfig.SPOUT_CONTROL_ID), GlobalConstant.REPAIR_CONTROL_STREAM);
        builder.setBolt(bconfig.get(BleachConfig.BOLT_REPAIR_AGGREGATOR_ID), new RepairAggregatorBolt(bconfig), Integer.parseInt(bconfig.get(BleachConfig.BOLT_REPAIR_AGGREGATOR_NUM))).
                fieldsGrouping(bconfig.get(BleachConfig.BOLT_REPAIR_WORKER_ID), GlobalConstant.REPAIR_DATA_STREAM, new Fields("kid"));
//                allGrouping(bconfig.get(BleachConfig.SPOUT_CONTROL_ID), GlobalConstant.CONTROL_STREAM);

        builder.setBolt(bconfig.get(BleachConfig.BOLT_REPAIR_COORDINATOR_ID), new RepairCoordinatorBolt(bconfig), 1).
                shuffleGrouping(bconfig.get(BleachConfig.BOLT_REPAIR_WORKER_ID), GlobalConstant.REPAIR_TO_COORDINATE_STREAM);

        Config conf = new Config();
//        conf.setMaxTaskParallelism(3);
        registerSerialization(conf);
//        conf.setNumWorkers(Integer.parseInt(bconfig.get(BleachConfig.NUMPROCESS)));
//        if(bconfig.getNumAcker() != 0)
//        {
        conf.setNumAckers(bconfig.getNumAcker());
//        }
//        conf.setFallBackOnJavaSerialization(false);
        if (bconfig.getMaxspoutpending() > 0) {
            conf.setMaxSpoutPending(bconfig.getMaxspoutpending());
        }

        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());

        while(true){
            Thread.sleep(10000);
            if(LocalGlobal.finish == 1){
                break;
            }
        }

        cluster.shutdown();

//        StormSubmitter.submitTopologyWithProgressBar(bconfig.getTopologyname(), conf, builder.createTopology());
//        TopologyMetrics tm = new TopologyMetrics();

    }

}
