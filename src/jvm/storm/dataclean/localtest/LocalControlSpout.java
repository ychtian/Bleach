package storm.dataclean.localtest;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dataclean.auxiliary.GlobalConstant;
import storm.dataclean.util.BleachConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tian on 15/03/2016.
 */
public class LocalControlSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(LocalControlSpout.class);
    private String fileName;
    private SpoutOutputCollector _collector;
    private Scanner sc;
    private AtomicInteger linesRead;


    public List<String> attributes;

//    public int counter;
    public String del;
    public String DETECT_STREAM_NAME;
    public String REPAIR_STREAM_NAME;

    public String[] buffered_values;

    public LocalControlSpout(BleachConfig config){
        String schemestring = config.get(BleachConfig.SCHEMA);
        attributes = Arrays.asList(schemestring.split(","));
        del = config.get(BleachConfig.KAFKA_DATA_DELIMITER);
        DETECT_STREAM_NAME = GlobalConstant.CONTROL_STREAM;
        REPAIR_STREAM_NAME = GlobalConstant.REPAIR_CONTROL_STREAM;
    }


    /**
     * Prepare the spout. This method is called once when the topology is submitted
     * @param conf
     * @param context
     * @param collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        linesRead = new AtomicInteger(0);
        try {
            sc=new Scanner(System.in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

//    @Override
//    public void deactivate() {
//
//            sc.close();
//
//    }

    /**
     * Storm will call this method repeatedly to pull tuples from the spout
     */
    @Override
    public void nextTuple() {
        try {
            String line = sc.nextLine();
            if (line != null) {
//                int id = linesRead.incrementAndGet();
                int id = Integer.MAX_VALUE;
                String[] values = line.split(del, -1);
//                _collector.emit(DETECT_STREAM_NAME,new Values(values), "detect_control"+id);
//                _collector.emit(DETECT_STREAM_NAME,new Values(values), id);
                _collector.emit(DETECT_STREAM_NAME,new Values(values[0], values[1]), id);
//                buffered_values = values;

//                Thread.sleep(1000);
//                _collector.emit(REPAIR_STREAM_NAME, new Values(values), "control_repair" + linesRead.get());
//                _collector.emit(REPAIR_STREAM_NAME, new Values(values), id+10);
//                buffered_values = null;
//                Thread.sleep(1000);
//                _collector.emit(REPAIR_STREAM_NAME, new Values(values), "control_repair_back" + linesRead.get());
            } else {
                System.out.println("wrong...");
//                Thread.sleep(10000);
//                LocalGlobal.finish = 1;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Storm will call this method when tuples are acked
     * @param id
     */
    @Override
    public void ack(Object id) {
        System.err.println("+++++++++++++++spout acked: "+id);

//        if(buffered_values != null)
//        {
//            _collector.emit(DETECT_STREAM_NAME, new Values(buffered_values), "control_repair" + linesRead.get());
//            buffered_values = null;
//        }

//        1.HERE TO SEND MSG TO REPAIR....
//        2. not here, try to print number of sgs (not vc) left after rule delete.

    }

    /**
     * Storm will call this method when tuples fail to process downstream
     * @param id
     */
    @Override
    public void fail(Object id) {
        System.err.println("--------------------Failed line number " + id);
    }

    /**
     * Tell storm which fields are emitted by the spout
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // read csv header to get field info
        ArrayList<String> newattrs = new ArrayList<>();
        newattrs.add("control-msg-id");
        newattrs.add("control-msg");
//        declarer.declare(new Fields(newattrs));
        declarer.declareStream(DETECT_STREAM_NAME, new Fields(newattrs));
        declarer.declareStream(REPAIR_STREAM_NAME, new Fields(newattrs));

    }

}

