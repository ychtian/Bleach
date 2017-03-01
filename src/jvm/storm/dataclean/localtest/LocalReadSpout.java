package storm.dataclean.localtest;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dataclean.util.BleachConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tian on 15/03/2016.
 */
public class LocalReadSpout  extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(LocalReadSpout.class);
    private String fileName;
    private SpoutOutputCollector _collector;
    private BufferedReader reader;
    private AtomicInteger linesRead;


    public List<String> attributes;

//    public int counter;
    public String del;

    public LocalReadSpout(BleachConfig config, String input){
        String schemestring = config.get(BleachConfig.SCHEMA);
        attributes = Arrays.asList(schemestring.split(","));
        del = config.get(BleachConfig.KAFKA_DATA_DELIMITER);
//        fileName = config.get(BleachConfig.LOCAL_INPUTFILE);
        fileName = input;
    }


    /**
     * Prepare the spout. This method is called once when the topology is submitted
     * @param conf
     * @param context
     * @param collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        linesRead = new AtomicInteger(0);
        _collector = collector;
        try {
//            fileName= (String) conf.get("data_witherror_tiny");
//            fileName = "data_witherror_null_0.1_shuf_tmp";
            reader = new BufferedReader(new FileReader(fileName));
            // read and ignore the header if one exists
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deactivate() {
        try {
            reader.close();
        } catch (IOException e) {
            LOG.warn("Problem closing file");
        }
    }

    /**
     * Storm will call this method repeatedly to pull tuples from the spout
     */
    @Override
    public void nextTuple() {
        try {
            String line = reader.readLine();
//            System.out.println("spout send:" + line);
            if (line != null) {
                int id = linesRead.incrementAndGet();
                String[] values = line.split(del, -1);
                Object[] objects = new Object[values.length];
                objects[0] = Integer.parseInt(values[0]);
                for(int i = 1; i < objects.length; i++){
                    objects[i] = values[i];
                }
                _collector.emit(new Values(objects), id);
            } else {
                System.out.println("Finished reading file, " + linesRead.get() + " lines read");
                Thread.sleep(10000);
                LocalGlobal.finish = 1;
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
//        System.err.println("acked: "+id);
    }

    /**
     * Storm will call this method when tuples fail to process downstream
     * @param id
     */
    @Override
    public void fail(Object id) {
        System.err.println("Failed line number " + id);
    }

    /**
     * Tell storm which fields are emitted by the spout
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // read csv header to get field info
        ArrayList<String> newattrs = new ArrayList<>();
//        newattrs.add("kid");
        newattrs.addAll(attributes);
        declarer.declare(new Fields(newattrs));
    }

}

