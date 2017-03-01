package storm.dataclean.localtest;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by yongchao on 7/7/15.
 */
public class LocalBolt extends BaseRichBolt{
        OutputCollector _collector;
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
//            LOG.info(output_prefix + tuple.getValues());
            System.out.println(tuple.getIntegerByField("kid"));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
//            declarer.declareStream("streambolt", new Fields(OUTPUT_FIELD));
        }






}
