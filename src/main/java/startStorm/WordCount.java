package startStorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @Package: startStorm
 * @Description: TODO
 * @author: Minsky
 * @date: 2019/7/16 20:24
 * @version: v1.0
 */
public class WordCount implements IRichBolt {

    private OutputCollector collector;

    private Map<String,Integer> map = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        int count;
        if(!map.containsKey(word)){
            map.put(word,1);
        }else{
            count = map.get(word);
            map.put(word,++count);
        }

        collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
