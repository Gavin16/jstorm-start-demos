package startStorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import shade.storm.org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * @Package: startStorm
 * @Description: TODO
 * @author: Minsky
 * @date: 2019/7/16 20:04
 * @version: v1.0
 */
public class WordsNormalizer implements IRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    /**
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String line = tuple.getString(0);
        String[] words = line.split(" ");

        for(String word : words){
            if(StringUtils.isNotEmpty(word)){
                collector.emit(new Values(word.trim().toLowerCase()));
            }
        }
        // 某次数据执行完之后
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    /**
     * 定义输出属性
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
