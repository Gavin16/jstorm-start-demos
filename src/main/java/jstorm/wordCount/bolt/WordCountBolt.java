package jstorm.wordCount.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * 统计单词个数
 */
public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String outputFile;
    private Map<String,Object> counts;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        outputFile = map.get("outputResult").toString();
        counts = new HashMap<String,Object>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Integer count = (Integer) this.counts.get(word);
        if(count == null){
            count = 0;
        }
        count++;
        this.counts.put(word, count);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void cleanUp(){
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(outputFile);
            for(String key : keys){
                fileWriter.write(key + " "+this.counts.get(key)+"/n");
            }
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try{
                if(null != fileWriter) {
                    fileWriter.close();
                }
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
