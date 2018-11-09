package jstorm.wordCount.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * spout 从inputFile 中读取文本
 */
public class SentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private BufferedReader bufferedReader;
    private boolean completed = false;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        try {
            String inputFile = map.get("inputFile").toString();
            fileReader = new FileReader(inputFile);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    public void nextTuple() {
        if(completed){
            return ;
        }

        bufferedReader = new BufferedReader(fileReader);

        try {
            String setence;
            while(null != (setence = bufferedReader.readLine())){
                this.collector.emit(new Values(setence));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            completed = true;
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
