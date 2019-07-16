package startStorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * @Package: startStorm
 * @Description: TODO
 * @author: Minsky
 * @date: 2019/7/16 19:16
 * @version: v1.0
 */
public class WordsReader implements IRichSpout {

    private TopologyContext context;

    private FileReader fileReader;

    private SpoutOutputCollector collector;

    private boolean completed;

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.context = context;
            this.fileReader = new FileReader(map.get("wordsFile").toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        this.collector = collector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    /**
     * 向Bolts分发数据
     */
    @Override
    public void nextTuple() {
        if(completed){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return ;
        }

        String str;
        BufferedReader reader = new BufferedReader(fileReader);

        // 读取文件中的数据,并将读取的数据发送到 Bolts
        try {
            while((str = reader.readLine())!= null){
                this.collector.emit(new Values(str),str);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            completed = true;
        }
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    /**
     * 定义 spout 的出参, 此出参也就是 Bolt的入参
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
