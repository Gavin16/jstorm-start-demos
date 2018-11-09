package jstorm.wordCount.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import jstorm.wordCount.bolt.SplitSentenceBolt;
import jstorm.wordCount.bolt.WordCountBolt;
import jstorm.wordCount.spout.SentenceSpout;

public class WordCountTopology {

    public static void main(String[] args){

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sentence-spout", new SentenceSpout(), 1);
        builder.setBolt("split-bolt", new SplitSentenceBolt(), 2).localOrShuffleGrouping("sentence-spout");
        builder.setBolt("count-bolt", new WordCountBolt()).fieldsGrouping("split-bolt", new Fields("word"));
        //在项目根目录下存在这两个文件
        String inputFile = "input.txt";
        String outputFile = "output.txt";
        Config conf = new Config();
        conf.put("inputFile", inputFile);
        conf.put("outputFile", outputFile);
        conf.setDebug(true);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            try{
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }
            catch (InvalidTopologyException e){
                e.printStackTrace();
            }
            catch (AlreadyAliveException e){
                e.printStackTrace();
            }
        } else {
            String topName = "word-count-topology";
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topName, conf, builder.createTopology());
            Utils.sleep(10000);
            // cluster.killTopology(topName);
            cluster.shutdown();
        }
    }
}
