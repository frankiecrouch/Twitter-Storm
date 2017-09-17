package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;


/**
 * A bolt that counts the words that it receives
 */
public class CountBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;

  // Map to store the count of the words
  private Map<String, Integer> countMap;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {

    // save the collector for emitting tuples
    collector = outputCollector;

    // create and initialize the map
    countMap = new HashMap<String, Integer>();
  }

  @Override
  public void execute(Tuple tuple)
  {
    // get category from the Category Bolt
    String category = tuple.getStringByField("category");

    // check if the category is present in the map
    if (countMap.get(category) == null) {

      // not present, add the category with a count of 1
      countMap.put(category, 1);
    } else {

      // already there, hence get the count
      Integer val = countMap.get(category);

      // increment the count and save it to the map
      countMap.put(category, ++val);
    }

    // emit the word and count
    collector.emit(new Values(category, countMap.get(category)));
  }

  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a two columns called 'category' and 'count'    
    outputFieldsDeclarer.declare(new Fields("category","count"));
  }
}
