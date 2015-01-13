import java.util.ArrayList;
import java.util.List;
import nl.utwente.bigdata.GoalSort;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

 
public class GoalSortTest {
 
  private MapDriver<Object, Text, LongWritable, Text> mapDriver;
  private ReduceDriver<LongWritable, Text, LongWritable, ArrayWritable> reduceDriver;
  private MapReduceDriver<Object, Text, LongWritable, Text, LongWritable, ArrayWritable> mapReduceDriver; 
 
  @Before
  public void setUp() {
    GoalSort.TimestampMapper mapper   = new GoalSort.TimestampMapper();
    GoalSort.GoalReducer reducer = new GoalSort.GoalReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 

  @Test
  public void testMapper() {
    Object key = new Object();
    Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"Hello world!\",\"id_str\":\"1\", \"created_at\":\"Thu Jul 03 05:17:20 +0000 2014\"}");
    mapDriver.withInput(key, value);
    mapDriver.withOutput(new LongWritable(1404364640), new Text("Hello world!"));
    mapDriver.runTest();
  }
 

//  @Test
//  public void testReducer() {
//    List<Text> values = new ArrayList<Text>();
//    values.add(new Text("A simple tweet"));
//    values.add(new Text("Hello world!"));
//    String[] texts = {"A simple tweet", "Hello world!"};
//    reduceDriver.withInput(new LongWritable(1404364640), values);
//    reduceDriver.withOutput(new LongWritable(1404364640), new ArrayWritable(texts));
//    reduceDriver.runTest();
//  }


//  @Test
//  public void testMapReduce() {
//    Object key = new Object();
//    Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"A nice tweet indeed\",\"id_str\":\"1\"}");
//    mapReduceDriver.withInput(key, value);
//    String[] texts = {"A nice tweet indeed"};
//    mapReduceDriver.withOutput(new LongWritable(1404364640), new ArrayWritable(texts));
//    mapReduceDriver.runTest();
//  }

}
