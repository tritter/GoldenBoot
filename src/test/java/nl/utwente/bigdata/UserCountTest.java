import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

import nl.utwente.bigdata.UserCount;

public class UserCountTest {
    
    private MapDriver<Object, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver;
    private MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
    
    @Before
    public void setUp() {
        UserCount.CountMapper mapper   = new UserCount.CountMapper();
        UserCount.CountReducer reducer = new UserCount.CountReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
    
    
    @Test
    public void testMapper() {
        Object key = new Object();
        Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"Hello world!\",\"id_str\":\"1\", \"user\":{\"id_str\":\"1\"}}");
        mapDriver.withInput(key, value);
        mapDriver.withOutput(new Text("1"), new IntWritable(1));
        mapDriver.runTest();
    }
    
    
    @Test
    public void testReducer() {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("1"), values);
        reduceDriver.withOutput(new Text("1"), new IntWritable(2));
        reduceDriver.runTest();
    }
    
    
    @Test
    public void testMapReduce() {
        Object key = new Object();
        Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"Hello world!\",\"id_str\":\"1\", \"user\":{\"id_str\":\"1\"}}");
        mapReduceDriver.withInput(key, value);
        mapReduceDriver.withOutput(new Text("1"), new IntWritable(1));
        mapReduceDriver.runTest();
    }
    
}
