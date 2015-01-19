package nl.utwente.bigdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

public class GoalPlayerCountTest {
    
    private MapDriver<Object, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver;
    private MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
    
    @Before
    public void setUp() {
        GoalPlayerCount.CountMapper mapper   = new GoalPlayerCount.CountMapper();
        GoalPlayerCount.CountReducer reducer = new GoalPlayerCount.CountReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
    
    @Test
    public void testMapper() {
        Object key = new Object();
        Text value = new Text("20140703053	Arjen Robben");
        mapDriver.withInput(key, value);
        mapDriver.withOutput(new Text("Arjen Robben"), new IntWritable(1));
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
        Text value = new Text("20140703053	Arjen Robben");
        mapReduceDriver.withInput(key, value);
        mapReduceDriver.withOutput(new Text("Arjen Robben"), new IntWritable(1));
        mapReduceDriver.runTest();
    }
    
}
