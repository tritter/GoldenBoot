/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author thom
 */
public class GoalScorerDefinerTest {
    private MapDriver<Object, Text, Text, Text> mapDriver;
    private ReduceDriver<Text,Text,Text,Text> reduceDriver;
    private MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;
    
    @Before
    public void setUp() {
        GoalScorerDefiner.ScoreMapper mapper   = new GoalScorerDefiner.ScoreMapper("res/players.csv");
        GoalScorerDefiner.ScoreReducer reducer = new GoalScorerDefiner.ScoreReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
    
    @Test
    public void testNoPlayer() {
        List<Pair<Text, Text>> out = null;
        try {
            Object key = new Object();
            Text value = new Text("20140703053	Hello world my country did not score!");
            out = mapDriver.withInput(key, value).run();
            assert(out.isEmpty());
        } catch (IOException ioe) {
            //Failed 
            assert(false);
        }
        assert(out != null);
    }
    @Test
    public void testPlayerWithSurename() {
        Object key = new Object();
        Text value = new Text("20140703053	RT @SportsCenter: USA wins World Cup opener vs Ghana, 2-1. Clint Dempsey scores in 1st minute, substitute John Brooks gets game-winner." +
"");
        mapDriver.withInput(key, value);
        mapDriver.withOutput(new Text("20140703053"), new Text("Clint DEMPSEY"));
        mapDriver.runTest();
    }
    
    @Test
    public void testOtherPlayerWithSurename() {
        Object key = new Object();
        Text value = new Text("20140703053	Hello world my Mueller did score!");
        mapDriver.withInput(key, value);
        mapDriver.withOutput(new Text("20140703053"), new Text("Thomas MUELLER"));
        mapDriver.runTest();
    }
        @Test
    public void testPlayerWithSpecialSurename() {
        Object key = new Object();
        Text value = new Text("20140703053	Hello world my Müller did score!");
        mapDriver.withInput(key, value);
        mapDriver.withOutput(new Text("20140703053"), new Text("Thomas MUELLER"));
        mapDriver.runTest();
    }
    
    @Test
    public void testPlayerWithFirstname() {
        Object key = new Object();
        Text value = new Text("20140703053	Hello world my Thomas did score!");
        mapDriver.withInput(key, value);
        mapDriver.withOutput(new Text("20140703053"), new Text("Thomas MUELLER"));
        mapDriver.runTest();
    }
    
//    @Test
//    public void testReducer() {
//        Text key = new Text("20140703053");
//        List<Text> values = new ArrayList<Text>();
//        values.add(new Text("Thomas Mueller"));
//        values.add(new Text("Arjen ROBBEN"));
//        values.add(new Text("Arjen ROBBEN"));
//
//        reduceDriver.withInput(key, values);
//        reduceDriver.withOutput(key, new Text("Arjen ROBBEN"));
//        reduceDriver.runTest();
//    }
//    
//    @Test
//    public void testMapReduce() {
//        Object key = new Object();
//        Text value = new Text("20140703053	Hello world my Müller did score!");
//        mapReduceDriver.withInput(key, value);
//        mapReduceDriver.withOutput(new Text("20140703053"), new Text("Thomas MUELLER"));
//        mapReduceDriver.runTest();
//    }
 
}
