package nl.utwente.bigdata;

import java.io.IOException;
import java.util.List;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;

import org.junit.Before;
import org.junit.Test;

 
public class GoalDefinerTest {
 
  private MapDriver<Object, Text, Text, Text> mapDriver;

  @Before
  public void setUp() {
    GoalDefiner.GoalMapper mapper   = new GoalDefiner.GoalMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
  }
  
  @Test
    public void testNoGoal() {
        List<Pair<Text, Text>> out = null;
        try {
            Object key = new Object();
            Text value = new Text("2014070305320140703053	1	Thu Jul 03 05:17:20 +0000 2014	en	Hello world my country did not score!");
            out = mapDriver.withInput(key, value).run();
            assert(out.isEmpty());
        } catch (IOException ioe) {
            //Failed 
            assert(false);
        }
        assert(out != null);
    }
    
    @Test
    public void testGoalWithKeyword() {
        Object key = new Object();
        Text value = new Text("2014070305320140703053	1	Thu Jul 03 05:17:20 +0000 2014	en	Hello world my country scored a Japenese ゴール!");
        mapDriver.withInput(key, value);
        mapDriver.withOutput(new Text("20140703053"), new Text("Hello world my country scored a Japenese ゴール!"));
        mapDriver.runTest();
    }
    
    @Test
    public void testGoalWithScore() {
        Object key = new Object();
        Text value = new Text("2014070305320140703053	1	Thu Jul 03 05:17:20 +0000 2014	en	Hello world my country scored 1-1!");
        mapDriver.withInput(key, value);
        mapDriver.withOutput(new Text("20140703053"), new Text("Hello world my country scored 1-1!"));
        mapDriver.runTest();
    }

}
