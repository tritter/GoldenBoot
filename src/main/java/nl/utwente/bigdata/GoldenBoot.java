/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.bigdata;

import java.util.ArrayList;
import java.util.List;
import nl.utwente.bigdata.GoalDefiner.GoalMapper;
import nl.utwente.bigdata.GoalPlayerCount.CountMapper;
import nl.utwente.bigdata.GoalPlayerCount.CountReducer;
import nl.utwente.bigdata.GoalScorerDefiner.ScoreMapper;
import nl.utwente.bigdata.GoalScorerDefiner.ScoreReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author thom
 */
public class GoldenBoot {
    public static void main(String[] args) throws Exception {
    List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            other_args.add(args[i]);
        }
    Configuration conf = new Configuration();
    
    //First Job - GoalDefiner, the tweet that are containing goals within 5 minute blocks 
    Job job1 = new Job(conf, "GoalDefiner");
    job1.setJarByClass(GoalDefiner.class);
    job1.setMapperClass(GoalMapper.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.setInputPaths(job1, new Path(other_args.get(0)));
    FileOutputFormat.setOutputPath(job1, new Path("goal_definer_output"));

    job1.waitForCompletion(true);

    //Second Job - GoalScorerDefiner, Counts players within the goal tweets who scored probably the goal
    Job job2= new Job(conf, "GoalScorerDefiner");
    job2.setJarByClass(GoalScorerDefiner.class);
    job2.setMapperClass(ScoreMapper.class);
    job2.setCombinerClass(ScoreReducer.class);
    job2.setReducerClass(ScoreReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.setInputPaths(job2, new Path("goal_definer_output/part*"));
    FileOutputFormat.setOutputPath(job2, new Path("goal_scorer_definer_output"));

    job2.waitForCompletion(true);

    //Third Job - GoalPlayerCount, counts all goals of a player
    Job job3 = new Job(conf, "GoalPlayerCount");
    job3.setJarByClass(GoalPlayerCount.class);
    job3.setMapperClass(CountMapper.class);
    job3.setCombinerClass(CountReducer.class);
    job3.setReducerClass(CountReducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(IntWritable.class);
    job3.setInputFormatClass(TextInputFormat.class);
    job3.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.setInputPaths(job3, new Path("goal_scorer_definer_output/*"));
    FileOutputFormat.setOutputPath(job3, new Path("golden_boot_output"));

    job3.waitForCompletion(true);

    
  }
}
