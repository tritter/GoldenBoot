/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author thom
 */
public class GoldenBoot {
    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
      
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: GoldenBoot <in> [<in>...] <out>");
      System.exit(2);
    }
    
    //1. Sort tweets if they contain goal or no goal
    //MAP timestamp, tweettext
    //REDUCE minutestamp, tweettext
    
    Job job = new Job(conf, "Goal Sorter");
    job.setJarByClass(TwitterExample.class);
    job.setMapperClass(GoalDefiner.GoalMapper.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    job.waitForCompletion(true);
    
    
    //2. Group all goals individually
    //MAP numberOfTweets, tweetTexts
    //REDUCE 
    
    //3. Check if players are mentioned
    //MAP playername, 1 (if player found)
    //REDUCE playername, mentioncount
  }
}
