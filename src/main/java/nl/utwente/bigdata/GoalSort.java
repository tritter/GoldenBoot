/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.utwente.bigdata;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class GoalSort {

  public static Date getTwitterDate(String date) throws java.text.ParseException{
    final String TWITTER =  "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
    SimpleDateFormat sf = new SimpleDateFormat(TWITTER, Locale.ENGLISH);
    sf.setLenient(true);
    return sf.parse(date);
   }
    
  public static class TimestampMapper 
       extends Mapper<Object, Text, LongWritable, Text>{
    
    private LongWritable tweetTimestamp  = new LongWritable();
    private Text tweetText = new Text();
    private JSONParser parser = new JSONParser();
    private Map tweet;
      
    public void map(Object key, Text value, Reducer.Context context
                    ) throws IOException, InterruptedException, ParseException, java.text.ParseException {

      try {
        tweet = (Map<String, Object>) parser.parse(value.toString());
      }
      catch (ClassCastException e) {  
        return; // do nothing (we might log this)
      }
      
      //Get key
      String createdAtString = (String)tweet.get("created_at");
      try {
        Date createdAt = getTwitterDate(createdAtString);
        long timestamp =createdAt.getTime();
        tweetTimestamp.set(timestamp);
      }
      catch (java.text.ParseException e) {  
		System.out.println("Couldnt parse");
        return; // do nothing (we might log this)
      }
      
      //Get text
      tweetText.set(((String)tweet.get("text")).replaceAll("\n", " "));
      context.write(tweetTimestamp, tweetText);
    } 
  }
  
  public static class GoalReducer 
       extends Reducer<LongWritable, Text, LongWritable, ArrayWritable> {

    public void reduce(Text key, Iterable<Text> values, 
                       Reducer.Context context
                       ) throws IOException, InterruptedException {
      for (Text value : values) {
        context.write(key, value);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
      
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: GoalSort <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Goal sorter");
    job.setJarByClass(TwitterExample.class);
    job.setMapperClass(TimestampMapper.class);
    job.setReducerClass(GoalReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
