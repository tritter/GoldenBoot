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
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.json.simple.parser.JSONParser;

public class GoalDefiner {
  private static SimpleDateFormat dateFormatter(String format){
    TimeZone tz = TimeZone.getTimeZone("GMT");
    SimpleDateFormat sf = new SimpleDateFormat(format, Locale.ENGLISH);
    sf.setTimeZone(tz);
    sf.setLenient(true);
    return sf;
  }
  
  private static final SimpleDateFormat twitterDateFormatter = dateFormatter("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
  private static final SimpleDateFormat minuteDateFormatter = dateFormatter("mm");
  private static final SimpleDateFormat keyDateFormatter = dateFormatter("yyyyMMddHH");
  
  public static Date getTwitterDate(String dateString) throws java.text.ParseException{
    return twitterDateFormatter.parse(dateString);
   }
  
  public static String getMinuteKey(Date date){
    int minuteOfHour = Integer.valueOf(minuteDateFormatter.format(date));
    String minuteKey = String.valueOf(minuteOfHour/5);
    String key = keyDateFormatter.format(date);
    return key.concat(minuteKey);
  }
    
  public static boolean isGoal(Text tweetText){
      String rawText = tweetText.toString().toLowerCase();
      return (rawText.matches("^(.*?(\\b(gol|goal|ประตู|tor|ゴール|doelpunt)\\b)[^$]*)$")) || //Check keywords
             (rawText.matches("(.*)(\\d{1}-\\d{1}|\\d{1} - \\d{1})(.*)")); //Check scores
  }
  
 public static class GoalMapper
    extends Mapper<Object, Text, Text, Text>{
        
        private final Text goalKey = new Text();
        private final Text tweetText = new Text();
        private final JSONParser parser = new JSONParser();
	private Map tweet;    
	
         @Override
         public void map(Object key, Text value, Mapper.Context context
                         ) throws IOException, InterruptedException {
           
            try {
                tweet = (Map<String, Object>) parser.parse(value.toString());
            }
            catch (ClassCastException e) {
                return; // do nothing (we might log this)
            }
            catch (org.json.simple.parser.ParseException e) {
                return; // do nothing
            }
             //Get tweet text
            tweetText.set(((String) tweet.get("text")).replaceAll("\n", " "));
             
             //Check if tweet is a goal if not return
             if(!isGoal(tweetText)) return;
             
             //Get timestamp as key
            String createdAtString = (String)tweet.get("created_at");
             try { 
                 Date createdAt = getTwitterDate(createdAtString);
                 String timeRangeKey = getMinuteKey(createdAt);

                goalKey.set(timeRangeKey);
		context.write(goalKey, tweetText);
            }
            catch (java.text.ParseException e) {  
                return; // do nothing 
            }
	     
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
      
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: GoalDefiner <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "GoalDefiner");
    job.setJarByClass(GoalDefiner.class);
    job.setMapperClass(GoalMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(TextOutputFormat.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
