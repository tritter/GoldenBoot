/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.bigdata;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.simple.parser.JSONParser;

/**
 *
 * @author ime
 */
public class GoalScorerDefiner { 
    
    
    public static List<GoalByPlayer> goalsWithPlayer = new ArrayList<>();
    private static final List<String[]> playerNames = importCSVFile("res/players.csv");
    
    public static class CountMapper extends Mapper<Text, Text, Text, IntWritable> {
        
        public void map(Text key, Text value, Reducer.Context context) throws IOException, InterruptedException {
            String tweet = String.valueOf(value).toLowerCase();
            
            for (String[] playerName : playerNames) {
                String firstName = getFirstName(playerName);
                Text playerFullName = new Text(getFirstName(playerName) + " " + getSurname(playerName));
                
                if (firstName != null && tweet.matches("^(.*?(\\b(" + getFirstName(playerName) + ")\\b)[^$]*)$")) {
                    context.write(key, playerFullName);
                } else if (tweet.matches("^(.*?(\\b(" + getSurname(playerName) + ")\\b)[^$]*)$")) {
                    context.write(key, playerFullName);
                }
            }
        }
    }
    
    public static class CountReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {  
        
        // Create hashmap with key (key) and value (number of goals)
        // Loop through tweets and get player name
        
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values,
                           Reducer.Context context
                           ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: userCount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Twitter UserCounter");
        job.setJarByClass(GoalScorerDefiner.class);
        job.setMapperClass(GoalScorerDefiner.CountMapper.class);
        job.setCombinerClass(GoalScorerDefiner.CountReducer.class);
        job.setReducerClass(GoalScorerDefiner.CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                                       new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

     
    public static List<String[]> importCSVFile(String csvFile) {
        List<String[]> playerNames = new ArrayList<>();
        BufferedReader br = null;
        
        String line;

        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                String[] csvLine = line.split(";");
                String fullPlayerName = csvLine[0];
                playerNames.add(fullPlayerName.split(" "));
            }
        } 
        catch (FileNotFoundException e) {} catch (IOException e) {} 
        finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {}
            }
        }
        return playerNames;
    }
    
    public static String getFirstName(String[] playerNames) {
        if(playerNames.length <= 1) return null; // Has no firstname
        return playerNames[0].toLowerCase(); 
    }
    
    public static String getSurname(String[] playerNames) {
        if(playerNames.length < 1) return null; // Name not correct
        return playerNames[playerNames.length - 1].toLowerCase();
    }
    
    
}
