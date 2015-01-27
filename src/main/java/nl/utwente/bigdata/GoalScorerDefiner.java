/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.bigdata;
import java.net.URI;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

/**
 *
 * @author ime
 */
public class GoalScorerDefiner { 
    
    private static String normalizeCharacters(String text){
        text = text.replace("ü", "ue");
        text = text.replace("ö", "oe");
        text = text.replace("ā", "ae");
        text = text.replace("ë", "e");
        text = text.replace("á", "a");
        text = text.replace("é", "e");
        text = text.replace("ç", "c");
        text = text.replace("à", "a");
        text = text.replace("è", "e");
        text = text.replace("ù", "u");

        return text;
    }
    
    private static boolean containsPlayerName(String name, String text){
        String normalizedString = normalizeCharacters(text);
        return (name != null && normalizedString.matches("^(.*?(\\b(" + name + ")\\b)[^$]*)$"));
    }
    
    public static class ScoreMapper extends Mapper<Object, Text, Text, Text> {
        private List<String[]> playerNames = new ArrayList<>();

	public ScoreMapper (){
	    this.playerNames = importCSVFile("player.csv");
	};
	public ScoreMapper (String path){
	    this.playerNames = importCSVFile(path);
	}
	
        private final Text goalId = new Text();
	private final Text player = new Text();
	
        @Override
        public void map(Object key, Text value, Mapper.Context context)
                        throws IOException, InterruptedException {
	    
            String[] split = value.toString().split("\t");
            if(split.length <= 1) return;

            goalId.set(split[0]);            
            boolean playerFound = false;
            String tweet = String.valueOf(split[1]).toLowerCase();
            for (String[] playerName : playerNames) { //Surename
                playerFound = containsPlayerName(getSurname(playerName), tweet);
                if(playerFound){
                    player.set(StringUtils.join(" ", playerName));
                    break;
                }
            }

            if(!playerFound){
                for (String[] playerName : playerNames) {
                    playerFound = containsPlayerName(getFirstName(playerName), tweet);
                    if(playerFound){
                        player.set(StringUtils.join(" ", playerName));
                        break;
                    }
                }
            }
       
            if(playerFound){
                context.write(goalId, player);
            }
        }
    }
    
    public static class ScoreReducer extends Reducer<Text,Text,Text,Text> {  

        private Text playerResult = new Text();
        
        public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
            HashMap<Text, Integer> playerCountMap = new HashMap<>();
        
            for (Text val : values) {  
                if(playerCountMap.get(val) == null) {
                    playerCountMap.put(val, 1);
                } else {
                    playerCountMap.put(val, playerCountMap.get(val) + 1);
                }
            }
            
            int maxValueInMap = (Collections.max(playerCountMap.values()));
            
            for (Entry<Text, Integer> entry : playerCountMap.entrySet()) {  
                if (entry.getValue()==maxValueInMap) {
                    playerResult.set(entry.getKey());
                }
            }
            if(maxValueInMap < 300){
                playerResult.set("NO REAL GOAL");
            }
            context.write(key, playerResult);
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: userCount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "GoalScorerDefiner");
        job.setJarByClass(GoalScorerDefiner.class);
        job.setMapperClass(GoalScorerDefiner.ScoreMapper.class);
        job.setCombinerClass(GoalScorerDefiner.ScoreReducer.class);
        job.setReducerClass(GoalScorerDefiner.ScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                                       new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

     
    public static List<String[]> importCSVFile(String csvFile){
	List<String[]> playerNames = new ArrayList<>();
	try {
	    Path file_path=new Path(csvFile);
            FileSystem filesystem = FileSystem.get(URI.create("files/player.csv"),new Configuration());
	    BufferedReader br = null;
	    String line;
	    br = new BufferedReader(new InputStreamReader(filesystem.open(file_path)));
	    while ((line = br.readLine()) != null) {
		String[] csvLine = line.split(";");
		String fullPlayerName = csvLine[0];
		playerNames.add(fullPlayerName.split(" "));
	    }
	    br.close();
	} catch (FileNotFoundException ex) {
	    Logger.getLogger(GoalScorerDefiner.class.getName()).log(Level.SEVERE, null, ex);
	} catch (IOException ex) {
	    Logger.getLogger(GoalScorerDefiner.class.getName()).log(Level.SEVERE, null, ex);
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
