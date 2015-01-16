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

/**
 *
 * @author ime
 */
public class PlayerGoalCounter { 
    
    public static List<String[]> playerNames = new ArrayList<>();
    public static List<GoalByPlayer> goalsWithPlayer = new ArrayList<>();
    
    public static void main(String[] args) {
        PlayerGoalCounter obj = new PlayerGoalCounter();
        obj.importCSVFile("res/players.csv");

        GoalTweet[] inputArray = { 
            new GoalTweet(12345, "dit is een test tweet voor Arjen"),
            new GoalTweet(23456, "tweede tweet"),
            new GoalTweet(344, "wat een goal van Robin van persie!"),
            new GoalTweet(344, "goetze fout"),
            new GoalTweet(344, "goal robin van persie!")
        };      
        
        obj.findPlayerNameInTweets(inputArray);
        
        for (GoalByPlayer goalsWithPlayer1 : goalsWithPlayer) {
            System.out.println(goalsWithPlayer1.goal + ": " + goalsWithPlayer1.player);
        }
    }
     
    public void importCSVFile(String csvFile) {
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
    }
    
    // Find the firstname and/or surname in the tweet, by looping throug all tweets and players
    // If player is mentioned, add the goal and playername to the list goalsWithPlayer
    public void findPlayerNameInTweets(GoalTweet[] goalTweets) {
        for (GoalTweet goalTweet : goalTweets) {
            String tweet = goalTweet.tweet.toLowerCase();
            for (String[] playerName : playerNames) {
                String firstName = getFirstName(playerName);
                
                if (firstName != null && tweet.matches("^(.*?(\\b(" + getFirstName(playerName) + ")\\b)[^$]*)$")) {
                    goalsWithPlayer.add(new GoalByPlayer(goalTweet.goal, getFirstName(playerName) + " " + getSurname(playerName)));
                } else if (tweet.matches("^(.*?(\\b(" + getSurname(playerName) + ")\\b)[^$]*)$")) {
                    goalsWithPlayer.add(new GoalByPlayer(goalTweet.goal, getFirstName(playerName) + " " + getSurname(playerName)));
                }
            }
        }
    }
    
    public String getFirstName(String[] playerNames) {
        if(playerNames.length <= 1) return null; // Has no firstname
        return playerNames[0].toLowerCase(); 
    }
    
    public String getSurname(String[] playerNames) {
        if(playerNames.length < 1) return null; // Name not correct
        return playerNames[playerNames.length - 1].toLowerCase();
    }
    
    
}
