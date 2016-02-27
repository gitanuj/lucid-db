package com.lucid.spanner;

public class LeaderNotFound extends Exception{
    public LeaderNotFound(String message){
        super(message);
    }
}
