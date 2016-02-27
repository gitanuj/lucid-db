package com.lucid.spanner;

/**
 * @author Aviral
 */
public class UnexpectedCommand extends Exception{
    public UnexpectedCommand(String message){
        super(message);
    }
}
