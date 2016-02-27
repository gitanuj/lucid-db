package com.lucid.spanner;

/**
 * @author Aviral
 */
public class UnexpectedCommand extends Exception{
    UnexpectedCommand(String message){
        super(message);
    }
}
