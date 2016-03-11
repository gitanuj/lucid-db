package com.lucid.common;

/**
 * @author Aviral
 */
public class UnexpectedCommand extends Exception{
    public UnexpectedCommand(String message){
        super(message);
    }
}
