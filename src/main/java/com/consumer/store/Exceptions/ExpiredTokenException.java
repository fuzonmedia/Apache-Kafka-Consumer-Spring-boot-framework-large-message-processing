package com.consumer.store.Exceptions;

public class ExpiredTokenException extends Exception{

    public ExpiredTokenException(String message){

         super( message );
    }
    
}
