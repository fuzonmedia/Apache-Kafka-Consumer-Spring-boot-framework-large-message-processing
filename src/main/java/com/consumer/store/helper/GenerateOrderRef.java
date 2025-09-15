package com.consumer.store.helper;
import java.math.BigInteger;

public class GenerateOrderRef {

    public static String getRef(String previous_ref,String prefix){

        Integer default_limit=12;
        if ( previous_ref.replace(prefix,"") == previous_ref ){
            return getInitialRef(prefix);
        }else{
            return prefix+addLeadingZero((new BigInteger(previous_ref.replace(prefix,""))).add(new BigInteger("1")), default_limit);
        }
    }

    public static String getInitialRef(String prefix){

        Integer default_limit=12;
        return prefix+addLeadingZero(new BigInteger("1"), default_limit);

    }

    static String addLeadingZero(BigInteger num,Integer limit){
        String numstr = num.toString();
        Integer len = numstr.length();
        Integer count = limit - len;
        String zeros="";
        for( int i = 0 ; i < count; i++){
            zeros+="0";
        }
        return zeros+numstr;

    }
    
}
