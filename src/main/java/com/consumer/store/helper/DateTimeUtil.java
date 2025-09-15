// package com.consumer.store.helper;

// import java.time.format.*;
// import java.time.*;
// import java.time.;

// public class DateTimeUtil {


//     private String zone;
//     private String original;
//     private String parsed;
//     private LocalDateTime ltd;

//     public DateTimeUtil(String dateTime, String zone){
//         String first, second;
//         this.original = dateTime;
//         this.zone = zone;

//         first =  this.original.split("Z")[0];
//         second = this.original.split("Z")[1];
//         second = second.split("T").length > 1?
//                  second.split("T")[0]: second;

//         this.parsed = first + "T" +  second;
        
//     }

//     public DateTimeUtil toAsiaKolkata(){
//         this.ltd = LocalDateTime.now(ZoneId.of("Asia/Kolkata"));
//         return this;
//     }

//     public String toISO8061(){
//         this.ltd = LocalDateTime.now(ZoneId.of("Asia/Kolkata"));
//         return this.ltd.format(DateTimeFormatter.ISO_DATE_TIME);
//     }

    
    
// }
