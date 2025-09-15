package com.consumer.store;

import com.consumer.store.service.KafkaProducerService;
import com.squareup.okhttp.Call;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.hibernate.query.Query;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import java.util.List;
import com.consumer.store.model.*;

@Service
public class SyncIntervalSetController extends StoreConsumer {


    public SyncIntervalSetController(KafkaProducerService kafkaproducer) {
        super(kafkaproducer);
    }

    public OkHttpClient httpClient = new OkHttpClient();

    /**
     * @param message
     */
    @KafkaListener( topics = "gcp.store.system.sync.interval.set.0" , groupId="nano_group_id")
    public void intervalSetTask(String message){

        try {

            Configuration configuration = new Configuration();
            SessionFactory  sessionFactory = null;
            Transaction tx =  null;
            Session session = null;
            String hql = "From Client as CLT";
            String topic = "gcp.store.system.sync.interval.set.0";

            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat  simpleDateFormatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            // String clientsString =  this.getEnv().getProperty("spring.integration.system.CHECK_CLIENTS");
            // JSONArray clients_json = new JSONArray( clientsString );

            


            configuration.configure();
            configuration.setProperties( this.SetMasterDB() );
            sessionFactory = configuration.buildSessionFactory();
            session = sessionFactory.openSession();
            Transaction transaction = session.beginTransaction();

            Query query = session.createQuery( hql );
            List<Client> client_list = query.list();

            AdminLog log = new AdminLog();

            for (int i = 0; i < client_list.size(); i++) {
                
                Client client = client_list.get(i) ;

                try{
                    // boolean match = false;
                    
                    // for (int j = 0; j < clients_json.length(); j++) {
                    //     if(client.getId().compareTo(BigInteger.valueOf(clients_json.getInt(j))) ==0){
                    //         match = true;
                    //     }
                    // }
                   
                    // if(match == true){
                        // System.out.println("match");
                        this.setJobIntervalForClient("AMAZON_ORDER_SYNC", client, log);
                        this.setJobIntervalForClient("FLIPKART_ORDER_SYNC", client, log);
                        this.setJobIntervalForClient("AMAZON_FLIPKART_INVENTORY_SYNC", client, log);
                    // }               
                }catch(Exception ex){
                    //ex.printStackTrace();
                    JSONArray log_description =  ( log.getDescription() == null )?
                              new JSONArray(): 
                              new JSONArray(log.getDescription());
                    JSONObject log_info = new JSONObject();
                    log_info.put("ClientID", client.getId());
                    log_info.put("ClientName", client.getName());
                    log_info.put("Error : ", ex.getMessage());
                    log_description.put(log_info);
                    log.setDescription(log_description.toString());
                }
              
            }
            
            log.setStatus("completed");
            log.setName("INTERVAL_SET_TASK");
            log.setTopic(topic);
            log.setCreated_at(Timestamp.valueOf(LocalDateTime.now()));
            log.setUpdated_at(Timestamp.valueOf(LocalDateTime.now()));
            session.save(log);
            transaction.commit();
            session.close();
            sessionFactory.close();
            
        } catch (Exception e) {
             e.printStackTrace();
             this.LogMessageError("Error: " + e.getMessage());
        }
    }

    public boolean isJobExistButNotPaused(String job, Client client) throws JSONException, Exception{

        String host = this.getEnv().getProperty("spring.integration.system.AUTOMATE_JOB_HOST");

        final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        JSONObject json = new JSONObject();
        json.put("job", job);
        json.put("client_id", client.getId());

        RequestBody requestBody = RequestBody.create(JSON, json.toString());
        Request request = new Request.Builder()
                          .post(requestBody)
                          .url( host + "/jobs/job-state")
                          .build();
        Call call = this.httpClient.newCall(request);
        Response response = call.execute();
        if(!response.isSuccessful()){
             throw new Error( "Error response: " + response.body().string());
        }else{
            
            if(!response.isSuccessful() ){
               throw new Exception("Error: from check paused job api " + response.body().string());
            }

            JSONObject jsonResponce = new JSONObject( response.body().string() );

            if( jsonResponce.has( "state") == true && 
                jsonResponce.isNull( "state") == false &&
                (
                    !jsonResponce.getString("state").equalsIgnoreCase("paused") 
                ) 
            ){
                return true;
            }else{
                return false;
            }
        }

    }
    
    public void changeJobInterval(String url, int interval,String json) throws IOException, Exception{
      
       final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
       RequestBody requestBody = RequestBody.create(JSON, json.toString());
       Request request = new Request.Builder()
                         .post(requestBody)
                         .url(url)
                         .build();
       Call call = this.httpClient.newCall(request);
       Response response = call.execute();
       if(!response.isSuccessful()){
            throw new Error( "Error response: " + response.body().string());
       }
    }

    public void setJobIntervalForClient(String jobName, Client client, AdminLog log) throws JSONException, Exception {

        
        String jobs = this.getEnv().getProperty("spring.integration.system.AUTOMATE_JOBS");
        String host = this.getEnv().getProperty("spring.integration.system.AUTOMATE_JOB_HOST");
        
        JSONArray log_description =  ( log.getDescription() == null )?
                              new JSONArray(): 
                              new JSONArray(log.getDescription());
        JSONObject log_info = new JSONObject();
        log_info.put("ClientID", client.getId());
        log_info.put("ClientName", client.getName());
        log_info.put("Job", jobName);

        JSONArray jobsJSON = new JSONArray( jobs );

        try{
            JSONObject foundJob = null;
            
            for (int i = 0; i <  jobsJSON.length(); i++) {
                JSONObject job = jobsJSON.getJSONObject(i);
                if(jobName.equals(job.getString("name"))){
                    foundJob = job;
                    break;
                }
            }
    
            if( foundJob != null ) {
    
                String jobTitle  = foundJob.getString("name");
                String jobUrl    = host + foundJob.getString("url");
                int    interval  = this.getJobIntervalForClient(jobName, client);
                if( this.isJobExistButNotPaused(jobTitle, client)){
                     JSONObject json = new JSONObject();
                     json.put("id", client.getId() );
                     json.put("start", true); 
                     json.put("interval", interval);
                     this.changeJobInterval(jobUrl, interval, json.toString());
                     log_info.put("description", "Interval is now " + interval + "m ");
                }else{
                     log_info.put("description", "Job is not active");
                }
            }else{
                log_info.put("description", "Job is not found");
            }
            log_description.put(log_info);
            log.setDescription(log_description.toString());
       }catch(Exception ex){
            log_info.put("description", "Error: " + ex.getMessage() );
            log_description.put(log_info);
            log.setDescription(log_description.toString());
       }
    }

    public int getJobIntervalForClient(String job, Client client) throws JSONException{

        int cal_interval  = 0;
        int extr_interval = 0;

        switch (job) {
            case ("AMAZON_ORDER_SYNC"):
                cal_interval  = this.calculateIntervalNum(client.getAmazon_order_count().intValue());
                extr_interval = this.getExtraInterval(job);
                break;

            case ("FLIPKART_ORDER_SYNC"):
                cal_interval  = this.calculateIntervalNum(client.getFlipkart_order_count().intValue());
                extr_interval = this.getExtraInterval(job);
                break;

            case ("AMAZON_FLIPKART_INVENTORY_SYNC"):
                cal_interval  =   this.calculateIntervalNum(client.getAmazon_order_count().intValue()) +
                                  this.calculateIntervalNum(client.getFlipkart_order_count().intValue());
                extr_interval =   this.getExtraInterval(job);
                break;
            default:
                break;
        }

        return cal_interval + extr_interval;
    }

    public int getExtraInterval(String job){

        int interval = 0;

        switch (job) {
            case ("AMAZON_ORDER_SYNC"):
                interval = 0;
                break;

            case ("FLIPKART_ORDER_SYNC"):
                interval = 5;
                break;

            case ("AMAZON_FLIPKART_INVENTORY_SYNC"):
                interval = 10;
                break;

            default:
                break;
        }

        return interval;

    }

    public int calculateIntervalNum(int order_count) throws JSONException{
        
        JSONObject json = this.calculateInterval( order_count );
        if( json != null){
            return json.getInt("interval");
        }else{
            return 0;
        }
    }

    public String calculateIntervalLevel(int order_count) throws JSONException{
        
        JSONObject json = this.calculateInterval( order_count );
        if( json != null){
            return json.getString("level");
        }else{
            return null;
        }
    }

    public JSONObject calculateInterval(int order_count) throws JSONException {
        
        String     frequency     = this.getEnv().getProperty("spring.integration.system.AUTOMATE_JOB_FREQUENCY");
        JSONArray  frequencyJSONArray = new JSONArray( frequency );
        JSONObject freqJson  = null;

        for (int i = 0; i < frequencyJSONArray .length(); i++) {
                     
            String freq = frequencyJSONArray.getJSONObject(i).getString("count");
            String min = freq.split("-")[0];
            String max = freq.split("-")[1];

            if(min.equals("*")==false  &&  max.equals("*") == false) {
                 
                if(order_count >= Integer.parseInt( min ) && order_count <= Integer.parseInt(max)){
                     freqJson = frequencyJSONArray.getJSONObject(i);
                     break;
                 }

            }else if(min.equals("*") == true  && max.equals("*")==false){

                if( order_count <= Integer.parseInt(max)){
                    freqJson = frequencyJSONArray.getJSONObject(i);
                    break;
                }
               

            }else if( min.equals("*")==false && max.equals("*") ==true){
                if( order_count >= Integer.parseInt( min )){
                    freqJson = frequencyJSONArray.getJSONObject(i);
                    break;
                }
            }
        }
        return freqJson;
    }

}