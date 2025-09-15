package com.consumer.store;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.springframework.core.env.Environment;
import com.consumer.store.model.*;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

@Service
public class BaseController {

    @Autowired
    private Environment env;

    public Properties SetMasterDB(){
        Properties properties = new Properties();
        properties.put("hibernate.connection.url", "jdbc:mysql://"+env.getProperty("spring.db.host")+":" + env.getProperty("spring.db.port") + "/" + env.getProperty("spring.db.database"));
        properties.put("hibernate.connection.username",env.getProperty("spring.db.user"));
        properties.put("hibernate.connection.password",env.getProperty("spring.db.pass"));
        properties.put("show_sql",env.getProperty("spring.db.show"));
        properties.put("format_sql",env.getProperty("spring.db.format"));
        properties.put("use_sql_comments",env.getProperty("spring.db.comments"));
        return properties;
    }

    public void SetMaxTokenCreateFail(String marketplace, Session session, boolean useNewTransaction) throws Exception{
        JSONObject access_token_creation_fail_json;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata"));
        Transaction transaction = null;
        

        if( marketplace.equalsIgnoreCase( "flipkart") ){
             access_token_creation_fail_json = new JSONObject( env.getProperty("spring.integration.flipkart.access_token_creation_fail") );
        }else if( marketplace.equalsIgnoreCase( "amazon") ){
             access_token_creation_fail_json = new JSONObject( env.getProperty("spring.integration.amazon.access_token_creation_fail") );
        }else{
            throw new Exception("Invalid marketplace");
        }
        if(useNewTransaction == true){
            transaction = session.beginTransaction();
        }
        Setting access_token_creation_fail = session.bySimpleNaturalId( Setting.class ).load( access_token_creation_fail_json.getString("key") );
        if(access_token_creation_fail == null){
             access_token_creation_fail = new Setting();
             access_token_creation_fail.setKey_name(access_token_creation_fail_json.getString("key"));
             access_token_creation_fail_json.remove("key");
             access_token_creation_fail_json.put("count", 1);
             access_token_creation_fail_json.put("time", localDateTime.format( formatter ));
             access_token_creation_fail.setValue( access_token_creation_fail_json.toString() );
             session.persist( access_token_creation_fail );
             if( transaction != null ){
                transaction.commit();
             }
             
       }else{               
           JSONObject json_value = new JSONObject(access_token_creation_fail.getValue());
           json_value.put( "count", json_value.getInt("count") +1);
           access_token_creation_fail.setValue( json_value.toString() ); 
           session.persist(access_token_creation_fail);
           if( transaction != null ){   
               transaction.commit();
           }
       }
}

public boolean IsMaxTokenCreateFailWithDate( String marketplace, Session session, boolean  useNewTransaction) throws Exception{

        
        Transaction transaction = null;
        LocalDateTime localDateTime = java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata"));

        JSONObject access_token_creation_fail_json;
        if(marketplace.equalsIgnoreCase( "flipkart")){
             access_token_creation_fail_json = new JSONObject( env.getProperty("spring.integration.flipkart.access_token_creation_fail") );
        }else if(marketplace.equalsIgnoreCase( "amazon")){
             access_token_creation_fail_json = new JSONObject( env.getProperty("spring.integration.amazon.access_token_creation_fail") );
        }else{
            throw new Exception("Invalid marketplace");
        }

        if( useNewTransaction == true ){
            transaction = session.beginTransaction();
        }

        Setting access_token_creation_fail = session.bySimpleNaturalId( Setting.class ).load( access_token_creation_fail_json.getString("key") );
        if(access_token_creation_fail == null){
            if( transaction != null ){    
               transaction.commit();
            }
            return false;
        }else{
        
           JSONObject json_value = new JSONObject(access_token_creation_fail.getValue());
           if(   Timestamp.valueOf( json_value.getString("time")).getTime()
                 <
                Timestamp.valueOf( localDateTime.minusHours(json_value.getInt("hours"))).getTime()
                 
            ){
                 json_value.put("sync_stop", true);
                 access_token_creation_fail.setValue( json_value.toString() );
                 session.persist(access_token_creation_fail);
                 if( transaction != null ){ 
                    transaction.commit();
                 }
                 return true;
            }
                
            }
            if( transaction != null ){ 
                transaction.commit();
            }
            return false;
}

public boolean IsMaxTokenCreateFailWithCount(String marketplace, Session session, boolean  useNewTransaction) throws Exception{
     
    Transaction transaction = null;
    LocalDateTime localDateTime = java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata"));

    JSONObject access_token_creation_fail_json;
    if(marketplace.equalsIgnoreCase( "flipkart")){
         access_token_creation_fail_json = new JSONObject( env.getProperty("spring.integration.flipkart.access_token_creation_fail") );
    }else if(marketplace.equalsIgnoreCase( "amazon")){
         access_token_creation_fail_json = new JSONObject( env.getProperty("spring.integration.amazon.access_token_creation_fail") );
    }else{
        throw new Exception("Invalid marketplace");
    }

    if( useNewTransaction == true ){
        transaction = session.beginTransaction();
    }

    Setting access_token_creation_fail = session.bySimpleNaturalId( Setting.class ).load( access_token_creation_fail_json.getString("key") );
    if(access_token_creation_fail == null){
        if( transaction != null ){    
           transaction.commit();
        }
        return false;
    }else{
       JSONObject json_value = new JSONObject(access_token_creation_fail.getValue());
       if( !json_value.isNull("count") &&
           !json_value.isNull("limit") &&
            json_value.getInt("count") >=
            json_value.getInt("limit") )
       {
           
           json_value.put("sync_stop", true);
           System.out.println( json_value );
           access_token_creation_fail.setValue( json_value.toString() );
           session.persist(access_token_creation_fail);
           if( transaction != null ){ 
             transaction.commit();
           }
           return true;
       }
            
    }
        if( transaction != null ){ 
            transaction.commit();
        }
        return false;

}

public void StopSyncJob(Session session, String name, boolean stop, BigInteger client_id, boolean useNewTransaction) throws Exception{

    String JOB_API = env.getProperty("spring.integration.system.AUTOMATE_JOB_HOST");
    JSONArray JOBS = new JSONArray( env.getProperty("spring.integration.system.AUTOMATE_JOBS") );
    Transaction transaction = null;


    for (int i = 0; i < JOBS.length(); i++) {
        String api_url  = JOB_API  + JOBS.getJSONObject(i).getString("url");
        String job_name = JOBS.getJSONObject(i).getString("name");
        String job_key = JOBS.getJSONObject(i).getString("key");
        System.out.println("run1");
        if(name.equals( job_name )){
            System.out.println("run2");
            JSONObject json_body = new JSONObject();
            if( useNewTransaction == true ){
                transaction = session.beginTransaction();
            }
            json_body.put("id", client_id);
            json_body.put("start", stop);
            RequestBody body    = RequestBody.create( MediaType.parse("application/json"), json_body.toString() );
            OkHttpClient client = new OkHttpClient();
            Request request = new Request.Builder()
            .url( api_url )
            .post(body)
            .build();      
            Response response = client.newCall(request).execute();
            System.out.println("run3");
            if(!response.isSuccessful()){
                throw new Exception("Sync Job stop error");
            }else {
                Setting sync_key = session.bySimpleNaturalId(Setting.class).load(job_key);
                if(sync_key != null){
                  sync_key.setValue( "0" );
                  session.persist( sync_key );
                }
                if( transaction != null ){
                    transaction.commit();
                }
                System.out.println("run4");
            }
        }
    }
}

  public void OrderImportCount(BigInteger client_id, String marketplace, BigInteger order_count){

    try {

        Configuration configuration = new Configuration();
        configuration.configure();
        configuration.setProperties(this.SetMasterDB());
        SessionFactory sessionFactory = configuration.buildSessionFactory();
        Session session = sessionFactory.openSession();

        Transaction transaction = session.beginTransaction();

        Client client = session.load(Client.class, client_id);
        String key = null;

        switch ( marketplace ) {
            case   "amazon":
                client.setAmazon_order_count( order_count );
                break;
            case "flipkart":
                client.setFlipkart_order_count( order_count );
                break;
            default:
                System.out.println("missfire");
                break;
        }
        
        session.save(client);

        transaction.commit();
        session.close();
        sessionFactory.close();

        
    } catch (Exception e) {
           e.printStackTrace();
    }


  }
    
}
