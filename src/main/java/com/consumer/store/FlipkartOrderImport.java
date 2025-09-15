package com.consumer.store;
import com.amazon.SellingPartnerAPIAA.AWSAuthenticationCredentials;
import com.amazon.SellingPartnerAPIAA.AWSSigV4Signer;
import com.consumer.store.helper.GenerateRandomString;
import com.consumer.store.Exceptions.*;
import com.consumer.store.helper.GenerateOrderRef;
import com.consumer.store.model.*;
import com.consumer.store.service.KafkaProducerService;
import com.google.api.client.util.DateTime;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import com.google.gson.JsonObject;
import com.opencsv.CSVWriter;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.MediaType;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.protocol.types.Field.Bool;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.*;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import static java.lang.Thread.sleep;
import com.squareup.okhttp.Credentials;


@Service
public class FlipkartOrderImport extends StoreConsumer{


  
    private final KafkaProducerService kafkaproducer;

    private int  API_TRY = 5;
    private int  JOB_INTERVAL = 5;
    private int  UNKNOWNHOST_EXCEPTION_TRY = 5;
    private int  MAX_SHIPMENT_PULL_PER_TASK = 20;    // not more than 20
    private Storage storage ;
    public final MediaType JSON  = MediaType.parse("application/json; charset=utf-8");
    
    public FlipkartOrderImport( KafkaProducerService kafkaproducer ) throws Exception{
        super( kafkaproducer );
        this.kafkaproducer = this.getKafkaProducer();
    }

    private void setStorage() throws IOException{

      this.storage = StorageOptions.newBuilder()
                    .setProjectId( this.getEnv().getProperty("spring.gcp.project-id") )
                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(this.getEnv().getProperty("spring.gcp.credentials"))))
                    .build()
                    .getService();
    }

    private Storage getStorage(){
          return this.storage;
    }


    private void uploadLogFile(Client client, String file) throws IOException{
        BlobId blobId = BlobId.of( this.getEnv().getProperty("spring.gcp.bucket-name"), client.getName() + "/" + this.getEnv().getProperty("spring.gcp.log-folder-name") + "/" + file);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        this.getStorage().create(blobInfo, Files.readAllBytes(Paths.get( file )));
    }

    @KafkaListener(topics="gcp.store.order.flipkart.sync.0", groupId="nano_group_id")
    public void startPoint(String message){
        try {
            Environment env       = this.getEnv();
            String     body      = new JSONObject(message).getString("body");
            JSONObject json      = new JSONObject(body);
            BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id    = BigInteger.valueOf(json.getInt("log_id"));
            Session session1 = null, session2 = null;
            SessionFactory sessionFactory1 = null, sessionFactory2 = null;
            Transaction tx1 = null, tx2 = null;
            Log log = null;
            String local_folder = new GenerateRandomString(10).toString();
            String local_file   = new GenerateRandomString(10).toString()+".csv";
            String csv_file_report = local_folder+"/"+ local_file;

            try {
                Configuration configuration = new Configuration();
                configuration.configure();
                configuration.setProperties(this.SetMasterDB());
                sessionFactory1 = configuration.buildSessionFactory();
                session1        = sessionFactory1.openSession();
                tx1              = session1.beginTransaction();
                Client client= (Client) session1.get(Client.class,client_id);
                tx1.commit();
                tx1 = null;
                if(client != null){
                    configuration.setProperties(this.SetClientDB(client.getDb_host(),client.getDb_port(),client.getDb_name(),client.getDb_user(),client.getDb_pass()));
                    sessionFactory2 = configuration.buildSessionFactory();
                    session2 = sessionFactory2.openSession();

                    tx2 = session2.beginTransaction();

                    log = session2.get(Log.class, log_id);
                    if(log != null ){
                        if ( !log.getStatus().equalsIgnoreCase("cancelled") ) {
                            System.out.println("<<<<<< Flipkart Order Sync Started >>>>>");
                            log.setStatus("started");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log_add.put("value","Task Started");
        
                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            Setting sync_after     = session2.bySimpleNaturalId(Setting.class).load(this.getEnv().getProperty("spring.integration.flipkart.order_last_sync_time"));
                            String  sync_after_val = sync_after.getValue();
                            LocalDateTime  sync_after_ltd = LocalDateTime.parse(sync_after_val.split("T")[0]+
                                                            "T" + sync_after_val
                                                            .split("T")[1]
                                                            .split("\\.")[0]);
                            LocalDateTime sync_after_utc = sync_after_ltd.minusHours(5L)
                                                        .minusMinutes(30L);
                                                        
                            LocalDateTime sync_before = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusMinutes(5L);
                            String sync_before_val = sync_before + "Z";
                            LocalDateTime sync_before_utc = LocalDateTime.now();
                            JSONObject context = new JSONObject();
                            context.put("sync_after", sync_after_val);
                            context.put("sync_after_utc", sync_after_utc.toString() + "Z");
                            context.put("sync_before", sync_before.toString() + "Z");
                            context.put("sync_before_utc", sync_before_utc + "Z");

                            // Files.createDirectories(Paths.get(local_folder));
                            // FileWriter csv_file = new FileWriter( csv_file_report );
                            // CSVWriter writer = new CSVWriter( csv_file );
                            // String[] header = { "System Order Ref", "Marketplace Identifier", "Order Date","Marketplace Status","Status"};
                            // writer.writeNext( header );
                            // writer.close();
                            // csv_file.close();
                            context.put("csv_file", csv_file_report );
                            log.setCsv_buffer(
                                new JSONArray().put(
                                    new JSONArray()
                                    .put("System Order Ref")
                                    .put("Marketplace Identifier")
                                    .put("Order Date")
                                    .put("Marketplace Status")
                                    .put("Status")
                                ).toString()
                            );

                            JSONArray api_store = new JSONArray();

                            JSONArray flipkart_order_status = new JSONArray(env.getProperty("spring.integration.flipkart.status"));

                            for(Integer i=0; i< flipkart_order_status.length(); i++) {
                                String    states          = flipkart_order_status.getJSONObject( i ).getString("type");
                                JSONArray status_json_arr = flipkart_order_status.getJSONObject( i ).getJSONArray("status");
                                List<String> status_list  = new ArrayList<>();
                                for(Integer j=0; j < status_json_arr.length(); j++) {
                                    String status_name = status_json_arr.get(j).toString();
                                    status_list.add( status_name );
                                }
                                api_store.put(
                                    new JSONObject()
                                    .put("stepId", i + 1)
                                    .put("stateName", states)
                                    .put("statusList", String.join(",", status_list))
                                    .put("callStatus", "pending")
                                    .put("processedOrders",0)
                                );
                            }

                            context.put("api", api_store );
                            log.setContext(context.toString());
                            System.out.println(context.toString());
                            session2.update(log);
                        } else {
                            this.LogMessageInfo("Flipkart Order Import process cancelled by user");
                        }
                        // end logic for checking status=cancelled
                    }
                    tx2.commit();
                    tx2 = null;
                    this.LogMessageInfo("api call job will start");
                    this.startGetOrdresJob(client_id, log_id);
                }


            } catch (Exception e) {
                
                  e.printStackTrace();

            }finally{
                  if(tx1 != null )  tx1.rollback();
                  if(tx2 != null )  tx2.rollback();
                  if(session1 != null) session1.close();
                  if(session2 != null) session2.close();
                  if(sessionFactory1 !=  null) sessionFactory1.close();
                  if(sessionFactory2 !=  null) sessionFactory2.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @KafkaListener(topics="gcp.store.order.flipkart.sync.process_orders.0", groupId="nano_group_id")
    public void getOrders(String message) throws InterruptedException {

        try {

            String current_topic = "gcp.store.order.flipkart.sync.0";
            // Extract JSON body of the message
            String body = new JSONObject(message).getString("body");
            JSONObject json = new JSONObject(body);
            BigInteger client_id= BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id= BigInteger.valueOf(json.getInt("log_id"));
            // Search DB to get client DB access details
            Configuration configuration = new Configuration();
            configuration.configure();
            // set master db from env setup
            configuration.setProperties(this.SetMasterDB());
            SessionFactory sessionFactory = configuration.buildSessionFactory();
            Session session = sessionFactory.openSession();
            Transaction tx = null;
            Boolean client_valid=false;
            Log log = null;
            Environment env      = this.getEnv();
            String job_key       = json.getString("job_key") ;
            String trigger_key   = json.getString("trigger_key");
            String topic         = json.getString("topic");
            int min              = json.getInt("min");
            String local_folder  = null;
            OkHttpClient flipkart_client = new OkHttpClient();
            BigInteger processed_orders  = BigInteger.ZERO;
            this.setStorage();

            
            try {
                tx = session.beginTransaction();
                Client client= (Client) session.get(Client.class,client_id);
                if(client!=null) {
                    // Set client as valid
                    client_valid=true;

                }
                else {
                    // Error log that client id not found or exist
                    // Log error in system log file
                    this.LogMessageWarning("Client not found : "+ message);
                }
                tx.commit();
                if(client_valid) {
                    // Switch to client DB
                    // <!-- Database connection settings -->
                    configuration.setProperties(this.SetClientDB(client.getDb_host(),client.getDb_port(),client.getDb_name(),client.getDb_user(),client.getDb_pass()));
                    SessionFactory sessionFactory1 = configuration.buildSessionFactory();
                    Session session1 = sessionFactory1.openSession();
                    Session exception_session = sessionFactory1.openSession();
                    Session exception_session_token = sessionFactory1.openSession();
                    Transaction tx1 = null;
                    Transaction tx2 = null;
                    Transaction tx3 = null;
                    Transaction tx4 = null;
                    BufferedReader br = null;
                    FileWriter outputfile = null;
                    CSVWriter  writer     = null;
                    try {
                        String line = "";
                        String cvsSplitBy = ",";
                        tx1 = session1.beginTransaction();
                        //Get Log details
                        log = session1.get(Log.class,log_id);
                        if(log!=null) {
                            tx1.commit();
                            
                            if ( log.getStatus().equalsIgnoreCase("cancelled") ) {
                                // throw custom exception
                                throw new UserInterruptionException("Cancelled by User");
                            }

                            // tx2 = session1.beginTransaction();
                            // log.setJob_running(true);
                            // session1.update(log);
                            // tx2.commit();
                            // tx2 = null;
                            // Storage storage = StorageOptions.newBuilder()
                            //         .setProjectId(env.getProperty("spring.gcp.project-id"))
                            //         .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                            //         .build()
                            //         .getService();

                            // Create Random directory to store the file
                            // Files.createDirectories(Paths.get(local_folder));
                            // // generate file name
                            // String csv_file_report=local_folder+"/"+ local_file;
                            // outputfile = new FileWriter(csv_file_report);
                            // // create CSVWriter object filewriter object as parameter
                            // writer = new CSVWriter(outputfile);
                            // // adding header to csv
                            // String[] header = { "System Order Ref", "Marketplace Identifier", "Order Date","Marketplace Status","Status"};
                            // writer.writeNext(header);
                            // Amazon Sync Logic start
                            // check amazon integration active & token valid?
                            Setting verified = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.verified"));
                            if(verified==null || verified.getValue().equals("0")) {
                                throw new Exception("Flipkart Integration is not verified");
                            }
                            Setting active = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.active"));
                            if(active==null || active.getValue().equals("0")) {
                                throw new Exception("Flipkart Integration is not activated");
                            }
                            Setting token = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.token_key"));
                            if(token==null || token.getValue().equals("0")) {
                                throw new Exception("Flipkart auth token is not found");
                            }
                            Setting refresh_token = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.refresh_token_key"));
                            if(refresh_token==null || refresh_token.getValue().equals("0")) {
                                throw new Exception("Flipkart auth refresh token is not found");
                            }
                            Setting expire_in = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.token_expire_in"));
                            if(expire_in==null || expire_in.getValue().equals("0")) {
                                throw new Exception("Flipkart auth refresh token is not found");
                            }

                            if(this.IsMaxTokenCreateFailWithDate("flipkart", session1, true)){
                                this.StopSyncJob( session1, "FLIPKART_ORDER_SYNC", false, client.getId(), true);
                                this.StopSyncJob( session1,"AMAZON_FLIPKART_INVENTORY_SYNC", false, client.getId(), true);
                                throw new Exception("Token creation failed max time. Sync Job Stopped");
                            }

                            Setting expired = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.expired"));
                            
                             // calculate token validity
                             Timestamp token_generated= expire_in.getUpdated_at();
                             Timestamp current_timestamp = Timestamp.valueOf(java.time.LocalDateTime.now());
                             long time_differnce_sec = (current_timestamp.getTime() - token_generated.getTime())/1000;
                             Boolean valid_token =false;
                             
                             if((Integer.valueOf(expire_in.getValue()) - 86400) > time_differnce_sec) {
                                 valid_token = true;
                                 
                             }
 
                             
                            tx2 = session1.beginTransaction();
                            
                            if(expired==null || expired.getValue().equals("1") || !valid_token ) {
                                // generate new token and continue
                                try {

                                    com.squareup.okhttp.Response token_response = null;

                                    String token_url = env.getProperty("spring.integration.flipkart.token_url") + "?" +
                                                       "redirect_uri="   + env.getProperty("spring.integration.flipkart.redirect_url") +
                                                       "&state="         + env.getProperty("spring.integration.flipkart.state") +
                                                       "&refresh_token=" + refresh_token.getValue() +
                                                       "&grant_type="    + "refresh_token";

                                    com.squareup.okhttp.Request token_request = new Request.Builder()
                                            .header("Authorization", Credentials.basic(env.getProperty("spring.integration.flipkart.app_id"),env.getProperty("spring.integration.flipkart.app_secret")))
                                            .url(token_url)
                                            .build();

                                    token_response = flipkart_client.newCall(token_request).execute();
                                    JSONObject token_response_object = new JSONObject( token_response.body().string() );
                                    System.out.print(token_response_object.toString());
                                    // retrieve the parsed JSONObject from the response
                                    System.out.println( token_response);
                                    if(!token_response.isSuccessful()){
                                        //System.out.println(token_response.getBody().getObject());
                                        throw new Exception("Issue generating Flipkart Access Token using existing Refresh Token.May be re auth needed");
                                    }
                                    System.out.println("Flipkart Token refreshing");
                                    String new_token= token_response_object.getString("access_token");
                                    String new_token_expire_in= token_response_object.getString("expires_in");
                                    // update settings
                                    token.setValue(new_token);
                                    token.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    session1.update(token);
                                    expire_in.setValue(new_token_expire_in);
                                    expire_in.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    session1.update(expire_in);
                                    expired.setValue("0");
                                    expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    session1.update(expired);

                                }
                                catch (Exception ex) {
                                    ex.printStackTrace();
                                    Transaction tx_exception = exception_session.beginTransaction();
                                    expired.setValue("1");
                                    expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    exception_session.update(expired);
                                    tx_exception.commit();
                                    SetMaxTokenCreateFail("flipkart", exception_session, true);
                                    throw new Exception("Issue generating flipkart token :" + ex.getMessage());
                                }
                            }

                            tx2.commit();
                            tx2 = null;

                            tx2 = session1.beginTransaction();

                            JSONObject log_info  = new JSONObject(log.getContext());
                            JSONArray  log_api   = log_info.getJSONArray("api");
                            // String csv_file      = log_info.getString("csv_file");
                            // local_folder         = csv_file.split("/")[0];

                            // outputfile  = new FileWriter( csv_file, true);
                            // writer      = new CSVWriter( outputfile );

                            //get uncompleted api call state and status
                            JSONObject current_api_state = null;
                            Boolean api_state_pending = false;
                            for (int i = 0; i < log_api.length(); i++) {
                                JSONObject api_state = log_api.getJSONObject( i );
                                String current_api_status = api_state.getString("callStatus");
                                if( current_api_status.equals("pending")  ||
                                    current_api_status.equals("started")
                                ){
                                    current_api_state = api_state;
                                    api_state_pending = true;
                                    break;
                                }
                            }
                            
                            Boolean stop_job     = false;
                            Boolean stop_process = false;
                            String lastUpdatedAfter  = log_info.getString("sync_after_utc");
                            String lastUpdatedBefore = log_info.getString("sync_before_utc");
                            String lastUpdatedAt     = log_info.getString("sync_before");

                            LocalDateTime last_inventory_update = LocalDateTime.parse(
                                lastUpdatedBefore.split("\\.")[0]
                            );

                            int shipment_pulled = 0;
                            int page_size  = this.MAX_SHIPMENT_PULL_PER_TASK > 20? 20: this.MAX_SHIPMENT_PULL_PER_TASK;

                            if(current_api_state == null || api_state_pending == false){
                                    stop_job = true;
                            } 
                            else{

                            while( true ) {
                                String state_name  =  current_api_state.getString("stateName");
                                String status_list =  current_api_state.getString("statusList");
                                String next_token  =  current_api_state.has("nextToken")?
                                                      current_api_state.getString("nextToken"):
                                                      "";

                                while(true) { // loop for multiple order page
                                    
                                    for (Integer i = 1; i <= 15; i++) {
                                        try {
                                            sleep(1000*Integer.valueOf(env.getProperty("spring.integration.flipkart.general_wait_time"))*i);
                                            //System.out.println("In the room");

                                            // call orders api to get orders LastUpdatedAfter > order_last_sync
                                            com.squareup.okhttp.Request orders_request = null;
                                            com.squareup.okhttp.RequestBody orders_request_body = null;
                                            JSONObject orders_request_json  = null;
                                            int per_page = ( this.MAX_SHIPMENT_PULL_PER_TASK - shipment_pulled );

                                            if(!state_name.equals("cancelled")){
                                                orders_request_json  = new JSONObject().put("filter",
                                                                       new JSONObject()
                                                                       .put("type", state_name)
                                                                       .put("states", new JSONArray(Arrays.asList( status_list.split(",") )))
                                                                       .put("modifiedDate", 
                                                                                 new JSONObject()
                                                                                 .put("from", lastUpdatedAfter.split("Z|\\.")[0])
                                                                                 .put("to", lastUpdatedBefore.split("Z|\\.")[0] )
                                                                        )
                                                               ).put("pagination", 
                                                                        new JSONObject()
                                                                        .put("pageSize", per_page )
                                                               );
                                            }else{
                                                orders_request_json   = new JSONObject().put("filter",
                                                                        new JSONObject()
                                                                       .put("type", state_name)
                                                                       .put("states", new JSONArray(Arrays.asList(status_list.split(","))))
                                                                       .put("cancellationDate", 
                                                                                 new JSONObject()
                                                                                 .put("from", lastUpdatedAfter.split("Z|\\.")[0])
                                                                                 .put("to", lastUpdatedBefore.split("Z|\\.")[0] )
                                                                        )
                                                               ).put("pagination", 
                                                                         new JSONObject()
                                                                        .put("pageSize", per_page )
                                                               );
                                            }
                                            // System.out.println(next_token);
                                            System.out.println("flipkart order get request");
                                            System.out.println( orders_request_json );

                                            String flipkart_shipments_url = env.getProperty("spring.integration.flipkart.api_url")+ (next_token.isEmpty() ? "/v3/shipments/filter" : next_token);
                                            com.squareup.okhttp.Response order_http_response = null;

                                           
                                            if(next_token.isEmpty()){
                                                  orders_request_body = com.squareup.okhttp.RequestBody.create( this.JSON, orders_request_json.toString());
                                                  orders_request      =  new com.squareup.okhttp.Request.Builder() 
                                                                        .url( flipkart_shipments_url ) // getOrderItems
                                                                        .addHeader("Content-Type", "application/json")
                                                                        .addHeader("Authorization", "Bearer "+ token.getValue())
                                                                        .addHeader("accept", "application/json")
                                                                        .post( orders_request_body )
                                                                        .build();
                                            }else{
                                                orders_request      =    new com.squareup.okhttp.Request.Builder() 
                                                                        .url( flipkart_shipments_url ) // getOrderItems
                                                                        .addHeader("Content-Type", "application/json")
                                                                        .addHeader("Authorization", "Bearer "+ token.getValue())
                                                                        .addHeader("accept", "application/json")
                                                                        .get()
                                                                        .build();
                                            }
                                            order_http_response = flipkart_client.newCall(orders_request).execute();
                                            JSONObject orders_json = new JSONObject(order_http_response.body().string());
                                            
                                            // retrieve the parsed JSONObject from the response
                                            // System.out.println("response flipkart order sync");
                                            System.out.println( orders_json.toString() );
                                            System.out.println("status: " + order_http_response.code() );
                                            //kong.unirest.json.JSONObject orders_response  = order_http_response.getBody().getObject();
                                            if(order_http_response.code() ==401) { // Token expired
                                                // update settings to set expired true
                                                tx3 = exception_session_token.beginTransaction();
                                                expired.setValue("1");
                                                expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                                exception_session_token.update(expired);
                                                tx3.commit();
                                                throw new ExpiredTokenException("Access token expired");
                                            }
                                            if(order_http_response.code() >= 400 && order_http_response.code() < 500) { // bad request
                                                // throw new Exception("400");
                                                this.LogMessageInfo("Retrying API call");
                                                continue;
                                            }
                                            if(order_http_response.code() !=200) { // wait to call again, service unavailable sleep
                                                throw new Exception(new JSONObject()
                                                 .put("status", order_http_response.code())
                                                 .put("response", orders_json.toString())
                                                 .toString()
                                                ); // wait to call again
                                            }
                                            else {
                                               
                                                // Process orders that received
                                                //latest_order_update_datetime= orders_json.getJSONObject("payload").getString("LastUpdatedBefore");
                                                
                                                JSONArray orders_json_array= orders_json.getJSONArray("shipments");
                                                // Enter the no. of orders processed in log description
                                                JSONArray log_description = new JSONArray(log.getDescription());
                                                JSONObject log_add2= new JSONObject();
                                                log_add2.put("key","event");
                                                log_add2.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                                                log_add2.put("value",orders_json_array.length()+" orders processed");
                                                log_description.put(log_add2);
                                                log.setDescription( log_description.toString() );
                                                log.setUpdated_at( Timestamp.valueOf( java.time.LocalDateTime.now() ) );

                                                // associate orders with line items segment line item for orders
                                                List<Order> orders= new ArrayList<>();
                                                for(int cnt = 0; cnt < orders_json_array.length(); cnt++) {
                                                    // loop through order items
                                                    LocalDateTime dispatchByDate = null;
                                                    String flipkart_shipment_id = orders_json_array.getJSONObject(cnt).getString("shipmentId");

                                                    JSONArray order_items_json_array = orders_json_array.getJSONObject(cnt).getJSONArray("orderItems");
                                                    for (int cnt_lt = 0; cnt_lt < order_items_json_array.length(); cnt_lt++) {
                                                        Order order = null;
                                                        String flipkart_order_id    = order_items_json_array.getJSONObject(cnt_lt).getString("orderId");
                                                        

                                                        // Display which order id is currently processed
                                                        log.setStatus("processing: [order #"+flipkart_order_id+"]");

                                                        Customer customer = new Customer();
                                                        customer.setName("Flipkart Customer");
                                                        customer.setCustomer_type("flipkart");

                                                        String finalOrder_ref = flipkart_order_id;
                                                        order = orders.stream()
                                                                .filter(order_check -> finalOrder_ref.equals(order_check.getMarketplace_identifier()))
                                                                .findAny()
                                                                .orElse(null);
                                                        

                                                        if (order == null) {
                                                            order = new Order();
                                                            //order.setOrder_ref(new GenerateRandomString(6).toString());
                                                            order.setMarketplace_identifier(flipkart_order_id + flipkart_shipment_id );
                                                            order.setMarketplace_id(Integer.valueOf(env.getProperty("spring.integration.flipkart.system_id")));
                                                            String order_date[]=order_items_json_array.getJSONObject(cnt_lt).getString("orderDate").split("T");
                                                            order.setOrder_date(Timestamp.valueOf(order_date[0]+" "+ order_date[1].split("\\+")[0]));
                                                            order.setStatus("unshipped");
                                                            //order.setBuyer_name(buyer_name);
                                                            //order.setBuyer_phone(buyer_phone);
                                                            //order.setBuyer_address(buyer_address);
                                                            //order.setBuyer_pincode(buyer_pincode);
                                                            //order.setBuyer_state(buyer_state);
                                                            order.setLineitems(new ArrayList<>());
                                                            order.setCustomer(customer);
                                                            order.setIs_valid(true);
                                                            
                                                            if(orders_json_array.getJSONObject(cnt).has("dispatchByDate")){
                                                                dispatchByDate = LocalDateTime.parse(orders_json_array.getJSONObject(cnt).getString("dispatchByDate"),DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                                                                order.setDispatch_by(Timestamp.valueOf(dispatchByDate));
                                                            }
                                                            
                                                        }
                                                        // System.out.println("itemcheck");
                                                        // System.out.println(order.getOrder_ref());
                                                        // for(int m=0;m<order_items_json_array.length();m++){
                                                        //     System.out.println(order_items_json_array.getJSONObject(m));
                                                        // }

                                                        //Associate line item to the order
                                                        // Add line item

                                                        //validate sku
                                                        Double total_price=0.0;
                                                        Item item_check = session1.bySimpleNaturalId(Item.class).load(order_items_json_array.getJSONObject(cnt_lt).getString("sku"));
                                                        Query aliased_item_query = session1.createQuery("From Item ITM where ITM.flipkart_sku_alias=:flipkart_sku_alias");
                                                        aliased_item_query.setParameter("flipkart_sku_alias", order_items_json_array.getJSONObject(cnt_lt).getString("sku"));
                                                        List<Item> aliased_items =aliased_item_query.list();
                                                        if(aliased_items.size()!=0){
                                                            item_check = aliased_items.get(0);
                                                        }
                                                        if(item_check!=null) {
                                                            LineItem line_item= new LineItem();
                                                            line_item.setItem(item_check);
                                                            line_item.setQuantity(order_items_json_array.getJSONObject(cnt_lt).getInt("quantity"));
                                                            line_item.setUnit_price(Double.valueOf(order_items_json_array.getJSONObject(cnt_lt).getJSONObject("priceComponents").getDouble("totalPrice")));
                                                            line_item.setOrder_item_id(order_items_json_array.getJSONObject(cnt_lt).getString("orderItemId"));
                                                            
                                                            if(order_items_json_array.getJSONObject(cnt_lt).has("priceComponents")) {
                                                                total_price = order_items_json_array.getJSONObject(cnt_lt).getJSONObject("priceComponents").getDouble("totalPrice");
                                                                line_item.setTotal_price(total_price);
                                                            }
                                                            order.getLineitems().add(line_item);
                                                            if(order.getMarketplace_status()==null) {
                                                               order.setMarketplace_status(order_items_json_array.getJSONObject(cnt_lt).getString("status"));
                                                            }

                                                        }
                                                        else {
                                                            // Ignore that order to process as line item is not found
                                                            
                                                            System.out.println(order_items_json_array.getJSONObject(cnt_lt));
                                                            Item blank_item = session1.bySimpleNaturalId(Item.class).load("undefined");
                                                            LineItem line_item= new LineItem();
                                                            line_item.setItem(blank_item);
                                                            line_item.setQuantity(order_items_json_array.getJSONObject(cnt_lt).getInt("quantity"));
                                                            line_item.setUnit_price(Double.valueOf(order_items_json_array.getJSONObject(cnt_lt).getJSONObject("priceComponents").getDouble("customerPrice")));
                                                            line_item.setOrder_item_id(order_items_json_array.getJSONObject(cnt_lt).getString("orderItemId"));
                                                            
                                                            order.getLineitems().add(line_item);
                                                            if(order.getMarketplace_status()==null) {
                                                               order.setMarketplace_status(order_items_json_array.getJSONObject(cnt_lt).getString("status"));
                                                            }
                                                            if(order_items_json_array.getJSONObject(cnt_lt).has("priceComponents")) {
                                                                total_price =order_items_json_array.getJSONObject(cnt_lt).getJSONObject("priceComponents").getDouble("totalPrice");
                                                                line_item.setTotal_price(total_price);
                                                            }
                                                            
                                                        }
                                                        // update orders array
                                                        Integer order_position= orders.indexOf(order);
                                                        if(order_position>-1) {
                                                            orders.remove(order_position);
                                                        }
                                                        orders.add(order);
                                                    }
                                                    shipment_pulled = shipment_pulled + 1;
                                                    current_api_state.put("processedOrders", current_api_state.getInt("processedOrders") + 1);
                                                    System.out.println("shipment pulled: " + shipment_pulled);
                                                    JSONObject log_add1= new JSONObject();
                                                    log_add1.put("key","event");
                                                    log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                                                    log_add1.put("value",orders_json_array.length()+" orders processed");
                
                                                }
                                                // test
                                                // parse orders & update in db
                                                // System.out.println("size: "+orders.size());
                                                // for(int k = 0; k<orders.size(); k++){
                                                //     System.out.println(orders.get(k).getMarketplace_status()==null);
                                                //     System.out.println(orders_json_array.getJSONObject(k).getJSONArray("orderItems"));
                                                // }
                                                for(int cnt = 0; cnt<orders.size(); cnt++) {
                                                    // System.out.println(orders.get(cnt));
                                                    // System.out.println("index: "+cnt);
                                                    String flipkart_order_id=orders.get(cnt).getMarketplace_identifier();
                                                    String order_status =orders.get(cnt).getMarketplace_status().toLowerCase();
                                                    Integer order_system_id=0;
                                                    Integer order_system_flipkart_id=0;
                                                    Boolean inventory_sub=false;
                                                    Boolean inventory_add=false;
                                                    Boolean cancel_order=false;
                                                    Order order=null;
                                                    Double order_total=0.0;
                                                    String orderSystemStatusName=null;
                                                    // find system order status id
                                                    JSONArray flipkart_status_system_id= new JSONArray(env.getProperty("spring.integration.flipkart.order_status"));

                                                    for (Integer j=0;j<flipkart_status_system_id.length();j++) {
                                                        JSONObject status= flipkart_status_system_id.getJSONObject(j);
                                                        if(order_status.equals(status.getString("status"))) {
                                                            order_system_id= status.getInt("system_id");
                                                            order_system_flipkart_id = status.getInt("id");
                                                            if(status.get("soft_cancel").equals(null)) {
                                                                cancel_order=true;
                                                            }
                                                            JSONArray system_status= new JSONArray(env.getProperty("spring.integration.system.order_status"));
                                                            for(Integer k=0; k<system_status.length();k++) {
                                                                if(system_status.getJSONObject(k).getInt("id")==order_system_id) {
                                                                    inventory_sub=system_status.getJSONObject(k).getBoolean("inventory_sub");
                                                                    inventory_add=system_status.getJSONObject(k).getBoolean("inventory_add");
                                                                    orderSystemStatusName = system_status.getJSONObject(k).getString("status");
                                                                    break;
                                                                }
                                                            }
                                                            break;
                                                        }
                                                    }
                                                    //String fulfillment_channel =orders_json_array.getJSONObject(cnt).getString("FulfillmentChannel");

                                                    // search order id in order table. If found just need to check status, if status changed add new status to status table, else follow rest to find other order specific details
                                                    
                                                    Query query = session1.createQuery("From Order O WHERE O.marketplace_id = :marketplace_id AND O.marketplace_identifier = :marketplace_identifier");
                                                    query.setParameter("marketplace_id", Integer.valueOf(env.getProperty("spring.integration.flipkart.system_id")));
                                                    query.setParameter("marketplace_identifier", flipkart_order_id);
                                                    List<Order> order_list = query.list();
                                                    if(order_list.size()>0) {  // order found. match status from db & update as needed
                                                        
                                                        // if status changed add into our system
                                                        order=order_list.get(0);
                                                        order.setIntegration_status("Updated");
                                                        Query status_query=session1.createQuery("From OrderTrack OT WHERE OT.order= :order ORDER BY OT.updated_at");
                                                        status_query.setParameter("order", order);
                                                        List<OrderTrack> order_status_list= status_query.list();
                                                        if (order_status_list.size()>0) {
                                                            if(order_status_list.get(0).getSub_status_position_id()==order_system_flipkart_id) {
                                                                // do nothing
                                                                // reset inventory adjust flag
                                                                inventory_sub=false;
                                                                inventory_add=false;
                                                            }
                                                            else {
                                                                //
                                                                // add new status to db
                                                                OrderTrack new_status= new OrderTrack();
                                                                new_status.setStatus_position_id(order_system_id);
                                                                new_status.setSub_status_position_id(order_system_flipkart_id);
                                                                //System.out.println(new_status);
                                                                // order_status_list.add(new_status);
                                                                // session1.update(order_status_list);
                                                                new_status.setOrder(order);
                                                                session1.persist(new_status);
                                                                // update order table status
                                                                order.setStatus_position_id(order_system_id);
                                                                order.setStatus_updated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                                                order.setStatus(orderSystemStatusName);
                                                                session1.update(order);
                                                                // check inventory adjust
                                                                if(cancel_order) {
                                                                    JSONArray flipkart_status= new JSONArray(env.getProperty("spring.integration.flipkart.order_status"));
                                                                    for(Integer k=0; k<flipkart_status.length();k++) {
                                                                        if(flipkart_status.getJSONObject(k).getInt("id")==order_status_list.get(order_status_list.size()-1).getSub_status_position_id()) {
                                                                            if(flipkart_status.getJSONObject(k).isNull("soft_cancel")==false && flipkart_status.getJSONObject(k).getBoolean("soft_cancel")) {
                                                                                // reset inventory adjust flag
                                                                                inventory_sub=false;
                                                                                inventory_add=true; // Inventory can be adjusted
                                                                            }
                                                                            else {
                                                                                // reset inventory adjust flag
                                                                                inventory_sub=false;
                                                                                inventory_add=false;
                                                                            }
                                                                            break;
                                                                        }
                                                                    }
                                                                }

                                                            }

                                                        }

                                                    }
                                                    else { // order need to newly created in db
                                                        // set wait time for other call
                                                        sleep(1000*Integer.valueOf(env.getProperty("spring.integration.flipkart.ex_wait_time"))*i);
                                                        String purchase_date =orders.get(cnt).getOrder_date().toString();
                                                        LocalDateTime purchase_localdatetime = LocalDateTime.parse(purchase_date.trim().replace(" ", "T"));
                                                        // if(purchase_localdatetime.isBefore(datetime_before_order_last_sync)){
                                                        //     continue;
                                                        // }
                                                        //String last_order_update_date =orders_json_array.getJSONObject(cnt).getString("updatedAt");
                                                        //System.out.println(orders_json_array.getJSONObject(cnt).toString());
                                                        //Double order_total =0.0;
                                                        //if(orders_json_array.getJSONObject(cnt).has("OrderTotal")) {
                                                        // order_total =orders_json_array.getJSONObject(cnt).getJSONObject("OrderTotal").getDouble("Amount");
                                                        // }
                                                        //String number_item_shipped =orders_json_array.getJSONObject(cnt).getString("NumberOfItemsShipped");
                                                        //Boolean is_replacement_order =Boolean.valueOf(orders_json_array.getJSONObject(cnt).getString("IsReplacementOrder")); // need to care
                                                        //String shipment_status =null;
                                                        //if(orders_json_array.getJSONObject(cnt).has("EasyShipShipmentStatus")) {
                                                        //   shipment_status =orders_json_array.getJSONObject(cnt).getString("EasyShipShipmentStatus"); // need to care
                                                        //}
                                                        //String order_type =orders_json_array.getJSONObject(cnt).getString("OrderType");
                                                        // Boolean is_business_order = orders_json_array.getJSONObject(cnt).getBoolean("IsBusinessOrder");
                                                        // Boolean is_prime_customer = orders_json_array.getJSONObject(cnt).getBoolean("IsPrime");
                                                        // Boolean is_premium_order = orders_json_array.getJSONObject(cnt).getBoolean("IsPremiumOrder");



                                                        // extract order address
                                                        //JSONObject order_address_json = new JSONObject(order_address_response.body().string()).getJSONObject("payload");
                                                        String shipping_state= null;
                                                        String shipping_postal_code=null;
                                                        String shipping_city =null;
                                                        String shipping_name=null;
                                                        String order_ref_num=null;
                                                        Query last_order_query=session1.createQuery("FROM Order OD WHERE upper(OD.order_ref) LIKE 'ORD%' ORDER BY OD.id DESC");
                                                        last_order_query.setFirstResult(0);
                                                        last_order_query.setMaxResults(1);
                                                        List<Order> last_order=  last_order_query.list();
                                                        //System.out.println(last_order.get(0));
                                                        if(last_order.size()==0){
                                                            order_ref_num=GenerateOrderRef.getInitialRef("ORD");
                                                        }else{
                                                            order_ref_num=GenerateOrderRef.getRef(last_order.get(0).getOrder_ref(),"ORD");
                                                        }


                                                        //Create order with available info

                                                        // Set orders data
                                                        order= orders.get(cnt);

                                                        order.setSales_channel("flipkart");
                                                        order.setOrder_ref(order_ref_num);
                                                        order.setStatus("unshipped");
                                                        //order.setDelivery_amount(Double.valueOf(shipping_cost));
                                                        order.setBuyer_name(shipping_name);
                                                        //order.setBuyer_phone(buyer_phone);
                                                        order.setBuyer_address(shipping_city);
                                                        order.setBuyer_pincode(shipping_postal_code);
                                                        order.setBuyer_state(shipping_state);
                                                        //order.setSum_total(order_total);


                                                        order.setOrder_tracks(new ArrayList());
                                                        //order.getLineitems().add(lineItem);
                                                        order.setIs_valid(true);
                                                        order.setMarketplace_id(Integer.valueOf(env.getProperty("spring.integration.flipkart.system_id")));
                                                        order.setIntegration_status("New");
                                                        // extract order items
                                                        // update status of the order along with tracking
                                                        OrderTrack new_status= new OrderTrack();
                                                        new_status.setStatus_position_id(order_system_id);
                                                        new_status.setSub_status_position_id(order_system_flipkart_id);
                                                        order.getOrder_tracks().add(new_status);
                                                        // update order table status
                                                        order.setStatus_position_id(order_system_id);
                                                        order.setStatus_updated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                                        order.setStatus(orderSystemStatusName);
                                                        // Save order to db along with line items, status data.  without tax info
                                                        session1.persist(order);
                                                        // adjust inventory flag
                                                        if(!cancel_order) {
                                                            // reset inventory adjust flag
                                                            inventory_sub=true;
                                                            inventory_add=false;
                                                        }


                                                    }

                                                    // increase / decrease item inventory if needed after successful order status validated
                                                    if(inventory_add && inventory_sub) {
                                                        throw new Exception("Inventory adjustment error");
                                                    }
                                                    if(inventory_add || inventory_sub) {
                                                        for(LineItem litm: order.getLineitems()) {
                                                            Item itm= litm.getItem();
                                                            if(itm.getSku()!="undefined"){
                                                            if(inventory_sub) {
                                                                if(itm.getQuantity()-litm.getQuantity()>=0) {
                                                                    itm.setQuantity(itm.getQuantity()-litm.getQuantity()); // decrease inventory
                                                                }
                                                                else {
                                                                    // flag item as error wrong inventory sync & set inventory as 0
                                                                    itm.setQuantity(0);
                                                                    // Set integration error flag
                                                                }

                                                            }
                                                            if (inventory_add) {
                                                                itm.setQuantity(itm.getQuantity()+litm.getQuantity()); // increase inventory
                                                            }
                                                            itm.setInventory_updated_at( Timestamp.valueOf( LocalDateTime.parse(lastUpdatedBefore.split("Z|\\.")[0]) ));

                                                            }
                                                        }
                                                    }
                                                    for(LineItem litm: order.getLineitems()){
                                                        if(litm.getUnit_price()!=null){
                                                           order_total+=litm.getUnit_price();
                                                        }
                                                    }
                                                    order.setSum_total(order_total);
                                                    session1.update(order);

                                                    // if order count > 0 then
                                                    // call
                                                    // write to csv what has been updated
                                                    // String[] row = new String[]{order.getOrder_ref(), order.getMarketplace_identifier(),order.getOrder_date().toString(), order_status,order.getIntegration_status()};
                                                    // writer.writeNext(row);
                                                    log.setCsv_buffer( 
                                                    new JSONArray( log.getCsv_buffer() )
                                                       .put(
                                                          new JSONArray().put(order.getOrder_ref())
                                                         .put(order.getMarketplace_identifier())
                                                         .put(order.getOrder_date().toString())
                                                         .put(order_status)
                                                         .put(order.getIntegration_status())
                                                        ).toString()
                                                    );
                                                    processed_orders = processed_orders.add(BigInteger.ONE);
                                                    
                                                }
                                                // loop if nextToken available with some sleep

                                                if(orders_json.has("hasMore") && orders_json.getBoolean("hasMore"))
                                                {
                                                    next_token= orders_json.getString("nextPageUrl");
                                                }
                                                else {
                                                    next_token="";
                                                }
                                                
                                                break; // continue while loop

                                            }

                                        }
                                        catch(UnknownHostException uhostex){
                                                    sleep(1000 * i * 1);
                                                    continue;
                                        }catch(ExpiredTokenException exte){
                                            
                                            throw new ExpiredTokenException("Access token expired");

                                        }catch (Exception ex) {
                                            ex.printStackTrace();
                                            throw new Exception(ex.getMessage()); // unhandled error
                                        }
                                    }

                                    // check pull quota
                                    // if next token
                                    if( shipment_pulled < this.MAX_SHIPMENT_PULL_PER_TASK ){
                                        if(next_token.isEmpty()) {
                                               int current_index =  current_api_state.getInt("stepId") - 1;
                                               int next_index    =  current_api_state.getInt("stepId");
                                               current_api_state.put("callStatus", "completed");
                                               log_api.put(
                                                   current_index ,
                                                   current_api_state
                                               );
                                               log_info.put("api", log_api);
                                               if( next_index < log_api.length() ){
                                                 current_api_state = log_api.getJSONObject( next_index );
                                               }else{
                                                 stop_process = true;
                                                 stop_job = true;
                                               }
                                               break;

                                        }else{
                                            current_api_state.put("nextToken", next_token);
                                            log_api.put(
                                                        current_api_state.getInt("stepId") - 1,
                                                        current_api_state
                                            );
                                            log_info.put("api", log_api);
                                        }

                                    }else{
                                           if(next_token.isEmpty()){
                                                 System.out.println("dec3: " + next_token + " : "  + shipment_pulled);
                                                 int current_index =  current_api_state.getInt("stepId") - 1;
                                                 int next_index    =  current_api_state.getInt("stepId");
                                                 current_api_state.put("callStatus", "completed");
                                                 log_api.put(
                                                    current_index ,
                                                    current_api_state
                                                 );
                                                 log_info.put("api", log_api);
                                                 if( next_index < log_api.length() ){

                                                 }else{
                                                    stop_process = true;
                                                    stop_job = true;
                                                 }
                                           }else{
                                                 current_api_state.put("nextToken", next_token);
                                                 log_api.put(
                                                    current_api_state.getInt("stepId") - 1,
                                                    current_api_state
                                                 );
                                                 log_info.put("api", log_api);
                                           }
                                           stop_process = true;
                                    }

                                    if( stop_process ){
                                        break;
                                    }
                                }

                                if( stop_process ){
                                    break;
                                }
                                // Amazon Sync logic end
                            }
                           }

                           if(stop_job){
                                // Update status & description
                                // Update task as completed
                                log.setStatus("completed");
                                JSONArray log_description1 = new JSONArray(log.getDescription());
                                JSONObject log_add1= new JSONObject();
                                log_add1.put("key","event");
                                log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                                log_add1.put("value","Task Completed");
    
                                log_description1.put(log_add1);
                                log.setDescription(log_description1.toString());
                                log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                

                                // Write CSV file only if it has at least one order
                                JSONArray csv_rows = new JSONArray( log.getCsv_buffer() );
                                if( csv_rows.length() > 1 ){
                                    String csv_file = log_info.getString("csv_file");
                                    local_folder = csv_file.split(File.separator)[0];
                                    Files.createDirectories( Paths.get( local_folder ) );
                                    outputfile   = new FileWriter( csv_file );
                                    writer       = new CSVWriter( outputfile );
                                    for (int k = 0; k < csv_rows.length(); k++) {
                                        JSONArray row = csv_rows.getJSONArray(k);
                                        writer.writeNext(new String[]{ 
                                            row.getString(0) , 
                                            row.getString(1) ,
                                            row.getString(2) ,
                                            row.getString(3) ,
                                            row.getString(4) ,
                                        
                                        });
                                    }
                                    if(writer != null){
                                        writer.close();
                                        writer = null;
                                    }
                                    if(outputfile != null){
                                        outputfile.close();
                                        writer = null;
                                    }

                                    this.uploadLogFile(client, csv_file);
                                    JSONArray output_files_array = new JSONArray();
                                    JSONObject output_file_object= new JSONObject();
                                    output_file_object.put("gcp_file_name",csv_file.split("/")[1]);
                                    output_file_object.put("gcp_log_folder_name", "logs");
                                    output_file_object.put("gcp_task_folder_name",csv_file.split("/")[0]);

                                    output_files_array.put( output_file_object );
                                    log.setOutput_files( output_files_array.toString() );
                                }

                                log.setContext(log_info.toString());
                                log.setJob_running(false);
                                session1.update(log);

                                if(local_folder != null){
                                    FileUtils.deleteQuietly( new File(local_folder) );
                                }

                                Setting order_last_sync = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.order_last_sync_time"));
                                order_last_sync.setValue(lastUpdatedAt);
                                session1.update(order_last_sync);
                                this.LogMessageInfo(this.getClass().getName() + " : " + "shutting down the job");
                                this.stopGetOrdersJob(client_id, log_id, topic, trigger_key);
                            }else{
                                log.setJob_running(false);
                                log.setContext(log_info.toString());
                                session1.update(log);
                            }

                            // // close report file
                           
                        }
                        tx2.commit();

                        tx4 = session.beginTransaction();
                        Client update_client = session.load(Client.class, client.getId());
                        update_client.setFlipkart_order_count(
                        update_client.getFlipkart_order_count().add(processed_orders));
                        session.persist(update_client);
                        tx4.commit();
                        tx4 = null;
                        
                        System.out.println("Done");
                        // success
                        this.LogMessageInfo("processed : "+ message);
                    }catch(UnknownHostException | ExpiredTokenException e){
                        
                        if (tx2 != null) tx2.rollback();
                        if (tx4 != null) tx4.rollback();
                        if(log!=null && log.getId()!=null) {
                            tx4 = session1.beginTransaction();
                            log.setJob_running(false);
                            session1.update(log);
                            tx4.commit();
                            tx4 = null;
                        }

                        if(writer!=null) {
                            writer.close();
                            writer=null;
                        }

                        if( outputfile != null){
                            outputfile.close();
                            outputfile = null;
                        }

                        this.LogMessageWarning("unknown host exception ocurred");
                           
                    }
                    // logic for catch the custom exception that thrown after checking status = cancelled
                    catch(UserInterruptionException ex) {
                        if (tx2 != null) tx2.rollback();

                        if(log!=null && log.getId()!=null) {

                            JSONArray log_description1 = new JSONArray(log.getDescription());
                            JSONObject log_add1= new JSONObject();
                            log_add1.put("key","event");
                            log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log_add1.put("value","Task Cancelled by User");
                            log_description1.put( log_add1 );
                            log.setDescription( log_description1.toString() );
                            log.setUpdated_at( Timestamp.valueOf( java.time.LocalDateTime.now() ) );

                            tx4 = session1.beginTransaction();
                            log.setJob_running(false);
                            session1.update(log);
                            tx4.commit();
                            tx4 = null;
                            if(local_folder != null){
                                FileUtils.deleteQuietly( new File(local_folder) );
                            }
                            this.stopGetOrdersJob(client_id, log_id, job_key, trigger_key);
                        }
                    } 
                    catch (Exception e) {

                        if(writer!=null) {
                            writer.close();
                            writer=null;
                        }

                        if( outputfile != null){
                            outputfile.close();
                            outputfile = null;
                        }

                        if(local_folder != null){
                            FileUtils.deleteQuietly(new File(local_folder));
                        }

                        if (tx2 != null) tx2.rollback();
                        if (tx4 != null) tx2.rollback();
                        e.printStackTrace();
                        // Log client DB issue
                        if(log!=null && log.getId()!=null) {
                            tx3 = session1.beginTransaction();
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            System.out.println("Error :" + e.getMessage());

                            log.setStatus("failed");
                            log_add.put("value","Error : "+ e.getMessage());
                            log_add.put("line",e.getStackTrace()[0]);
                            
                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            log.setJob_running(false);
                            session1.update(log);
                            tx3.commit();
                            this.stopGetOrdersJob(client_id, log_id, job_key, trigger_key);
                            
                        }else {
                            // Log error in system log file
                            this.LogMessageError(e.getMessage());
                        }


                    } finally {
                        // close report file
                        if(writer!=null) {
                            //closing writer connection
                            writer.close();
                            outputfile.close();
                        }
                        // Close original file (if open)
                        if(br!=null) {
                            br.close();
                        }
                        session.close();
                        sessionFactory.close();
                        session1.close();
                        exception_session.close();
                        exception_session_token.close();
                        sessionFactory1.close();
                    }
                }
            }

            catch (Exception e) {
                //if (tx!=null) tx.rollback();
                e.printStackTrace();
                // log primary DB issue
                this.LogMessageError("Primary DB issue : "+e.getMessage());

            } finally {
                session.close();
                sessionFactory.close();
            }


        } catch (Exception e) {
            //e.printStackTrace();
            //Store Exception for future ref & update message as failed
            this.LogMessageError("Error in message : "+ message+" :Error: "+ e.getMessage());
        }

    }

    public JSONObject startGetOrdresJob(BigInteger client_id, BigInteger log_id) throws Exception{

            Environment env = this.getEnv();
            JSONObject json = new JSONObject();
            json.put("client_id", client_id);
            json.put("log_id", log_id);
            json.put("topic", "gcp.store.order.flipkart.sync.process_orders.0");
            json.put("min", this.JOB_INTERVAL );
            json.put("start", true);
    
            RequestBody body    = RequestBody.create( MediaType.parse("application/json"), json.toString());
            OkHttpClient client = new OkHttpClient();
            Request request = new Request.Builder()
            .url( env.getProperty("spring.integration.system.AUTOMATE_JOB_HOST") + "/jobs/temporary-job" )
            .post(body)
            .build();      
            Response response = client.newCall(request).execute();
            if(response.isSuccessful()){
                return new JSONObject(response.body().string());
            }else{
                throw new Exception("Failed start amazon product cache process job");
            }

    }

    public JSONObject stopGetOrdersJob(BigInteger client_id, BigInteger log_id, String jobKey, String triggerKey) throws Exception{
          Environment env = this.getEnv();
          JSONObject json = new JSONObject();
          json.put("client_id", client_id);
          json.put("log_id", log_id);
          json.put("topic", "gcp.store.order.flipkart.sync.process_orders.0");
          json.put("min", 0);
          json.put("job_key", jobKey);
          json.put("trigger_key", triggerKey);
          json.put("start", false);
          
          RequestBody body    = RequestBody.create( MediaType.parse("application/json"), json.toString());
          OkHttpClient client = new OkHttpClient();
          Request request = new Request.Builder()
          .url( env.getProperty("spring.integration.system.AUTOMATE_JOB_HOST") + "/jobs/temporary-job" )
          .post( body )
          .build();      
          Response response = client.newCall(request).execute();
          if(response.isSuccessful()){
              return new JSONObject(response.body().string());
          }else{
              throw new Exception("Failed start amazon product cache process job");
          }
    }
}
