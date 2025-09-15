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


@Service
public class AmazonOrderImport extends StoreConsumer{

    private final KafkaProducerService kafkaproducer;

    private int  API_TRY = 5;
    private int  API_CACHE_JOB_INTERVAL = 5;
    private int  UNKNOWNHOST_EXCEPTION_TRY = 5;
    private int  MAX_ORDER_PULL_PER_TASK = 20; // not more than 20


    public Storage storage;

    public AmazonOrderImport( KafkaProducerService kafkaproducer ) {
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


    @KafkaListener(topics="gcp.store.order.amazon.sync.0", groupId="nano_group_id")
    public void startPoint(String message) {
        try {
            String     body      = new JSONObject(message).getString("body");
            JSONObject json      = new JSONObject(body);
            BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id    = BigInteger.valueOf(json.getInt("log_id"));
            Session session1 = null, session2 = null;
            SessionFactory sessionFactory1 = null, sessionFactory2 = null;
            Transaction tx1 = null, tx2 = null;
            Log log = null;
            String local_folder    = new GenerateRandomString(10).toString();
            String local_file      = new GenerateRandomString(10).toString()+".csv";
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
                    //this.startProcessCacheJob(client.getId(), new BigInteger("33119"));

                    tx2 = session2.beginTransaction();

                    log = session2.get(Log.class, log_id);
                    
                    if(log != null ){
                        // logic for checking status=cancelled
                        if ( !log.getStatus().equalsIgnoreCase("cancelled") ) {
                            System.out.println("<<<<<< Amazon Order Sync Started >>>>>");
                            log.setStatus("started");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log_add.put("value","Task Started");

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            Setting sync_after     = session2.bySimpleNaturalId(Setting.class).load(this.getEnv().getProperty("spring.integration.amazon.order_last_sync_time"));
                            String  sync_after_val = sync_after.getValue();
                            LocalDateTime  sync_after_ltd = LocalDateTime.parse(sync_after_val.split("T")[0]+
                            "T" + sync_after_val
                            .split("T")[1]
                            .split("\\.")[0]);
                            LocalDateTime sync_after_utc = sync_after_ltd.minusHours(5L)
                            .minusMinutes(30L);

                            LocalDateTime sync_before = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusMinutes(5L);
                            String sync_before_val = sync_before + "Z";
                            LocalDateTime sync_before_utc = LocalDateTime.now().minusMinutes(5L);

                            //progress info set
                            JSONObject context = new JSONObject();
                            context.put("sync_after", sync_after_val);
                            context.put("sync_after_utc", sync_after_utc.toString() + "Z");
                            context.put("sync_before", sync_before.toString() + "Z");
                            context.put("sync_before_utc", sync_before_utc + "Z");
                            context.put("job_info",
                                new JSONObject()
                                .put("status", "pending")
                                .put("next_token", null)
                                .put("processed_orders", 0)
                            );


                            JSONObject progress = new JSONObject();
                            progress.put("run_count", 0);
                            log.setProgress( progress.toString() );

                            //create folder and save path to log context
                            //Files.createDirectories(Paths.get(local_folder));
                            //FileWriter csv_file = new FileWriter( csv_file_report );
                            //CSVWriter writer = new CSVWriter( csv_file );
                            //String[] header = { "System Order Ref", "Marketplace Identifier", "Order Date","Marketplace Status","Status"};
                            //writer.writeNext( header );
                            //writer.close();
                            //csv_file.close();
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
                            context.put("csv_file", csv_file_report );

                            log.setContext(context.toString());

                            session2.update(log);

                            this.startProcessOrdersJob( client_id, log_id );
                            
                            this.LogMessageInfo("API Cache will start");
                        } else {
                            this.LogMessageInfo("Amazon Order Import process cancelled by user");
                        }
                        // end logic for checking status=cancelled
                    }
                    tx2.commit();
                    tx2 = null;
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if(tx1 != null )  tx1.rollback();
                if(tx2 != null )  tx2.rollback();
                if(session1 != null) session1.close();
                if(session2 != null) session2.close();
                if(sessionFactory1 !=  null) sessionFactory1.close();
                if(sessionFactory2 !=  null) sessionFactory2.close();
                System.out.println("<<<<<< Cleaning up Session >>>>>");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @KafkaListener(topics="gcp.store.order.amazon.sync.process_orders.0", groupId="nano_group_id")
    public void getOrders(String message) {
        try {
            this.LogMessageInfo("\n" + this.getClass().getName() +": API Cache Job started");
            Environment env       = this.getEnv();
            String current_topic  = "gcp.store.order.amazon.sync.process_orders.0";
            // Extract JSON body of the message
            String body          = new JSONObject(message).getString("body");
            JSONObject json      = new JSONObject(body);
            BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id    = BigInteger.valueOf(json.getInt("log_id"));
            String job_key       = json.getString("job_key") ;
            String trigger_key   = json.getString("trigger_key");
            String topic         = json.getString("topic");
            int min              = json.getInt("min");
            FileWriter  outputfile   = null;
            CSVWriter   writer       = null;
            String      local_folder = null;
            if(!topic.equalsIgnoreCase( current_topic )){
                throw new Exception("Error: Topic Mismatch");
            }
            this.setStorage();
            // Search DB to get client DB access details
            Configuration configuration = new Configuration();
            configuration.configure();
            // set master db from env setup
            configuration.setProperties(this.SetMasterDB());
            SessionFactory sessionFactory = configuration.buildSessionFactory();
            Session session = sessionFactory.openSession();
            Transaction tx = null;
            Boolean client_valid=false;
            Log log=null;
            BigInteger processed_orders  =  BigInteger.ZERO;
            try {
                tx = session.beginTransaction();
                Client client= (Client) session.get(Client.class,client_id);
                if(client!=null) {
                    // Set client as valid
                    client_valid=true;
                } else {
                    // Error log that client id not found or exist
                    // Log error in system log file
                    this.LogMessageWarning("Client not found : "+ message);
                }
                tx.commit();
                tx = null;
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
                    try {
                        String line = "";
                        String cvsSplitBy = ",";

                        //Get Log details
                        log = session1.get(Log.class,log_id);
                        if(log!=null) {
                            
                            // logic for checking status=cancelled
                            if ( log.getStatus().equalsIgnoreCase("cancelled") ) {
                                // throw custom exception
                                throw new UserInterruptionException("Cancelled by User");
                            }

                            // tx2 = session1.beginTransaction();
                            // log.setJob_running(true);
                            // tx2.commit();
                            // tx2 = null;
                            // JSONObject task_progress = log.getProgress() != null? 
                            //                            new JSONObject(log.getProgress()) : 
                            //                            new JSONObject();

                            // Amazon Sync Logic start
                            // check amazon integration active & token valid?
                            Setting verified = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.verified"));
                            if(verified==null || verified.getValue().equals("0")) {
                                throw new Exception("Amazon Integration is not verified");
                            }
                            Setting active = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.active"));
                            if(active==null || active.getValue().equals("0")) {
                                throw new Exception("Amazon Integration is not activated");
                            }
                            Setting token = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.token_key"));
                            if(token==null || token.getValue().equals("0")) {
                                throw new Exception("Amazon Auth Token is not found");
                            }
                            Setting refresh_token = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.refresh_token_key"));
                            if(refresh_token==null || refresh_token.getValue().equals("0")) {
                                throw new Exception("Amazon Auth Refresh Token is not found");
                            }

                            if(this.IsMaxTokenCreateFailWithDate("amazon", session1, true)){
                                this.StopSyncJob(session1, "AMAZON_ORDER_SYNC", false, client.getId(), true);
                                this.StopSyncJob(session1, "AMAZON_FLIPKART_INVENTORY_SYNC", false, client.getId(), true);
                                throw new Exception("Token creation failed Max Time. Sync Job stopped");
                            }

                            tx2 = session1.beginTransaction();

                            Setting expire_in = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.token_expire_in"));

                            if(expire_in==null || expire_in.getValue().equals("0")) {
                                throw new Exception("Amazon {Expire In} value not found");
                            }

                            Setting expired = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.expired"));
                            // calculate token validity
                            Timestamp token_generated= expire_in.getUpdated_at();
                            Timestamp current_timestamp = Timestamp.valueOf(java.time.LocalDateTime.now());
                            long time_differnce_sec = (current_timestamp.getTime() - token_generated.getTime())/1000;
                            Boolean valid_token =false;
                            if(Integer.valueOf(expire_in.getValue()) >= time_differnce_sec) {
                                valid_token = true;
                            }
                            if(expired==null || expired.getValue().equals("1") || !valid_token) {
                                // generate new token and continue
                                try {
                                    OkHttpClient amazon_client = new OkHttpClient();

                                    RequestBody formBody = new com.squareup.okhttp.FormEncodingBuilder()
                                    .add("grant_type", "refresh_token")
                                    .add("refresh_token",refresh_token.getValue())
                                    .add("client_id",env.getProperty("spring.integration.amazon.client_id"))
                                    .add("client_secret",env.getProperty("spring.integration.amazon.client_secret"))
                                    .build();
                                    com.squareup.okhttp.Request token_request = new Request.Builder()
                                    .url(env.getProperty("spring.integration.amazon.token_url"))
                                    .post(formBody)
                                    .build();

                                    Response response_token = amazon_client.newCall(token_request).execute();
                                    if(response_token.code()!=200) {
                                        throw new Exception("Issue generating amazon access token using existing refresh token.May be re auth needed");
                                    }
                                    // retrieve the parsed JSONObject from the response
                                    JSONObject token_response_object = new JSONObject(response_token.body().string());
                                    String new_token= token_response_object.getString("access_token");
                                    Integer token_expire_in = token_response_object.getInt("expires_in");
                                    // update settings
                                    token.setValue(new_token);
                                    token.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    session1.update(token);
                                    expired.setValue("0");
                                    expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    session1.update(expired);
                                    expire_in.setValue(token_expire_in.toString());
                                    expire_in.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    session1.update(expire_in);
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                    if( tx2 != null ) tx2.rollback();
                                    tx2 = null;

                                    Transaction tx_exception = exception_session.beginTransaction();
                                    expired.setValue("1");
                                    expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    exception_session.update(expired);
                                    tx_exception.commit();
                                    this.SetMaxTokenCreateFail("amazon", exception_session, true);
                                    // Will put timer to set integration disabled after 2-3 attempt
                                    throw new Exception("Issue generating Amazon Token :" + ex.getMessage());
                                }
                            }
                            tx2.commit();
                            tx2 = null;

                            tx3 = session1.beginTransaction();

                            JSONObject context_info = new JSONObject( log.getContext() );
                            JSONObject job_info     = context_info.getJSONObject("job_info");

                            String lastUpdatedAfter  = context_info.getString("sync_after_utc");
                            String lastUpdatedBefore = context_info.getString("sync_before_utc");
                            String lastUpdateAt      = context_info.getString("sync_before");

                            String next_token  =  "";

                            // String csv_file      =  context_info.getString("csv_file");
                            //local_folder         =  csv_file.split("/")[0];

                            //outputfile  = new FileWriter( csv_file, true);
                            // writer      = new CSVWriter( outputfile );

                            Boolean STOP_TASK  = false;

                            if(job_info.getString("status").equals("pending")){
                                job_info.put("status", "started");
                            }

                            if(job_info.getString("status").equals("started")){
                                next_token = ( job_info.has("next_token") && 
                                !job_info.getString("next_token").isEmpty() )
                                ? job_info.getString("next_token")
                                : "";
                            }


                            OkHttpClient amazon_client = new OkHttpClient();

                            AWSAuthenticationCredentials awsAuthenticationCredentials = AWSAuthenticationCredentials.builder()
                            .accessKeyId(env.getProperty("spring.integration.amazon.iam_access_keyID"))
                            .secretKey(env.getProperty("spring.integration.amazon.iam_access_secretKey"))
                            .region(env.getProperty("spring.integration.amazon.iam_region"))
                            .build();

                            Response orders_response   = null;
                            for ( int i = 0; i < API_TRY; i++ ) {
                                // if unknown host exception occured retry
                                for (int j = 0; j < this.UNKNOWNHOST_EXCEPTION_TRY; j++) {
                                    try {
                                        com.squareup.okhttp.Request request_orders = new Request.Builder()
                                        .url(
                                            env.getProperty("spring.integration.amazon.orders_url")+
                                            "?MarketplaceIds=" + env.getProperty("spring.integration.amazon.marketplace_id") + 
                                            "&LastUpdatedAfter=" + lastUpdatedAfter +
                                            "&LastUpdatedBefore=" + lastUpdatedBefore +
                                            (!next_token.isEmpty() ? "&NextToken=" + URLEncoder.encode(next_token, StandardCharsets.UTF_8) : "") +
                                            "&MaxResultsPerPage=" + this.MAX_ORDER_PULL_PER_TASK                                               
                                        )
                                        .get()
                                        .addHeader("x-amz-access-token", token.getValue())
                                        .build();
                                        com.squareup.okhttp.Request request_orders_signed = new AWSSigV4Signer(awsAuthenticationCredentials)
                                        .sign(request_orders);
                                        orders_response = amazon_client.newCall(request_orders_signed).execute();
                                        //dont try if unknownhost execption not occured
                                        break;
                                    } catch(UnknownHostException ex) {
                                        this.LogMessageWarning("Unknown Host. Retrying...");
                                        sleep(1000 * j);
                                        continue;
                                    }
                                }
                                // then break the outer loop
                                if( orders_response !=  null ){
                                    if(orders_response.code() == 404){
                                        this.LogMessageInfo("API Failed. Retrying...");
                                        sleep(1000 * 1);
                                        continue;
                                    } else {
                                        break;
                                    }
                                }
                            }
                            //throw error when all failed for unknownhost exception.
                            if(orders_response ==  null){
                                throw new UnknownHostException("Api Endpoint not reachable");
                            }

                            // run code when api has been called successfully

                            if(orders_response.code()==403) { 
                                // Token expired
                                // update settings to set expired true
                                if(tx3 != null) tx3.rollback();
                                    tx3 = exception_session_token.beginTransaction();
                                    expired.setValue("1");
                                    expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    exception_session_token.update(expired);
                                    tx3.commit();
                                    tx3 = null;
                                    throw new ExpiredTokenException("Token Expired");
                                }
                                //after many try api returning error then we will log error and stop the job
                                if(!orders_response.isSuccessful()) {
                                    throw new Exception("Error: " + orders_response.body().string());
                                }
                                if(orders_response.isSuccessful()){
                                    //accept that and enter in cache table and It will proccessed;
                                    JSONObject orders_json = new JSONObject(orders_response.body().string());
                                    if( orders_json.getJSONObject("payload").has("NextToken") && !orders_json.getJSONObject("payload").getString("NextToken").isEmpty() ) {
                                        STOP_TASK = false;
                                        job_info.put("next_token", orders_json
                                        .getJSONObject("payload")
                                        .getString("NextToken"));
                                        context_info.put("job_info", job_info);
                                    } else {
                                        job_info.put("status", "completed");
                                        job_info.put("next_token",null);
                                        context_info.put("job_info", job_info);
                                        STOP_TASK = true;
                                    }

                                    JSONArray orders_json_array = orders_json.getJSONObject("payload").getJSONArray("Orders");
                                    job_info.put("processed_orders", job_info.getInt("processed_orders") + orders_json_array.length() );
                                    context_info.put("job_info", job_info);

                                    //  orderCache = session1.load( OrderCache.class, orderCache.getId());
                                    
                                    // Enter the no. of orders processed in log description
                                    JSONArray log_description = new JSONArray(log.getDescription());
                                    JSONObject log_add1= new JSONObject();
                                    log_add1.put("key","event");
                                    log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                                    log_add1.put("value",orders_json_array.length()+" orders processed");
                                    log_description.put(log_add1);
                                    log.setDescription( log_description.toString() );
                                    log.setUpdated_at( Timestamp.valueOf( java.time.LocalDateTime.now() ) );

                                    for(int cnt = 0; cnt<orders_json_array.length(); cnt++) {
                                        JSONObject order_json          = orders_json_array.getJSONObject(cnt);
                                        String amazon_order_id = order_json.getString("AmazonOrderId");

                                        // Display which order id is currently processed
                                        log.setStatus("processing: [order #"+amazon_order_id+"]");

                                        JSONArray order_items_json = null;
                                        Response order_items_response  = null;
                                        JSONObject order_address_json = null;
                                        Response order_address_response = null;

                                        //retry if api returns error code
                                        for (int k = 0; k < this.API_TRY ; k++) {

                                            //retry loop if unknown host execption occured
                                            for (int l = 0; l < this.UNKNOWNHOST_EXCEPTION_TRY; l++) {
                                                try {
                                                    com.squareup.okhttp.Request request_order_items = new Request.Builder()
                                                    .url(env.getProperty("spring.integration.amazon.orders_url")+"/"+ amazon_order_id+"/orderItems") // getOrderItems
                                                    .get()
                                                    .addHeader("x-amz-access-token", token.getValue())
                                                    .build();

                                                    com.squareup.okhttp.Request request_order_items_signed = new AWSSigV4Signer(awsAuthenticationCredentials)
                                                    .sign(request_order_items);
                                                    order_items_response = amazon_client.newCall(request_order_items_signed).execute();
                                                    //dont try if unknownhost execption not occured
                                                    break;
                                                } catch (UnknownHostException e) {
                                                    sleep(1000 * l);
                                                    continue;
                                                }
                                            }
                                            // if unknown host execption not occured and also api call is successful.
                                            // then break the outer loop
                                            if( order_items_response !=  null ) {
                                                if(order_items_response.code() == 404) {
                                                    this.LogMessageInfo("API Failed. Retrying...");
                                                    sleep(1000 * 1);
                                                    continue;
                                                } else {
                                                    break;
                                                }
                                            }
                                        }

                                        //throw error when all failed for unknownhost exception.
                                        if(order_items_response == null){
                                            throw new UnknownHostException("Api Endpoint is not reachable");
                                        }

                                        if(order_items_response.code()==403) { // Token expired
                                            // update settings to set expired true
                                            if(tx3 != null) tx3.rollback();
                                            tx3 = exception_session_token.beginTransaction();
                                            expired.setValue("1");
                                            expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                            exception_session_token.update(expired);
                                            tx3.commit();
                                            tx3 = null;
                                            throw new ExpiredTokenException("Token Expired");
                                        }
                                        if(!order_items_response.isSuccessful()) {
                                            throw new Exception("Error: " + order_items_response.body().string()); 
                                        } else {
                                            order_items_json  = new JSONObject( order_items_response.body().string() )
                                            .getJSONObject("payload")
                                            .getJSONArray("OrderItems");
                                        }

                                        //retry if api returns error code
                                        for (int k = 0; k < this.API_TRY ; k++) {
                                            //retry loop if unknown host execption occured
                                            for (int l = 0; l < this.UNKNOWNHOST_EXCEPTION_TRY; l++) {
                                                try {
                                                    com.squareup.okhttp.Request request_order_adress = new Request.Builder()
                                                    .url(env.getProperty("spring.integration.amazon.orders_url")+"/"+ amazon_order_id+"/orderItems") // getOrderItems
                                                    .get()
                                                    .addHeader("x-amz-access-token", token.getValue())
                                                    .build();
                                                    com.squareup.okhttp.Request request_order_address_signed = new AWSSigV4Signer(awsAuthenticationCredentials)
                                                    .sign( request_order_adress );
                                                    order_address_response = amazon_client.newCall( request_order_address_signed ).execute();
                                                    //dont try if unknownhost execption not occured
                                                    break;
                                                } catch(UnknownHostException e) {
                                                    sleep(1000 * l);
                                                    continue;
                                                }
                                            }
                                            // if unknown host execption not occured and also api call is successful.
                                            // then break the outer loop
                                            if(order_address_response != null) {
                                                if(order_items_response.code() == 404) {
                                                    this.LogMessageInfo("API Failed. Retrying...");
                                                    sleep(1000 * 1);
                                                    continue;
                                                } else {
                                                    break;
                                                }
                                            }
                                        }

                                        //throw error when all failed for unknownhost exception.
                                        if(order_address_response == null) {
                                            throw new UnknownHostException("Api Endpoint is not reachable");
                                        }

                                        if(order_address_response.code()==403) { // Token expired
                                            // update settings to set expired true
                                            if(tx3 != null) tx3.rollback();
                                            tx3 = exception_session_token.beginTransaction();
                                            expired.setValue("1");
                                            expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                            exception_session_token.update(expired);
                                            tx3.commit();
                                            tx3 = null;
                                            throw new Exception("403");
                                        }
                                        if(!order_address_response.isSuccessful()) {
                                            throw new Exception("Error: " + order_address_response.body().string()); 
                                        } else {
                                            order_address_json  = new JSONObject( order_address_response.body().string() )
                                            .getJSONObject("payload");
                                        }
                                        String order_status =orders_json_array.getJSONObject(cnt).getString("OrderStatus").toLowerCase();
                                        Integer order_system_id=0;
                                        Integer order_system_amazon_id=0;
                                        Boolean inventory_sub=false;
                                        Boolean inventory_add=false;
                                        Boolean cancel_order=false;
                                        Order   order=null;
                                        String  orderSystemStatusName=null;
                                        String  ship_date = orders_json_array.getJSONObject(cnt).has("LatestShipDate")
                                        ? orders_json_array.getJSONObject(cnt).getString("LatestShipDate")
                                        : null;
                                        if(!ship_date.equals(null)) {
                                            ship_date = ship_date.split("T")[0] + " " + ship_date.split("T")[1].split("Z")[0];
                                        }
                                        // find system order status id
                                        JSONArray amazon_status_system_id= new JSONArray(env.getProperty("spring.integration.amazon.order_status"));

                                        for (Integer j=0;j<amazon_status_system_id.length();j++) {
                                            JSONObject status= amazon_status_system_id.getJSONObject(j);
                                            if(order_status.equals(status.getString("status"))) {
                                                order_system_id= status.getInt("system_id");
                                                order_system_amazon_id= status.getInt("id");
                                                if(status.get("soft_cancel").equals(null)) {
                                                    cancel_order=true;
                                                }
                                                JSONArray system_status= new JSONArray(env.getProperty("spring.integration.system.order_status"));
                                                for(Integer k=0; k<system_status.length();k++) {
                                                    if(system_status.getJSONObject(k).getInt("id")==order_system_id) {
                                                        inventory_sub=system_status.getJSONObject(k).getBoolean("inventory_sub");
                                                        inventory_add=system_status.getJSONObject(k).getBoolean("inventory_add");
                                                        orderSystemStatusName = system_status.getJSONObject(k).getString("status");;
                                                        break;
                                                    }
                                                }
                                                break;
                                            }
                                        }

                                        Double order_total =0.0;
                                        if(orders_json_array.getJSONObject(cnt).has("OrderTotal")) {
                                            order_total =orders_json_array.getJSONObject(cnt).getJSONObject("OrderTotal").getDouble("Amount");
                                        }

                                        String fulfillment_channel =orders_json_array.getJSONObject(cnt).getString("FulfillmentChannel");

                                        // search order id in order table. If found just need to check status, if status changed add new status to status table, else follow rest to find other order specific details
                                        Query order_query = session1.createQuery("From Order O WHERE O.marketplace_id = :marketplace_id AND O.marketplace_identifier = :marketplace_identifier");
                                        order_query.setParameter("marketplace_id", Integer.valueOf(env.getProperty("spring.integration.amazon.system_id")));
                                        order_query.setParameter("marketplace_identifier", amazon_order_id);
                                        List<Order> order_list = order_query.list();


                                        if(order_list.size()>0) {  // order found. match status from db & update as needed
                                            // if status changed add into our system
                                            order=order_list.get(0);
                                            order.setIntegration_status("Updated");

                                            Query status_query=session1.createQuery("From OrderTrack OT WHERE OT.order= :order ORDER BY OT.updated_at");
                                            status_query.setParameter("order", order);
                                            List<OrderTrack> order_status_list= status_query.list();
                                            if (order_status_list.size()>0) {
                                                if(order_status_list.get(0).getSub_status_position_id()==order_system_amazon_id) {
                                                    // do nothing
                                                    // reset inventory adjust flag
                                                    inventory_sub=false;
                                                    inventory_add=false;
                                                } else {
                                                    //
                                                    // add new status to db
                                                    OrderTrack new_status= new OrderTrack();
                                                    new_status.setStatus_position_id(order_system_id);
                                                    new_status.setSub_status_position_id(order_system_amazon_id);
                                                    new_status.setOrder(order);
                                                    session1.persist(new_status);
                                                    // update order table status
                                                    order.setStatus_position_id(order_system_id);
                                                    order.setStatus_updated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                                    order.setStatus(orderSystemStatusName);
                                                    order.setSum_total(order_total);
                                                    session1.update(order);

                                                    // check inventory adjust

                                                    if(cancel_order) {
                                                        JSONArray amazon_status= new JSONArray(env.getProperty("spring.integration.amazon.order_status"));
                                                        for(Integer k=0; k<amazon_status.length();k++) {
                                                            //System.out.println("last status");
                                                            //System.out.println(order_status_list.get(0));
                                                            if(amazon_status.getJSONObject(k).getInt("id")==order_status_list.get(order_status_list.size()-1).getSub_status_position_id()) {
                                                                if(amazon_status.getJSONObject(k).isNull("soft_cancel")==false && amazon_status.getJSONObject(k).getBoolean("soft_cancel")) {
                                                                    // reset inventory adjust flag
                                                                    inventory_sub=false;
                                                                    inventory_add=true; // Inventory can be adjusted
                                                                } else {
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
                                        } else { // order need to newly created in db
                                            // set wait time for other call
                                            //sleep(1000*Integer.valueOf(env.getProperty("spring.integration.amazon.ex_wait_time"))*i);
                                            String purchase_date =orders_json_array.getJSONObject(cnt).getString("PurchaseDate");
                                            LocalDateTime purchase_localdatetime = LocalDateTime.parse(purchase_date.split("T")[0]+"T"+purchase_date.split("T")[1].split("Z")[0]);
                                            //  if(purchase_localdatetime.isBefore( last_sync )){
                                            //      continue;
                                            //  }
                                            String last_order_update_date =orders_json_array.getJSONObject(cnt).getString("LastUpdateDate");
                                            //  System.out.println(orders_json_array.getJSONObject(cnt).toString());
                                            // Double order_total =0.0;
                                            // if(orders_json_array.getJSONObject(cnt).has("OrderTotal")) {
                                            //     order_total =orders_json_array.getJSONObject(cnt).getJSONObject("OrderTotal").getDouble("Amount");
                                            // }
                                            String number_item_shipped =orders_json_array.getJSONObject(cnt).getString("NumberOfItemsShipped");
                                            Boolean is_replacement_order =Boolean.valueOf(orders_json_array.getJSONObject(cnt).getString("IsReplacementOrder")); // need to care
                                            String shipment_status =null;
                                            if(orders_json_array.getJSONObject(cnt).has("EasyShipShipmentStatus")) {
                                                shipment_status =orders_json_array.getJSONObject(cnt).getString("EasyShipShipmentStatus"); // need to care
                                            }
                                            String order_type = orders_json_array.getJSONObject(cnt).getString("OrderType");
                                            Boolean is_business_order = orders_json_array.getJSONObject(cnt).getBoolean("IsBusinessOrder");
                                            Boolean is_prime_customer = orders_json_array.getJSONObject(cnt).getBoolean("IsPrime");
                                            Boolean is_premium_order  = orders_json_array.getJSONObject(cnt).getBoolean("IsPremiumOrder");

                                            // extract order address
                                            String shipping_state= null;
                                            String shipping_postal_code=null;
                                            String shipping_city =null;
                                            String shipping_name=null;
                                            if(order_address_json.has("ShippingAddress")) {
                                                shipping_state=order_address_json.getJSONObject("ShippingAddress").getString("StateOrRegion");
                                                shipping_postal_code=order_address_json.getJSONObject("ShippingAddress").getString("PostalCode");
                                                shipping_city=order_address_json.getJSONObject("ShippingAddress").getString("City");
                                                shipping_name=order_address_json.getJSONObject("ShippingAddress").getString("Name");
                                            }    

                                            Customer customer=new Customer();
                                            customer.setName("Amazon Customer");
                                            customer.setCustomer_type("amazon");
                                            // Set orders data
                                            order= new Order();
                                            String order_ref_num=null;
                                            Query last_order_query=session1.createQuery("FROM Order OD WHERE upper(OD.order_ref) LIKE 'ORD%' ORDER BY OD.id DESC");
                                            last_order_query.setFirstResult(0);
                                            last_order_query.setMaxResults(1);
                                            List<Order> last_order=  last_order_query.list();
                                            if(last_order.size()==0) {
                                                order_ref_num=GenerateOrderRef.getInitialRef("ORD");
                                            } else {
                                                order_ref_num=GenerateOrderRef.getRef(last_order.get(0).getOrder_ref(),"ORD");
                                            }
                                            order.setOrder_ref(order_ref_num);
                                            String date_time[]=purchase_date.split("T");
                                            //System.out.println(date_time[0]);
                                            //System.out.println(date_time[1]);

                                            order.setOrder_date(Timestamp.valueOf(date_time[0]+" "+ date_time[1].split("Z")[0]));    
                                            order.setSales_channel("amazon");
                                            //order.setStatus("ordered");
                                            //order.setDelivery_amount(Double.valueOf(shipping_cost));
                                            order.setBuyer_name(shipping_name);
                                            //order.setBuyer_phone(buyer_phone);
                                            order.setBuyer_address(shipping_city);
                                            order.setBuyer_pincode(shipping_postal_code);
                                            order.setBuyer_state(shipping_state);
                                            order.setSum_total(order_total);
                                            //order.setInvoice_number(invoice_number);
                                            //order.setOrder_type(order_type);
                                            order.setDispatch_by(Timestamp.valueOf(ship_date));
                                            //order.setShipping("[{\"ref\": \""+ tracking_info+"\"}]");
                                            order.setLineitems(new ArrayList<>());
                                            order.setOrder_tracks(new ArrayList());
                                            //order.getLineitems().add(lineItem);
                                            order.setCustomer(customer);
                                            order.setIs_valid(true);
                                            order.setMarketplace_id(Integer.valueOf(env.getProperty("spring.integration.amazon.system_id")));
                                            order.setMarketplace_identifier(amazon_order_id);
                                            order.setIntegration_status("New");

                                            // extract order items
                                            for(int cnt_item = 0; cnt_item<order_items_json.length(); cnt_item++) {
                                                String item_asin= order_items_json.getJSONObject(cnt_item).getString("ASIN");
                                                String order_item_id= order_items_json.getJSONObject(cnt_item).getString("OrderItemId");
                                                String item_seller_sku= order_items_json.getJSONObject(cnt_item).getString("SellerSKU");
                                                Integer quantity_ordered= order_items_json.getJSONObject(cnt_item).getInt("QuantityOrdered");
                                                String quantity_shipped= order_items_json.getJSONObject(cnt_item).getString("QuantityShipped");
                                                Double item_price=0.0;
                                                if(order_items_json.getJSONObject(cnt_item).has("ItemPrice")) {
                                                    item_price = order_items_json.getJSONObject(cnt_item).getJSONObject("ItemPrice").getDouble("Amount");
                                                }   
                                                Double item_tax= 0.0;
                                                if(order_items_json.getJSONObject(cnt_item).has("ItemTax")) {
                                                    item_tax=order_items_json.getJSONObject(cnt_item).getJSONObject("ItemTax").getDouble("Amount");
                                                }
                                                // search sku
                                                Item item_check = session1.bySimpleNaturalId(Item.class).load(item_seller_sku);
                                                Query aliased_item_query = session1.createQuery("From Item ITM where ITM.amazon_sku_alias=:amazon_sku_alias");
                                                aliased_item_query.setParameter("amazon_sku_alias", item_seller_sku);
                                                List<Item> aliased_items =aliased_item_query.list();
                                                if(aliased_items.size()!=0){
                                                    item_check = aliased_items.get(0);
                                                }
                                                if(item_check!=null) {
                                                    LineItem line_item= new LineItem();
                                                    line_item.setItem(item_check);
                                                    line_item.setQuantity(quantity_ordered);
                                                    line_item.setUnit_price(item_price);
                                                    line_item.setOrder_item_id(order_item_id);
                                                    order.getLineitems().add(line_item);
                                                } else {
                                                    // Ignore that order to process as line item is not found
                                                }
                                            }
                                            // update status of the order along with tracking
                                            OrderTrack new_status= new OrderTrack();
                                            new_status.setStatus_position_id(order_system_id);
                                            new_status.setSub_status_position_id(order_system_amazon_id);
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

                                        //}
                                        //}
                                        }

                                        // increase / decrease item inventory if needed after successful order status validated
                                        if(inventory_add && inventory_sub) {
                                            throw new Exception("Inventory adjustment error");
                                        }
                                        if(inventory_add || inventory_sub) {
                                            for(LineItem litm: order.getLineitems()) {
                                                Item itm= litm.getItem();
                                                if(itm.getSku()!="undefined") {
                                                    if(inventory_sub) {
                                                        if(itm.getQuantity()-litm.getQuantity()>=0) {
                                                            itm.setQuantity(itm.getQuantity()-litm.getQuantity()); // decrease inventory
                                                        } else {
                                                            // flag item as error wrong inventory sync & set inventory as 0
                                                            itm.setQuantity(0);
                                                            // Set integration error flag
                                                        }
                                                    }
                                                    if (inventory_add) {
                                                        itm.setQuantity(itm.getQuantity()+litm.getQuantity()); // increase inventory
                                                    }
                                                    itm.setInventory_updated_at( Timestamp.valueOf( LocalDateTime.parse(lastUpdatedBefore.split("Z|\\.")[0]) ) );
                                                    session1.update(itm);
                                                }
                                            }
                                        }
                                        
                                        // write to csv what has been updated
                                        //String[] row = new String[]{order.getOrder_ref(), order.getMarketplace_identifier(),order.getOrder_date().toString(), order_status,order.getIntegration_status()};
                                        //writer.writeNext(row);
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
                                        processed_orders  = processed_orders.add( BigInteger.ONE );
                                        // thread sleep after every order process
                                        sleep(1000 * 1);
                                    }
                                }
                                log.setContext(context_info.toString());
                                JSONObject progress = new JSONObject(log.getProgress());
                                progress.put("run_count", progress.getInt("run_count") + 1);
                                log.setProgress( progress.toString() );
                                // if(writer!=null) {
                                //     writer.close();
                                //     writer=null;
                                // }

                                // if( outputfile != null){
                                //     outputfile.close();
                                //     outputfile = null;
                                // }

                                this.LogMessageInfo("\n" + this.getClass().getName() +": api cache job ended");
                                this.LogMessageInfo("\n" + this.getClass().getName() +": processed " + message);

                                if(STOP_TASK) {
                                    log.setStatus("completed");
                                    JSONArray log_description1 = new JSONArray(log.getDescription());
                                    JSONObject log_add1= new JSONObject();
                                    log_add1.put("key","event");
                                    log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                                    log_add1.put("value","Task Completed");

                                    log_description1.put( log_add1 );
                                    log.setDescription( log_description1.toString() );
                                    log.setUpdated_at( Timestamp.valueOf( java.time.LocalDateTime.now() ) );
                                    log.setContext( context_info.toString() );

                                    // Write CSV file only if it has at least one order
                                    JSONArray csv_rows = new JSONArray( log.getCsv_buffer() );
                                    if( csv_rows.length() > 1 ){
                                        String csv_file = context_info.getString("csv_file");
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
                                        if(writer != null) {
                                            writer.close();
                                            writer = null;
                                        }
                                        if(outputfile != null) {
                                            outputfile.close();
                                            outputfile = null;
                                        }
                                        this.uploadLogFile(client, csv_file);

                                        JSONArray output_files_array = new JSONArray();
                                        JSONObject output_file_object= new JSONObject();
                                        output_file_object.put("gcp_file_name",csv_file.split("/")[1]);
                                        output_file_object.put("gcp_log_folder_name", "logs");
                                        output_file_object.put("gcp_task_folder_name",csv_file.split("/")[0]);

                                        output_files_array.put( output_file_object );
                                        log.setOutput_files( output_files_array.toString() );
                                        FileUtils.deleteDirectory(new File(local_folder));
                                    }
                                    // session1.persist( log );

                                    Setting last_synced = session1.bySimpleNaturalId(Setting.class).load(this.getEnv().getProperty("spring.integration.amazon.order_last_sync_time"));
                                    last_synced.setValue(lastUpdateAt);
                                    session1.update(last_synced);
                                    this.LogMessageInfo("\n" + this.getClass().getName() + ": shutting down api cache task");
                                    this.stopProcessOrdersJob(client_id, log_id, job_key, trigger_key);
                                }
                                log.setJob_running(false);
                                session1.persist(log);
                                tx3.commit();
                                tx3 = null;

                                tx4 = session.beginTransaction();
                                Client client_update = session.load(Client.class, client.getId() );
                                client_update.setAmazon_order_count( client_update.getAmazon_order_count().add( processed_orders ));
                                session.persist( client_update );
                                tx4.commit();
                                tx4 = null;
                        }
                    } catch(ExpiredTokenException e) {
                        if (tx2 != null) tx2.rollback();
                        // if (tx4 != null) tx4.rollback();
                        if(log!=null && log.getId()!=null) {
                            tx4 = session1.beginTransaction();
                            log.setJob_running(false);
                            session1.update(log);
                            tx4.commit();
                            tx4 = null;
                        }
                    } // logic for catch the custom exception that thrown after checking status = cancelled
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
                            this.stopProcessOrdersJob(client_id, log_id, job_key, trigger_key);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        if( outputfile != null && writer != null){
                            FileUtils.deleteDirectory(new File(local_folder));
                        }

                        if(writer!=null) {
                            writer.close();
                            writer=null;
                        }

                        if( outputfile != null) {
                            outputfile.close();
                            outputfile = null;
                        }

                        if (tx2 != null) tx2.rollback();
                        if (tx3 != null) tx3.rollback();
                        if (tx4 != null) tx4.rollback();

                        e.printStackTrace();

                        // Log client DB issue
                        if(log!=null && log.getId()!=null) {
                            tx4 = session1.beginTransaction();
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log.setStatus("failed");
                            log_add.put("value","Error : "+ e.getMessage());

                            JSONArray log_description = new JSONArray(log.getDescription());
                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            log.setJob_running(false);
                            session1.update(log);
                            tx4.commit();
                            this.stopProcessOrdersJob(client_id, log_id, job_key, trigger_key);
                        }
                        else {
                            // Log error in system log file
                            this.LogMessageError(e.getMessage());
                        }
                    } finally {
                        session.close();
                        sessionFactory.close();
                        session1.close();
                        exception_session.close();
                        exception_session_token.close();
                        sessionFactory1.close();
                    }
                }
            } catch (Exception e) {
                if (tx!=null) tx.rollback();
                e.printStackTrace();
                //this.stopCacheOrdersJob(client_id, log_id, job_key, trigger_key);
                this.LogMessageError("Primary DB issue : "+e.getMessage());
            } finally {
                session.close();
                sessionFactory.close();
            }
        } catch (Exception e) {
            // e.printStackTrace();
            // Store Exception for future ref & update message as failed
            this.LogMessageError("Error in message : "+ message+" :Error: "+ e.getMessage());
        }
    }

    public JSONObject stopProcessOrdersJob(BigInteger client_id, BigInteger log_id,  String jobKey, String triggerKey) throws Exception {
        Environment env = this.getEnv();
        JSONObject json = new JSONObject();
        json.put("client_id", client_id);
        json.put("log_id", log_id);
        json.put("topic", "gcp.store.order.amazon.sync.process_orders.0");
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
        if(response.isSuccessful()) {
            return new JSONObject(response.body().string());
        } else {
            throw new Exception("Failed start amazon product cache process job");
        }
    }

    public JSONObject startProcessOrdersJob(BigInteger client_id, BigInteger log_id ) throws Exception {
        Environment env = this.getEnv();
        JSONObject json = new JSONObject();
        json.put("client_id", client_id);
        json.put("log_id", log_id);
        json.put("topic", "gcp.store.order.amazon.sync.process_orders.0");
        json.put("min", this.API_CACHE_JOB_INTERVAL );
        json.put("start", true);
        RequestBody body    = RequestBody.create( MediaType.parse("application/json"), json.toString());
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
        .url( env.getProperty("spring.integration.system.AUTOMATE_JOB_HOST") + "/jobs/temporary-job" )
        .post(body)
        .build();      
        Response response = client.newCall(request).execute();
        if(response.isSuccessful()) {
            return new JSONObject(response.body().string());
        }else{
            throw new Exception("Failed start amazon product cache process job");
        }
    }
}