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
import com.squareup.okhttp.Credentials;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.MediaType;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import org.json.JSONException;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.protocol.types.Field.Bool;
import org.apache.kafka.common.utils.Time;
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
import com.consumer.store.Exceptions.*;
import static java.lang.Thread.sleep;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


@Service
public class InventoryUpdateController extends StoreConsumer{


    private final KafkaProducerService kafkaproducer;
    private Storage storage;
    private int INVENTORY_UPDATE_JOB_INTERVAL = 5;
    private int MAX_INVENTORY_UPDATE = 40;
    public final MediaType JSON  = MediaType.parse("application/json; charset=utf-8");
    private int  UNKNOWNHOST_EXCEPTION_TRY = 5;


    public InventoryUpdateController( KafkaProducerService kafkaproducer ) throws Exception{
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
    
    @KafkaListener(topics="gcp.store.inventory.amazon_flipkart.sync.0",groupId="nano_group_id")
    public void startPoint(String message){
         
        try {
            Environment env       = this.getEnv();
            String     body      = new JSONObject(message).getString("body");
            JSONObject json      = new JSONObject(body);
            BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id    = BigInteger.valueOf(json.getInt("log_id"));
            Boolean force        = json.has("force")? json.getBoolean("force"): false;
            Session session1 = null, session2 = null;
            SessionFactory sessionFactory1 = null, sessionFactory2 = null;
            Transaction tx1 = null, tx2 = null;
            Log log = null;
            String local_folder = new GenerateRandomString(10).toString();
            String local_file   = new GenerateRandomString(10).toString()+".csv";
            String csv_file_report = local_folder+"/"+ local_file;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            try {
                Configuration configuration = new Configuration();
                configuration.configure();
                configuration.setProperties(this.SetMasterDB());
                sessionFactory1 = configuration.buildSessionFactory();
                session1        = sessionFactory1.openSession();
                tx1             = session1.beginTransaction();
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
                            this.LogMessageInfo("client: " + client.getId() + " log: " + log.getId());
                            System.out.println("<<<<<< Inventory Started >>>>>");
                            log.setStatus("started");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log_add.put("value","Task Started");
                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            System.out.println("<<<<<< Started syncing Amazon & Flipart Inventory >>>>>");
                            Setting sync_after     = session2.bySimpleNaturalId(Setting.class).load("amazon_flipkart_last_sync_time");
                            String  sync_after_val = sync_after.getValue();
                            LocalDateTime  sync_after_ltd = LocalDateTime.parse(sync_after_val.replace(" ", "T")).plusSeconds(1L);
                            LocalDateTime sync_after_utc  = sync_after_ltd.minusHours(5L)
                                                            .minusMinutes(30L);
                            sync_after_val = sync_after_ltd.toString().split("\\.|Z")[0].replace("T", " ");
                                                        
                            LocalDateTime sync_before = LocalDateTime.now(ZoneId.of("Asia/Kolkata"));
                            LocalDateTime sync_before_utc = LocalDateTime.now();
                            JSONObject context = new JSONObject();
                            context.put("sync_after", sync_after_val);
                            context.put("sync_after_utc", sync_after_utc.toString() + "Z");
                            context.put("sync_before", sync_before.toString() + "Z");
                            context.put("sync_before_utc", sync_before_utc + "Z");
                            String update_hql = "From Item I";
                            if(force == false){
                                update_hql +=" where I.sku!=:sku AND ((I.inventory_updated_at > I.amazon_qty_updated_at) or (I.inventory_updated_at > I.flipkart_qty_updated_at)) or I.flipkart_qty_updated_at is null or I.amazon_qty_updated_at is null";
                            }
                            Query query = session2.createQuery( update_hql );
                            System.out.println("<<<<<< "+ query +" >>>>>");
                            if(force == false){
                                query.setParameter("sku","undefined");
                            }
                            List<Item> item_list = query.list();
                            JSONObject json_items = new JSONObject();
                            for (int i = 0; i < item_list.size(); i++) {
                                Item item = item_list.get(i);
                                json_items.put( item.getSku() ,
                                    new JSONObject()
                                    .put("sku", item.getSku())
                                    .put("flipkart_alias", item.getFlipkart_sku_alias())
                                    .put("amazon_alias", item.getAmazon_sku_alias())
                                    .put("quantity", item.getQuantity())
                                    .put("processed", false)
                                    .put("amazon_status", new JSONObject())
                                    .put("flipkart_status", new JSONObject())
                                    .put("amazon_processed", false)
                                    .put("flipkart_processed", false)
                                    .put("flipkart_product_id", item.getFlipkart_product_id())
                                );
                            }
                            context.put("items", json_items);
                            System.out.println("<<<<<< building csv file structure  >>>>>");
                            // Files.createDirectories(Paths.get(local_folder));
                            // FileWriter csv_file = new FileWriter( csv_file_report );
                            // CSVWriter writer = new CSVWriter( csv_file );
                            // String[] header = {"SKU","amazon inventory(new)", "flipkart inventory(old)","flipkart inventory(new)"};
                            //writer.writeNext( header );
                            //writer.close();
                            //csv_file.close();
                            context.put("csv_file", csv_file_report );
                            //csv_file.close();
                            log.setContext(context.toString());
                            System.out.println("<<<<<< built csv -- updating session 2 >>>>>");
                            session2.update(log);
                        } else {
                            this.LogMessageInfo("Amazon-Flipkart Inventory sync process cancelled by user");
                        }
                        // end logic for checking status=cancelled
                    }
                    tx2.commit();
                    tx2 = null;
                    this.LogMessageInfo("Inventory update task will be started");
                    System.out.println("<<<<<< Initializing Task: Inventory update >>>>>");
                    this.startInventoryUpdateJob(client_id, log_id);
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
                  System.out.println("<<<<<< Cleaning up Session >>>>>");
            }

        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    @KafkaListener(topics="gcp.store.inventory.amazon_flipkart.sync.process.0",groupId="nano_group_id")
    //@KafkaListener(topics="gcp.store.inventory.amazon_flipkart.sync.0",groupId="nano_group_id")
    public void updateInventory(String message){

        try {
            System.out.println("<<<<<< Started Listening again: Inventory update >>>>>");
            this.LogMessageInfo("\n" + this.getClass().getName() +": Inventory update task started");
            Environment env      = this.getEnv();
            String current_topic = "gcp.store.inventory.amazon_flipkart.sync.process.0";
            // Extract JSON body of the message
            String body          = new JSONObject(message).getString("body");
            JSONObject json      = new JSONObject(body);
            BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id    = BigInteger.valueOf(json.getInt("log_id"));
            int invCount         = 1;
            String job_key       = json.getString("job_key") ;
            String trigger_key   = json.getString("trigger_key");
            String topic         = json.getString("topic");
            int min              = json.getInt("min");
            String local_folder  = null;
            String csv_file      = null;
            Configuration configuration  = null;
            SessionFactory sessionFactory1 = null, sessionFactory2 = null;
            Session session1 = null, session2 = null;
            Transaction tx1 = null, tx2 = null, tx3 = null, tx4 = null;
            LocalDateTime current_datetime    = LocalDateTime.now( ZoneId.of("Asia/Kolkata"));
            String current_datetime_formatted = current_datetime.toString().split("\\.|Z")[0].replace("T", " "); 
            OkHttpClient http_client = new OkHttpClient();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String xml_folder = new GenerateRandomString(10).toString();
            String xml_file   = new GenerateRandomString(10).toString()+".csv";
            String xml_path   = xml_folder + File.separator + xml_file;
            System.out.println("<<<<<< Creating AmzFeedApiService >>>>>");
            AmzFeedApiService feedService = new AmzFeedApiService();

            if(!topic.equalsIgnoreCase( current_topic )){
                throw new Exception("Error: Topic mismatch");
            }

            try {
                this.setStorage();
                // Search DB to get client DB access details
                System.out.println("<<<<<< Searching DB to get client DB access details >>>>>");
                configuration = new Configuration();
                configuration.configure();
                // set master db from env setup
                configuration.setProperties(this.SetMasterDB());
                sessionFactory1 = configuration.buildSessionFactory();
                session1 = sessionFactory1.openSession();
                Boolean client_valid=false;
                Log log=null;
                BigInteger processed_inventory  =  BigInteger.ZERO;
                tx1 = session1.beginTransaction();
                Client client= (Client) session1.get(Client.class,client_id);
                if(client!=null) {
                    // Set client as valid
                    client_valid=true;

                }
                else {
                    // Error log that client id not found or exist
                    // Log error in system log file
                    this.LogMessageWarning("Client not found : "+ message);
                }
                tx1.commit();
                tx1 = null;
                if(client_valid) {
                    // Switch to client DB
                    System.out.println("<<<<<< Switching over to client DB >>>>>");
                    // <!-- Database connection settings -->
                    configuration.setProperties(this.SetClientDB(client.getDb_host(),client.getDb_port(),client.getDb_name(),client.getDb_user(),client.getDb_pass()));
                    sessionFactory2 = configuration.buildSessionFactory();
                    session2 = sessionFactory2.openSession();
                   
                    try {
                        String line = "";
                        String cvsSplitBy = ",";

                        //Get Log details
                        tx2 = session2.beginTransaction();
                        log = session2.load(Log.class, log_id);
                        tx2.commit();
                        tx2 = null;
                        if( log!=null ) {
                            
                            // logic for checking status=cancelled
                            if ( log.getStatus().equalsIgnoreCase("cancelled") ) {
                                // throw custom exception
                                throw new UserInterruptionException("Cancelled by User");
                            }

                            AWSAuthenticationCredentials awsCredentials = AWSAuthenticationCredentials.builder()
                            .accessKeyId(this.getEnv().getProperty("spring.integration.amazon.iam_access_keyID") )//env value
                            .secretKey( this.getEnv().getProperty("spring.integration.amazon.iam_access_secretKey") )// env value
                            .region( this.getEnv().getProperty("spring.integration.amazon.iam_region") )//env value
                            .build();
                            
                            tx3 = session2.beginTransaction();
                            Setting flipkart_verified = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.verified"));
                            if(flipkart_verified ==null || flipkart_verified.getValue().equals("0")) {
                                throw new Exception("Flipkart Integration is not verified");
                            }
                            Setting flipkart_active = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.active"));
                            if( flipkart_active ==null || flipkart_active.getValue().equals("0")) {
                                throw new Exception("Flipkart Integration is not activated");
                            }
                            Setting flipkart_token = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.token_key"));
                            if( flipkart_token == null || flipkart_token.getValue().equals("0")) {
                                throw new Exception("Flipkart auth token is not found");
                            }
                            Setting flipkart_refresh_token = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.refresh_token_key"));
                            if(flipkart_refresh_token ==null || flipkart_refresh_token.getValue().equals("0")) {
                                throw new Exception("Flipkart auth refresh token is not found");
                            }
                            Setting flipkart_expire_in = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.token_expire_in"));
                            if( flipkart_expire_in == null || flipkart_expire_in.getValue().equals("0")) {
                                throw new Exception("Flipkart expire in not found");
                            }

                            Setting flipkart_expire = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.expired"));
                            if( flipkart_expire== null) {
                                throw new Exception("Flipkart expire value in not found");
                            }
                            Setting flipkart_default_location = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.default_location_key"));

                            if( flipkart_default_location == null || flipkart_default_location.getValue().equals("0") ) {
                               throw new Exception("Amazon expire in not found");
                            }

                            LocalDateTime flipkart_token_expire_date = LocalDateTime.parse(flipkart_token.getUpdated_at().toString().replace(" ", "T"))
                                                                       .plusSeconds( Long.parseLong(flipkart_expire_in.getValue()) );
                         
                            tx3.commit();
                            tx3 = null;
                            
                            if( current_datetime.isAfter(flipkart_token_expire_date) || 
                                (flipkart_expire != null && 
                                 flipkart_expire.getValue().equals("1"))
                            ){
                               try{
                                com.squareup.okhttp.Response flipkart_token_response = null;

                                String flipkart_token_url = env.getProperty("spring.integration.flipkart.token_url") + "?" +
                                                   "redirect_uri="   + env.getProperty("spring.integration.flipkart.redirect_url") +
                                                   "&state="         + env.getProperty("spring.integration.flipkart.state") +
                                                   "&refresh_token=" + flipkart_refresh_token.getValue() +
                                                   "&grant_type="    + "refresh_token";

                                com.squareup.okhttp.Request flipkart_token_request = new Request.Builder()
                                        .header("Authorization", Credentials.basic(env.getProperty("spring.integration.flipkart.app_id"),env.getProperty("spring.integration.flipkart.app_secret")))
                                        .url( flipkart_token_url )
                                        .build();

                                flipkart_token_response =  http_client.newCall( flipkart_token_request ).execute();
                                JSONObject flipkart_token_response_object = new JSONObject( flipkart_token_response.body().string() );
                                
                                System.out.println("Flipkart token refreshing");
                                System.out.print(flipkart_token_response_object.toString());
                                // retrieve the parsed JSONObject from the response
                                if(!flipkart_token_response.isSuccessful()){
                                    throw new Exception("Issue generating flipkart access token using existing refresh token.May be re auth needed");
                                }
                                tx3 = session2.beginTransaction();
                                String flipkart_new_token = flipkart_token_response_object.getString("access_token");
                                String flipkart_new_token_expire_in = flipkart_token_response_object.getString("expires_in");
                                // update settings
                                flipkart_token.setValue(flipkart_new_token);
                                flipkart_token.setUpdated_at(Timestamp.valueOf(LocalDateTime.now()));
                                session2.update( flipkart_token );
                                flipkart_expire_in.setValue(flipkart_new_token_expire_in);
                                flipkart_expire_in.setUpdated_at(Timestamp.valueOf(LocalDateTime.now()));
                                session2.update( flipkart_expire_in );
                                flipkart_expire.setValue("0");
                                flipkart_expire.setUpdated_at(Timestamp.valueOf(LocalDateTime.now()));
                                session2.update(flipkart_expire);
                                tx3.commit();
                                tx3 = null;

                            }
                            catch (Exception ex) {
                                ex.printStackTrace();
                                if(tx3 != null) tx3.rollback();
                                tx3 = session2.beginTransaction();
                                flipkart_expire.setValue("1");
                                flipkart_expire.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                session2.update(flipkart_expire);
                                tx3.commit();
                                tx3 = null;
                                SetMaxTokenCreateFail("flipkart", session2, true);
                                throw new Exception("Issue generating flipkart token :" + ex.getMessage());
                            }
                          }
                          tx3 = null;
                          tx3 = session2.beginTransaction();
                          Setting amazon_verified = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.verified"));
                          if(amazon_verified == null || amazon_verified.getValue().equals("0")) {
                              throw new Exception("Amazon Integration is not verified");
                          }
                          Setting amazon_active = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.active"));
                          if(amazon_active ==null || amazon_active.getValue().equals("0")) {
                              throw new Exception("Amazon Integration is not activated");
                          }
                          Setting amazon_token = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.token_key"));
                          if( amazon_token ==null || amazon_token.getValue().equals("0")) {
                              throw new Exception("Amazon auth token is not found");
                          }
                          Setting amazon_refresh_token = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.refresh_token_key"));
                          if( amazon_refresh_token ==null || amazon_refresh_token.getValue().equals("0")) {
                              throw new Exception("Amazon auth refresh token is not found");
                          }
                          Setting amazon_expired = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.expired"));
                          if( amazon_expired == null) {
                              throw new Exception("Amazon expired not found");
                          }
                          Setting amazon_expire_in = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.token_expire_in"));
                          if( amazon_expire_in == null || amazon_expire_in.getValue().equals("0") ) {
                            throw new Exception("Amazon expire in not found");
                          }
                          
                          String amazon_marketplace_id = env.getProperty("spring.integration.amazon.marketplace_id");

                          LocalDateTime amazon_token_expire_date = LocalDateTime.parse(amazon_token.getUpdated_at().toString().replace(" ", "T"))
                                                                       .plusSeconds( Long.parseLong(amazon_expire_in.getValue()) );
                          tx3.commit();
                          tx3 = null;
                          if( current_datetime.isAfter(amazon_token_expire_date) || 
                              ( amazon_expired != null && 
                                amazon_expired.getValue().equals("1"))
                          ){
                            try {

                                RequestBody amazon_token_body = new com.squareup.okhttp.FormEncodingBuilder()
                                        .add("grant_type", "refresh_token")
                                        .add("refresh_token",amazon_refresh_token.getValue())
                                        .add("client_id",env.getProperty("spring.integration.amazon.client_id"))
                                        .add("client_secret",env.getProperty("spring.integration.amazon.client_secret"))
                                        .build();
                                com.squareup.okhttp.Request amazon_token_request = new Request.Builder()
                                        .url(env.getProperty("spring.integration.amazon.token_url"))
                                        .post( amazon_token_body )
                                        .build();

                                Response amazon_token_response = http_client.newCall( amazon_token_request ).execute();
                                JSONObject amazon_token_response_object = new JSONObject( amazon_token_response.body().string());

                                System.out.println( amazon_token_response_object.toString());
                                if( !amazon_token_response.isSuccessful() ){
                                    throw new Exception("Issue generating amazon access token using existing refresh token.May be re auth needed");
                                }
                                this.LogMessageError("Amazon token is refreshing");
                                tx3 = session2.beginTransaction();
                                // retrieve the parsed JSONObject from the response
                                String  amazon_new_token = amazon_token_response_object.getString("access_token");
                                Integer amazon_token_expire_in  = amazon_token_response_object.getInt("expires_in");
                                // update settings
                                amazon_token.setValue( amazon_new_token );
                                amazon_token.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                session2.update( amazon_token );
                                amazon_expired.setValue("0");
                                amazon_expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                session2.update( amazon_expired );
                                amazon_expire_in.setValue( amazon_token_expire_in.toString());
                                amazon_expire_in.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                session2.update( amazon_expire_in );
                                tx3.commit();
                                tx3 = null;

                            }
                            catch (Exception ex) {
                                ex.printStackTrace();
                                if( tx3 != null ) tx3.rollback();
                                tx3 = session2.beginTransaction();
                                amazon_expired.setValue("1");
                                amazon_expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                session2.update( amazon_expired );
                                tx3.commit();
                                tx3 = null;
                                this.SetMaxTokenCreateFail("amazon", session2, true);
                                // Will put timer to set integration disabled after 2-3 attempt
                                throw new Exception("Issue generating amazon token :" + ex.getMessage());
                            }
                          }
                          tx3 = session2.beginTransaction();
                          JSONObject log_context = new JSONObject(log.getContext());
                          String sync_after  = log_context.getString("sync_after");
                          String sync_before = log_context.getString("sync_before");
                          String sync_after_utc  = log_context.getString("sync_after_utc");
                          String sync_before_utc = log_context.getString("sync_before_utc");
                          csv_file   = log_context.getString("csv_file");
                          JSONObject logged_items  = log_context.getJSONObject("items");

                          JSONObject items  = this.extractWithLimit(logged_items, this.MAX_INVENTORY_UPDATE );
                          int item_len = this.getLen( items );

                          if(item_len != 0){
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add1= new JSONObject();
                            log_add1.put("key","event");
                            log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log_add1.put("value",item_len+" SKUs processed");
                            log_description.put(log_add1);
                            log.setDescription( log_description.toString() );
                            log.setUpdated_at( Timestamp.valueOf( java.time.LocalDateTime.now() ) );
  
                          List<JSONObject> chunklist = this.createJSONChunk(items, flipkart_default_location.getValue(), 10);
                          
                          JSONObject log_obj = new JSONObject();
                          for (int i = 0; i < chunklist.size(); i++) {
                              JSONObject chunk = chunklist.get(i);
                              log.setStatus("processing: [pkt #"+invCount+"]");
                              invCount += i;
                              System.out.println("packet" + i + ": " + chunk);
                              JSONArray  json_sku = chunk.getJSONArray("sku_list");
                              JSONObject request_obj = chunk.getJSONObject("body");

                              for (int j = 0; j < this.UNKNOWNHOST_EXCEPTION_TRY; j++) {
                                  try {
                                    com.squareup.okhttp.RequestBody flipkart_inventory_request_body = com.squareup.okhttp.RequestBody.create(this.JSON, request_obj.toString());
                                    com.squareup.okhttp.Request flipkart_inventory_request = new com.squareup.okhttp.Request.Builder()
                                              .url( env.getProperty("spring.integration.flipkart.api_url")+"/listings/v3/update/inventory" )
                                              .addHeader("Content-Type", "application/json")
                                              .addHeader("Authorization", "Bearer " + flipkart_token.getValue())
                                              .addHeader("accept", "application/json" )
                                              .post( flipkart_inventory_request_body )
                                              .build();
                                    com.squareup.okhttp.Response flipkart_inventory_response = http_client.newCall(flipkart_inventory_request).execute();
                               
                                    JSONObject flipkart_inventory_response_obj =  new JSONObject( flipkart_inventory_response.body().string() );
      
                                    if( flipkart_inventory_response.isSuccessful()){
                                        List<String> sku_list = this.getSkuList( json_sku );
                                        for (String single_sku : sku_list) {
                                            logged_items.getJSONObject( single_sku ).put("flipkart_status", 
                                                 new JSONObject()
                                                .put("status",  "successful")
                                                .put("message", "inventory updated successfully")
                                            ).put("flipkart_processed", true);
                                        } 
                                        // Query query = session2.createQuery("From Item I where I.sku in (:sku_list)");
                                        // query.setParameterList("sku_list", sku_list);
                                        // List<Item> item_list = query.list();
                                        // for (Item single_item : item_list){
                                        //     single_item.setFlipkart_qty_updated_at(Timestamp.valueOf( LocalDateTime.parse( sync_before_utc.split("\\.|Z")[0] )));
                                        //     session2.persist(single_item);
                                        // }
                                        break;
                                    }else{
                                         if( flipkart_inventory_response.code() == 404 ){
                                              throw new NotFoundException("Page Not Found");
                                         }else if( flipkart_inventory_response.code() == 401 ){
                                              throw new ExpiredTokenException("Flipkart Token is expired");
                                         }
                                         else{
                                             throw new Exception("Error : " + flipkart_inventory_response_obj.toString());
                                         }
                                    }
                              } 
                              catch (UnknownHostException uhx) {
                                  uhx.printStackTrace();
                                  List<String> sku_list = this.getSkuList( json_sku );
                                  for (String single_sku : sku_list) {
                                      logged_items.getJSONObject( single_sku ).put("flipkart_status", 
                                           new JSONObject()
                                          .put("status",  "fail")
                                          .put("message", uhx.getMessage() )
                                      ).put("flipkart_processed", true);
                                  } 
                                  continue;
                              }catch(NotFoundException nfx){
                                  nfx.printStackTrace();
                                  List<String> sku_list = this.getSkuList( json_sku );
                                  for (String single_sku : sku_list) {
                                      logged_items.getJSONObject( single_sku ).put("flipkart_status", 
                                           new JSONObject()
                                          .put("status",  "fail")
                                          .put("message", nfx.getMessage() )
                                      ).put("flipkart_processed", true);
                                  }
                                  continue;
                              }catch(ExpiredTokenException etx){
                                etx.printStackTrace();
                                if(tx3  != null)  tx3.rollback();
                                tx3 = session2.beginTransaction();
                                flipkart_expire.setValue("1");
                                flipkart_expire.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                session2.update(flipkart_expire);
                                tx3.commit();
                                tx3 = null;
                                throw etx;
                              }
                              catch(Exception e){
                                 e.printStackTrace();
                                 List<String> sku_list = this.getSkuList( json_sku );
                                 for (String single_sku : sku_list) {
                                     logged_items.getJSONObject( single_sku ).put("flipkart_status", 
                                          new JSONObject()
                                         .put("status",  "fail")
                                         .put("message", e.getMessage() )
                                     ).put("flipkart_processed", true);
                                 }
                                 break;
                              }
                           }
                         }

                           String feed_doc_id = null, feed_upload_url = null, xml_content = null;
                        //    Files.createDirectories(Paths.get(xml_folder));
                           xml_content =  this.createXML( amazon_marketplace_id, items );
                           this.LogMessageInfo("XML File created");
                          
                           com.squareup.okhttp.RequestBody amazon_feed_doc_req_body = com.squareup.okhttp.RequestBody
                          .create(MediaType.parse("application/json; charset=utf-8"), "{\"contentType\":\"text/xml; charset=UTF-8\"}" );
                           com.squareup.okhttp.Request amazon_feed_doc_req = new com.squareup.okhttp.Request.Builder()
                          .url( env.getProperty("spring.integration.amazon.feed_api_url") +"/feeds/2021-06-30/documents")
                          .post( amazon_feed_doc_req_body )
                          .addHeader("x-amz-access-token", amazon_token.getValue() )//db value
                          .addHeader( "Accept","application/json" )
                          .build();
                          com.squareup.okhttp.Request amazon_feed_doc_req_signed = new AWSSigV4Signer(awsCredentials)
                                                                                       .sign(amazon_feed_doc_req);
                          com.squareup.okhttp.Response amazon_feed_doc_res = http_client.newCall(amazon_feed_doc_req_signed).execute();
                          JSONObject amazon_feed_doc_res_obj = new JSONObject( amazon_feed_doc_res.body().string() );
                          
                          if(!amazon_feed_doc_res.isSuccessful()){
                              if(amazon_feed_doc_res.code() == 403){
                                if(tx3  != null)  tx3.rollback();
                                tx3 = session2.beginTransaction();
                                amazon_expired.setValue("1");
                                amazon_expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                session2.update( amazon_expired );
                                tx3.commit();
                                tx3 = null;
                                throw new ExpiredTokenException("Amazon Token is expired");
                              }else{
                                Iterator keys = items.keys();
                                while( keys.hasNext()){
                                     String sku = (String) keys.next();
                                     logged_items.getJSONObject( sku ).put("amazon_status", 
                                            new JSONObject()
                                           .put("status",  "fail")
                                           .put("message", amazon_feed_doc_res_obj.toString() )
                                     ).put("amazon_processed", true);
                                }
                              }
                         }else{
                            
                            feed_upload_url = amazon_feed_doc_res_obj.getString("url");
                            feed_doc_id     = amazon_feed_doc_res_obj.getString("feedDocumentId");
                            this.LogMessageInfo("Feed Document created");
                         }

                         
                        //    xml_content = this.fileToString( xml_path );
                           Request upload_request    = new Request.Builder()
                          .url( feed_upload_url )
                          .put(RequestBody.create(MediaType.parse( String.format("text/xml; charset=%s", StandardCharsets.UTF_8)  ), xml_content.getBytes(StandardCharsets.UTF_8)))
                          .build();
                          Response upload_response = http_client.newCall(upload_request).execute();
                        //   JSONObject upload_response_obj = new JSONObject( upload_response.body().string() );
                          if(!upload_response.isSuccessful()){
                            if( upload_response.code() == 403){
                              if(tx3  != null)  tx3.rollback();
                              tx3 = session2.beginTransaction();
                              amazon_expired.setValue("1");
                              amazon_expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                              session2.update( amazon_expired );
                              tx3.commit();
                              tx3 = null;
                              throw new ExpiredTokenException("Amazon Token is expired");
                            }else{
                                Iterator keys = items.keys();
                                while( keys.hasNext()){
                                     String sku = (String) keys.next();
                                     logged_items.getJSONObject( sku ).put("amazon_status", 
                                            new JSONObject()
                                           .put("status",  "fail")
                                           .put("message", amazon_feed_doc_res_obj.toString() )
                                     ).put("amazon_processed", true);
                                }
                            }
                         }else{
                            this.LogMessageInfo("Feed XML uploaded");
                         }

                          JSONObject create_feed_json = new JSONObject();
                          create_feed_json.put("feedType", "POST_INVENTORY_AVAILABILITY_DATA");
                          create_feed_json.put("inputFeedDocumentId", feed_doc_id );
                          create_feed_json.put("marketplaceIds", new JSONArray().put( amazon_marketplace_id ));
                          RequestBody create_feed_req_body
                          = RequestBody.create(MediaType.parse("application/json; charset=utf-8")
                          , create_feed_json.toString());
                          Request create_feed_req = new Request.Builder()
                          .url( env.getProperty("spring.integration.amazon.feed_api_url") + "/feeds/2021-06-30/feeds")
                          .post( create_feed_req_body )
                          .addHeader("x-amz-access-token", amazon_token.getValue())//db value
                          .addHeader("Accept", "application/json")
                          .build();

                          Request create_feed_req_sign = new AWSSigV4Signer(awsCredentials)
                          .sign( create_feed_req );
                          Response create_feed_response = http_client.newCall(create_feed_req_sign).execute();
                          JSONObject create_feed_response_obj = new JSONObject( create_feed_response.body().string());

                          if(!create_feed_response.isSuccessful()){
                            if( upload_response.code() == 403){
                                if(tx3  != null)  tx3.rollback();
                                tx3 = session2.beginTransaction();
                                amazon_expired.setValue("1");
                                amazon_expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                session2.update( amazon_expired );
                                tx3.commit();
                                tx3 = null;
                                throw new ExpiredTokenException("Amazon Token is expired");
                              }else{
                                  Iterator keys = items.keys();
                                  while( keys.hasNext()){
                                       String sku = (String) keys.next();
                                       logged_items.getJSONObject( sku ).put("amazon_status", 
                                              new JSONObject()
                                             .put("status",  "fail")
                                             .put("message", create_feed_response_obj.toString() )
                                       ).put("amazon_processed", true);
                                  }
                              }
                          }else{
                            Iterator keys = items.keys();
                            while( keys.hasNext()){
                                 String sku = (String) keys.next();
                                 logged_items.getJSONObject( sku ).put("amazon_status", 
                                      new JSONObject()
                                     .put("status",  "successful")
                                     .put("message", "inventory updated")
                                 ).put("amazon_processed", true);
                            }
                          }

                          List<String> processed_skus = new ArrayList<>();
                          Iterator all_sku = logged_items.keys();

                          for (String sku : processed_skus) {
                             logged_items.getJSONObject( sku ).put("processed", true);
                          }
                          
                          while (all_sku.hasNext()) {
                            String sku = (String) all_sku.next();
                            JSONObject item = logged_items.getJSONObject(sku);
                            if(item.getBoolean("amazon_processed") == true || item.getBoolean("flipkart_processed") == true){
                                processed_skus.add( sku );
                            }
                          }
                          for (String sku : processed_skus) {
                             logged_items.getJSONObject(sku).put("processed", true);
                          }
                          
                          log_context.put("items", logged_items);
                          log.setContext( log_context.toString() );
                          log.setJob_running(false);
                          session2.persist( log );
                          tx3.commit();
                          tx3 = null;
                          
                          this.LogMessageInfo("\n" + this.getClass().getName() + ": Inventory Update task completed");
                        }else{
                          List<String> processed_skus = new ArrayList<>();
                          Iterator all_sku = logged_items.keys();
                          JSONObject items_details_log = new JSONObject()
                                                        .put("flipkart_synced", new JSONArray())
                                                        .put("flipkart_not_synced", new JSONArray())
                                                        .put("amazon_synced", new JSONArray())
                                                        .put("amazon_not_synced", new JSONArray());

                          while (all_sku.hasNext()) {
                            String sku = (String) all_sku.next();
                            JSONObject item = logged_items.getJSONObject(sku);
                            if(item.getBoolean("amazon_processed") == true || item.getBoolean("flipkart_processed") == true){
                                processed_skus.add( sku );
                            }
                          }

                          Query item_query = session2.createQuery("From Item I Where I.sku in (:sku_list)");
                          item_query.setParameterList("sku_list", processed_skus);
                          List<Item> process_items = item_query.list();
                          local_folder           = csv_file.split(File.separator)[0];
                          Files.createDirectories( Paths.get( local_folder ) );
                          FileWriter outputfile  = new FileWriter( csv_file, true);
                          CSVWriter writer       = new CSVWriter( outputfile );
                          writer.writeNext(new String[]{"SKU","amazon inventory(new)", "flipkart inventory(old)","flipkart inventory(new)"});
                          for(Item current_item : process_items) {

                               String new_flipkart_qty = "Not updated";
                               String new_amazon_qty = "Not updated";
                               Boolean flipkart_success = false, amazon_success = false;

                               Boolean amazon_processed =  logged_items
                                   .getJSONObject(current_item.getSku())
                                   .getBoolean("amazon_processed");
                                   
                               Boolean flipkart_processed =  logged_items
                                   .getJSONObject(current_item.getSku())
                                   .getBoolean("flipkart_processed");

                               Boolean flipkart_sync = logged_items
                                                       .getJSONObject(current_item.getSku())
                                                       .has("flipkart_product_id");

                               if(flipkart_processed){
                                   String status =  logged_items
                                                       .getJSONObject(current_item.getSku())
                                                       .getJSONObject("flipkart_status")
                                                       .getString("status");
                                   new_flipkart_qty =  status.equals("fail") ? "Not updated": String.valueOf(current_item.getQuantity());
                                   flipkart_success =  !status.equals("fail");
                               }
                               if(!flipkart_sync){
                                  new_flipkart_qty = String.valueOf( current_item.getQuantity() );
                                  flipkart_success = true;
                               }

                               if(amazon_processed){
                                  String status =  logged_items
                                                  .getJSONObject(current_item.getSku())
                                                  .getJSONObject("amazon_status")
                                                  .getString("status");
                                  new_amazon_qty  =   status.equals("fail") ? "Not updated": String.valueOf(current_item.getQuantity());
                                  amazon_success  =  !status.equals("fail");
                               }

                               writer.writeNext(new String[]{
                                  current_item.getSku(),
                                  new_flipkart_qty ,
                                  String.valueOf(current_item.getFlipkart_quantity()) ,
                                  new_amazon_qty ,
                               });

                               if( flipkart_success ){
                                   current_item.setFlipkart_qty_updated_at(Timestamp.valueOf( LocalDateTime.parse( sync_before_utc.split("\\.|Z")[0] )));
                                   if(flipkart_sync){
                                     current_item.setFlipkart_quantity(current_item.getQuantity());
                                   }
                                   items_details_log
                                   .getJSONArray("flipkart_synced")
                                   .put(current_item.getSku());
                               }else{
                                    items_details_log
                                   .getJSONArray("flipkart_not_synced")
                                   .put(current_item.getSku());
                               }

                               if(amazon_success){
                                   current_item.setAmazon_qty_updated_at(Timestamp.valueOf( LocalDateTime.parse( sync_before_utc.split("\\.|Z")[0] )));
                                   items_details_log
                                  .getJSONArray("amazon_synced")
                                  .put(current_item.getSku());
                               }else{
                                   items_details_log
                                  .getJSONArray("amazon_not_synced")
                                  .put(current_item.getSku());
                               }
                            //    if(amazon_processed && flipkart_processed){
                            //       //current_item.setInventory_updated_at(Timestamp.valueOf( LocalDateTime.parse( sync_before_utc.split("\\.|Z")[0] )));
                            //    }
                               session2.update( current_item );
                          }


                          if(writer!=null) {
                             writer.close();
                             writer=null;
                          }

                          if( outputfile != null){
                             outputfile.close();
                             outputfile = null;
                           }

                          log.setStatus("completed");
                          JSONArray log_description1 = new JSONArray(log.getDescription());
                          JSONObject log_add1= new JSONObject();
                          log_add1.put("key","event");
                          log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                          log_add1.put("value","Task Completed");
                          log_add1.put("details", items_details_log);
                          log_description1.put( log_add1 );
                          log.setDescription( log_description1.toString() );
                          log.setUpdated_at( Timestamp.valueOf( java.time.LocalDateTime.now() ) );

                          // Write CSV file only if it has at least one order
                        //   if( process_items.size() > 0 ){
                            this.uploadLogFile(client, csv_file);
                            JSONArray output_files_array = new JSONArray();
                            JSONObject output_file_object= new JSONObject();
                            output_file_object.put("gcp_file_name",csv_file.split("/")[1]);
                            output_file_object.put("gcp_log_folder_name", "logs");
                            output_file_object.put("gcp_task_folder_name",csv_file.split("/")[0]);

                            output_files_array.put( output_file_object );
                            log.setOutput_files( output_files_array.toString() );
                        //   }
                          
                          
                          log.setJob_running(false);
                          session2.persist( log );
                          if(local_folder != null){
                             FileUtils.deleteQuietly( new File(local_folder) );
                          }
                          Setting last_synced = session2.bySimpleNaturalId(Setting.class).load("amazon_flipkart_last_sync_time");
                          last_synced.setValue( sync_before.split("\\.|Z")[0].replace("T", " ") );
                          session2.update(last_synced);
                          FileUtils.deleteQuietly(new File(csv_file.split(File.separator)[0]));
                          this.LogMessageInfo("\n" + this.getClass().getName() + ": Shutting down API Cache Task");
                          this.stopInventoryUpdateJob(client_id, log_id, job_key, trigger_key);
                          tx3.commit();
                          tx3 = null;
                        }
                     }

                    }catch(ExpiredTokenException e){
                        if (tx2 != null){ 
                            tx2.rollback();
                            tx2 = null;
                        }
                        
                        if (tx3 != null){ 
                            tx3.rollback();
                            tx3 = null;
                        }

                        if(log!=null && log.getId()!=null) {
                            tx4 = session2.beginTransaction();
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log_add.put("value","Error : "+ e.getMessage() + " . will retry");

                            JSONArray log_description = new JSONArray(log.getDescription());
                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            log.setJob_running(false);
                            session2.update(log);
                            tx4.commit();
                            tx4 = null;
                        }

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
                            this.stopInventoryUpdateJob(client_id, log_id, job_key, trigger_key);
                        }
                    } 
                    catch (Exception e) {
                        

                        if (tx2 != null){ 
                            tx2.rollback();
                            tx2 = null;
                        }
                        if (tx3 != null){ 
                            tx3.rollback();
                            tx3 = null;
                        }

                        e.printStackTrace();

                        // Log client DB issue
                        if(log!=null && log.getId()!=null) {
                            tx4 = session2.beginTransaction();
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
                            session2.update(log);
                            tx4.commit();
                            tx4 = null;
                            if(local_folder != null){
                                FileUtils.deleteQuietly( new File(local_folder) );
                            }
                            this.stopInventoryUpdateJob(client_id, log_id, job_key, trigger_key);
                        }
                        else {
                            // Log error in system log file
                            this.LogMessageError(e.getMessage());
                        }


                    } finally {
                        // session.close();
                        // sessionFactory.close();
                        // session1.close();
                        // exception_session.close();
                        // exception_session_token.close();
                        // sessionFactory1.close();
                    }
                }
            }
            catch (Exception e) {
                if (tx1!=null) tx1.rollback();
                e.printStackTrace();
                this.LogMessageError("Primary DB issue : "+e.getMessage());

            } finally {
                if( tx1 != null ){
                    tx1.rollback();
                }
                if( tx2 != null ){
                    tx2.rollback();
                }
                if( tx3 != null ){
                    tx3.rollback();
                }
                if( tx4 != null ){
                    tx4.rollback();
                }
                if(session1 != null){
                   session1.close();
                }
                if(sessionFactory1 != null){
                   sessionFactory1.close();
                }
                if(session2 != null){
                    session2.close();
                }
                if(sessionFactory2 != null){
                   sessionFactory2.close();
                }
                System.out.println("<<<<<< Cleaning up Session >>>>>");
            }


        } catch (Exception e) {
            e.printStackTrace();
            // Store Exception for future ref & update message as failed
            this.LogMessageError("Error in message : "+ message+" :Error: "+ e.getMessage());
        }
    }

    public JSONObject stopInventoryUpdateJob(BigInteger client_id, BigInteger log_id,  String jobKey, String triggerKey) throws Exception{
            
        Environment env = this.getEnv();
        JSONObject json = new JSONObject();
        json.put("client_id", client_id);
        json.put("log_id", log_id);
        json.put("topic", "gcp.store.inventory.amazon_flipkart.sync.process.0");
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
            throw new Exception("Failed to start Amazon Product Cache process job");
        }
    }

    public JSONObject startInventoryUpdateJob(BigInteger client_id, BigInteger log_id ) throws Exception{

        Environment env = this.getEnv();
        JSONObject json = new JSONObject();
        json.put("client_id", client_id);
        json.put("log_id", log_id);
        json.put("topic", "gcp.store.inventory.amazon_flipkart.sync.process.0");
        json.put("min", this.INVENTORY_UPDATE_JOB_INTERVAL );
        json.put("start", true);
        System.out.println("<<<<<< Entered startInventoryUpdateJob() >>>>>");
        RequestBody body    = RequestBody.create( MediaType.parse("application/json"), json.toString());
        OkHttpClient client = new OkHttpClient();
        System.out.println("<<<<<< Sending Request to AUTOMATE_JOB_HOST/jobs/temporary-job >>>>>");
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

    public List<JSONObject> createJSONChunk(JSONObject items, String default_location, int chunksize) throws JSONException {
        List<JSONObject> chunk_list = new ArrayList<JSONObject>();
        int count  = 0;
        System.out.println("<<<<<< Started creating JSON chunks >>>>>");
        JSONObject chunk = new JSONObject();
        Iterator keys =  items.keys(); 
        int total_len = this.getLen( items );
        JSONArray sku_list = new JSONArray();
        keys = items.keys();
        while(keys.hasNext()){
            count++ ;
            String sku = (String) keys.next();
            JSONObject item     = items.getJSONObject( sku );
            String product_id   = item.has("flipkart_product_id")? item.getString("flipkart_product_id"): null;
            String flipkart_sku = item.has("flipkart_alias")? item.getString("flipkart_alias"): null;
            Integer qty = item.has("quantity")? Integer.valueOf(item.getInt("quantity")): 0;
            if(product_id !=null && !product_id.isEmpty()){
               chunk.put( ((flipkart_sku != null && !flipkart_sku.isEmpty())? flipkart_sku: sku) , 
                   new JSONObject()
                   .put("product_id", product_id )
                   .put("locations", 
                       new JSONArray()
                       .put( 
                           new JSONObject()
                          .put("id", default_location)
                          .put("inventory", qty ) 
                       )
                   )
               );
               sku_list.put( sku );
            }
            if( count % chunksize == 0){
                chunk_list.add(
                     new JSONObject()
                     .put("body", chunk)
                     .put("sku_list", sku_list)
                );
                chunk    = new JSONObject();
                sku_list = new JSONArray();
            }
        }

        if( (total_len % chunksize != 0) && (chunk.length() != 0)){
            System.out.println("<<<<<< Adding element to chunk_list >>>>>");
            chunk_list.add(
                new JSONObject()
                .put("body", chunk)
                .put("sku_list", sku_list)
           );
        }
        return chunk_list;
    }

    private void uploadLogFile(Client client, String file) throws IOException{
        BlobId blobId = BlobId.of( this.getEnv().getProperty("spring.gcp.bucket-name"), client.getName() + "/" + this.getEnv().getProperty("spring.gcp.log-folder-name") + "/" + file);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        this.getStorage().create(blobInfo, Files.readAllBytes(Paths.get( file )));
        FileUtils.deleteQuietly(new File( file ));
    }

    public List<Item> extractItems( JSONObject json_obj, List<Item> item_list){

         List<Item> search_item_list = new ArrayList<Item>();
         Iterator<String> sku_list = json_obj.keys();

         while (sku_list.hasNext()) {
            String sku = ( String ) sku_list.next();
            for (int i = 0; i < item_list.size(); i++) {
                  Item item = item_list.get(i);
                  if(sku.equalsIgnoreCase( item.getSku() ) || sku.equalsIgnoreCase( item.getFlipkart_sku_alias() ) || sku.equalsIgnoreCase( item.getAmazon_sku_alias() )){
                     search_item_list.add( item );
                     break;
                  }
            }
         }
         return search_item_list;
    }
    
    public String fileToString(String filepath) throws Exception{
        String content="", line;
        File file=new File(filepath);
        FileReader fileReader=new FileReader(file);
        BufferedReader bufferedReader =new BufferedReader(fileReader);
        while ( ( line = bufferedReader.readLine() ) != null) {
            content = content + line;
        }
        bufferedReader.close();
        fileReader.close();
        return content;
    }

    public JSONObject extractWithLimit(JSONObject items, int limit) throws Exception{

        JSONObject cutoff_items = new JSONObject();
        Iterator keys = items.keys();
        int count = 0 ;
        while (keys.hasNext()) {
            String sku        = (String) keys.next();
            JSONObject item   = items.getJSONObject( sku );
            Boolean processed = item.getBoolean("processed");
            if(processed == false){
                count++;
                cutoff_items.put( sku, item);
            }
            if(count != 0 && count % limit == 0){
                break;
            }
        }
        return cutoff_items;
    }

    public int getLen(JSONObject items){
        
         int len = 0;
         Iterator keys = items.keys();
         while ( keys.hasNext() ) {
             String sku = (String) keys.next();
             len++;
         }
         return len;
    }

    public List<String> getSkuList(JSONArray  json_array) throws JSONException{

        List<String> list = new ArrayList<String>();
        for (int i = 0; i < json_array.length(); i++) {
            list.add( json_array.getString(i) );
        }
        return list;
    }

    public String createXML(String marketplace_id, JSONObject items ) throws Exception{
        String xml_text = "";
        try {
            
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document document = documentBuilder.newDocument();
            Element root = document.createElement("AmazonEnvelope");
            Attr attr1 = document.createAttribute("xmlns:xsi");
            attr1.setValue("http://www.w3.org/2001/XMLSchema-instance");
            root.setAttributeNode(attr1);
            Attr attr2 = document.createAttribute("xsi:noNamespaceSchemaLocation");
            attr2.setValue("amzn-envelope.xsd");
            root.setAttributeNode(attr2);
            Element header= document.createElement("Header");
            Element merchantIdentifier=document.createElement("MerchantIdentifier");
            Element documentVersion= document.createElement("DocumentVersion");
            documentVersion.appendChild(document.createTextNode("1.01"));
            merchantIdentifier.appendChild( document.createTextNode( marketplace_id ) );
            header.appendChild(documentVersion);
            header.appendChild(merchantIdentifier);
            root.appendChild(header);
            Element messageType=document.createElement("MessageType");
            messageType.appendChild(document.createTextNode("Inventory"));
            root.appendChild(messageType);
            StringWriter writer = new StringWriter();

            Iterator keys = items.keys();
            int count  = 0;

            while( keys.hasNext() ){
                count++;
                String sku = (String) keys.next();
                JSONObject item = items.getJSONObject( sku );
                String amazon_sku = item.has("amazon_alias")? item.getString("amazon_alias"): null;
                String final_sku = amazon_sku != null? amazon_sku: sku;
                int qty = item.has("quantity")? item.getInt("quantity"): 0;
                Element message=document.createElement("Message");
                Element messageId=document.createElement("MessageID");
                Element operationType=document.createElement("OperationType");
                operationType.appendChild(document.createTextNode("PartialUpdate"));
                messageId.appendChild(document.createTextNode(String.valueOf(count)));
                Element inventory = document.createElement("Inventory");
                Element skuElement = document.createElement("SKU");
                skuElement.appendChild(document.createTextNode( final_sku ));
                Element quantity=document.createElement("Quantity");
                quantity.appendChild(document.createTextNode(String.valueOf(qty)));
                inventory.appendChild(skuElement);
                inventory.appendChild(quantity);
                message.appendChild(messageId);
                message.appendChild(operationType);
                message.appendChild(inventory);
                root.appendChild(message);
                sleep(100 * 1);
            }

            document.appendChild(root);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource domSource = new DOMSource(document);
            StreamResult streamResult = new StreamResult( writer );
            transformer.transform(domSource, streamResult);
            return writer.toString();

        }catch (ParserConfigurationException pce){
            pce.printStackTrace();
            throw  new Exception("parser config exception");
        }catch (TransformerException tfe){
            tfe.printStackTrace();
            throw new Exception("xml transform exception");
        }
    }
}
