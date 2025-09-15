package com.consumer.store;
import com.consumer.store.Exceptions.ExpiredTokenException;
import com.consumer.store.helper.GenerateRandomString;
import com.consumer.store.model.*;
import com.consumer.store.service.KafkaProducerService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.aspectj.weaver.ast.And;
import org.hibernate.Session;

import com.amazon.SellingPartnerAPIAA.AWSAuthenticationCredentials;
import com.amazon.SellingPartnerAPIAA.AWSSigV4Signer;
//hibernet classes
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
//time classes
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.sql.Timestamp;
//OkHttp
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

import net.bytebuddy.asm.Advice.Local;

import com.squareup.okhttp.MediaType;
//import com.squareup.okhttp;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Acl;
import com.google.gson.JsonObject;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.squareup.okhttp.HttpUrl;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import com.google.cloud.storage.*;

//GZIP
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import com.consumer.store.helper.TextReportProcessor;

import static java.lang.Thread.sleep;
import java.time.LocalDateTime;

@Service
public class ProductImportController extends StoreConsumer{


    private   int      API_CACHE_JOB_INTERVAL     = 5;
    private   int      CACHE_PROCESS_JOB_INTERVAL = 2;
    private   int      FIRST_RUN_DELAY = 1;
    private   int      MAX_DATA_INSERT      = 500;
    private   int      MAX_CACHE_PER_TASK   = 500;
    private   Storage  storage;


    @Autowired
    private Environment env;
    
    @Autowired
    private final KafkaProducerService kafkaproducer;

    public ProductImportController(KafkaProducerService kafkaproducer){
        super(kafkaproducer);
        this.kafkaproducer = kafkaproducer;
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
  
    public void LogMessageWarning(String message){
        LogManager lgmngr = LogManager.getLogManager();
        Logger lg = lgmngr.getLogger(Logger.GLOBAL_LOGGER_NAME);
        lg.log(Level.WARNING, message);
    }
    public void LogMessageInfo(String message){
        LogManager lgmngr = LogManager.getLogManager();
        Logger lg = lgmngr.getLogger(Logger.GLOBAL_LOGGER_NAME);
        lg.log(Level.INFO, message);
    }
    public void LogMessageError(String message){
        LogManager lgmngr = LogManager.getLogManager();
        Logger lg = lgmngr.getLogger(Logger.GLOBAL_LOGGER_NAME);
        lg.log(Level.SEVERE, message);
    }

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

    public Properties SetClientDB(String host, String port, String db_name, String user, String pass){
        Properties properties = new Properties();
        if(env.getActiveProfiles()[0]=="prod") {
            properties.put("hibernate.connection.url", "jdbc:mysql://" + host + ":" + port + "/" + db_name); // for production docker container
        }
        else {
            properties.put("hibernate.connection.url", "jdbc:mysql://"+env.getProperty("spring.db.host")+":" + env.getProperty("spring.db.port") + "/" + db_name);
        }
        properties.put("hibernate.connection.username", user);//Here your connection username
        properties.put("hibernate.connection.password", pass); //Here your connection password
        properties.put("show_sql",env.getProperty("spring.db.show"));
        properties.put("format_sql",env.getProperty("spring.db.format"));
        properties.put("use_sql_comments",env.getProperty("spring.db.comments"));
        properties.put("hibernate.jdbc.batch_size", 100);
        return properties;
    }


    private void uploadLogFile(Client client, String file) throws IOException{
        BlobId blobId = BlobId.of( this.getEnv().getProperty("spring.gcp.bucket-name"), client.getName() + "/" + this.getEnv().getProperty("spring.gcp.log-folder-name") + "/" + file);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        this.getStorage().create(blobInfo, Files.readAllBytes(Paths.get( file )));
    }


    @KafkaListener(topics="gcp.store.products.amazon.import.0", groupId="nano_group_id")
    public void AmazonProductImport(String message){

           try{
              String body = new JSONObject(message).getString("body");
              JSONObject json = new JSONObject(body);
              BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
              BigInteger log_id = BigInteger.valueOf(json.getInt("log_id"));
              String local_folder = null;
              String local_file = null;
              Configuration configuration = new Configuration();
              configuration.configure();
              configuration.setProperties(this.SetMasterDB());
              SessionFactory sessionFactory1 = null;
              Session session1 = null;
              Transaction tx1 = null;
              DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
              OkHttpClient amazon_client = new OkHttpClient();
              String report_id = null;
              String document_id = null;
              String document_download_url = null;
              String[] includedData = {"variations","attributes","productTypes","salesRanks","summaries","images"};
              String current_topic = "gcp.store.products.amazon.import.0";
              Boolean isGzip = false;
              String gzip_folder = this.getRandStr(10),gzip_input_file=this.getRandStr(10),gzip_output_file=this.getRandStr(10), img_folder = getRandStr(12);;
              byte[] buffer = new byte[2048];
              try{
                   sessionFactory1 = configuration.buildSessionFactory();
                   session1 = sessionFactory1.openSession();
                   tx1 = session1.beginTransaction();
                   Client client = session1.load(Client.class, client_id);
                   if(client == null){
                       throw new Exception("Client not valid");
                   }
                   tx1.commit();
                   configuration.setProperties(this.SetClientDB(client.getDb_host(), client.getDb_host(), client.getDb_name(), client.getDb_user(), client.getDb_pass()));
                   SessionFactory sessionFactory2 = null;
                   Session session2 = null;
                   Transaction tx2 = null;
                   Transaction tx3 = null;
                   Transaction tx4 = null;
                   try{
                      sessionFactory2 = configuration.buildSessionFactory();
                      session2 = sessionFactory2.openSession();
                      tx2 = session2.beginTransaction();
                      Log log = session2.load(Log.class, log_id);
                      if(log==null){
                          throw new Exception("Log is not valid");
                      }
                      JSONArray log_description = new JSONArray(log.getDescription());
                      JSONObject log_add = new JSONObject();
                      log.setStatus("started");
                      log_add.put("key", "event");
                      log_add.put("time",LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                      log_add.put("value", "Task Started");
                      log_description.put(log_add);
                      log.setDescription(log_description.toString());
                      log.setUpdated_at( Timestamp.valueOf(LocalDateTime.now()));
                      log.setProgress(new JSONObject().put("gzip_process",
                          new JSONObject()
                          .put("name", "Gzip Process")
                          .put("progress",
                              new JSONArray()
                              .put(
                                  new JSONObject()
                                  .put("status","started")
                                  .put("time", LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1))
                              )
                          )
                      ).toString());
                      JSONObject context = new JSONObject();
                      context.put("image_folder", getRandStr(12));
                      context.put("old_items", new JSONArray());
                      context.put("old_products", new JSONArray());

                      Query product_query = session2.createQuery("From Product");
                      List<Product> product_list = product_query.list();
                      Query item_query = session2.createQuery("From Item");
                      List<Item> item_list = item_query.list();
                      
                      for (Product product : product_list) {
                           context.getJSONArray("old_products")
                           .put(product.getAsin());
                      }
                      for (Item item : item_list) {
                           context.getJSONArray("old_items")
                          .put(item.getAsin());
                      }

                      log.setContext( context.toString() );
                      session2.update( log );
                      tx2.commit();
                      tx2 = null;
                     
                      Setting verified = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.verified"));
                      if(verified==null || verified.getValue().equals("0")) {
                          throw new Exception("Amazon Integration is not verified");
                      }
                      Setting active = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.active"));
                      if(active==null || active.getValue().equals("0")) {
                          throw new Exception("Amazon Integration is not activated");
                      }
                      Setting token = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.token_key"));
                      if(token==null || token.getValue().equals("0")) {
                          throw new Exception("Amazon auth token is not found");
                      }
                      Setting refresh_token = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.refresh_token_key"));
                      if(refresh_token==null || refresh_token.getValue().equals("0")) {
                          throw new Exception("Amazon auth refresh token is not found");
                      }
                      if(this.IsMaxTokenCreateFailWithCount("amazon", session2, true)){
                        this.StopSyncJob(session2, "AMAZON_ORDER_SYNC", false, client.getId(), true);
                        this.StopSyncJob(session2, "AMAZON_FLIPKART_INVENTORY_SYNC", false, client_id, true);
                        throw new Exception("Token creation failed max time. Sync Job Stopped");
                      }

                      tx3 =  session2.beginTransaction();
                      Setting expire_in = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.token_expire_in"));
                     //   System.out.println(expire_in);//rm
                     //   System.out.println(expire_in.getValue());//rm
                      if(expire_in==null || expire_in.getValue().equals("0")) {
                          
                          throw new Exception("Amazon expire in not found");
                      }

                      Setting expired = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.expired"));
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
                            
                            // System.out.println("generating new token");
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
                            session2.update(token);
                            expired.setValue("0");
                            expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session2.update(expired);
                            expire_in.setValue(token_expire_in.toString());
                            expire_in.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session2.update(expire_in);
                            

                        }
                        catch (Exception ex) {
                            tx3.rollback();
                            tx3 = null;
                            Transaction tx_exception = session2.beginTransaction();
                            expired.setValue("1");
                            expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session2.update(expired);
                            this.SetMaxTokenCreateFail("amazon", session2, false);
                            tx_exception.commit();
                            throw new Exception("Issue generating amazon token :" + ex.getMessage());
                        }
                      }

                      tx3.commit();
                      tx3 = null;

                      final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
                      com.squareup.okhttp.RequestBody createReportRequestBody = RequestBody.create( JSON,
                      new JSONObject()
                        .put("reportType", "GET_MERCHANT_LISTINGS_ALL_DATA")
                        .put("marketplaceIds" , new JSONArray().put(env.getProperty("spring.integration.amazon.marketplace_id")))
                        .toString()
                        );
  
                        com.squareup.okhttp.Request creatReportRequest = new Request.Builder()
                        .url(env.getProperty("spring.integration.amazon.feed_api_url")+"/reports/2021-06-30/reports")
                        .addHeader("x-amz-access-token",token.getValue())
                        .post(createReportRequestBody)
                        .build();
                        com.squareup.okhttp.Request createReportRequestSigned = this.getAwsSigned(creatReportRequest);
                        com.squareup.okhttp.Response createReportResponse = amazon_client.newCall(createReportRequestSigned).execute();
                        
                        if(createReportResponse.code()==403){
                            this.LogMessageInfo("token expired. will continue");
                            Transaction tx_exception = session2.beginTransaction();
                            expired.setValue("1");
                            expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session2.update(expired);
                            tx_exception.commit();
                            throw new Exception("403");
                      }if(createReportResponse.code()==404){
                            throw new Exception("404");
                      }
                      else if(createReportResponse.code()>=400 && createReportResponse.code()<500){
                           throw new Exception("400");
                      }else if(createReportResponse.code()!=202){
                           //System.out.println(createReportResponse.body().string());
                           throw new Exception("500");
                      }
                       //System.out.println(createReportResponse.body().string());
                      report_id = new JSONObject(createReportResponse.body().string()).getString("reportId");
                      for(int i=0; i < 15 ; i++ ){
                          try {
                             com.squareup.okhttp.Request getReportRequest =  new Request.Builder()
                            .url(env.getProperty("spring.integration.amazon.feed_api_url")+"/reports/2021-06-30/reports/"+report_id)
                            .addHeader("x-amz-access-token",token.getValue())
                            .build();
                             com.squareup.okhttp.Request getSignedReportRequest = this.getAwsSigned(getReportRequest);
                             com.squareup.okhttp.Response getReportResponse = amazon_client.newCall(getSignedReportRequest).execute();
                             if(getReportResponse.code()==403)
                             {  
                                this.LogMessageInfo("token expired. will continue");
                                Transaction tx_exception = session2.beginTransaction();
                                expired.setValue("1");
                                expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                session2.update(expired);
                                tx_exception.commit();
                                throw new Exception("403");
                             }else if(getReportResponse.code()==404){
                                 sleep(1000 * i);
                                 continue;
                             }else if(getReportResponse.code()>=400 && getReportResponse.code()<500){
                                 throw new Exception("400");
                             }else if(getReportResponse.code() != 200){
                                 throw new Exception("500");
                             }else{
                                 JSONObject get_report_json = new JSONObject(getReportResponse.body().string());
                                 String process_status= get_report_json.getString("processingStatus");
                                 if(!process_status.equals("DONE")){
                                     System.out.println("current process status: "+process_status);
                                     sleep(1000 * i);
                                     continue;
                                 }else{
                                    document_id = get_report_json.getString("reportDocumentId");
                                    break;
                                 }
                             }
                          } catch (UnknownHostException e) {
                              sleep(1000 * 5);
                              i--;
                              continue;
                          }
                      }
                      for (int i = 0; i < 15; i++) {
                          try {
                                com.squareup.okhttp.Request getDocumentRequest = new Request.Builder()
                               .url(env.getProperty("spring.integration.amazon.feed_api_url")+"/reports/2021-06-30/documents/"+document_id)
                               .addHeader("x-amz-access-token",token.getValue())
                               .build();
                               com.squareup.okhttp.Request getSignedDocumentRequest = this.getAwsSigned(getDocumentRequest);
                               com.squareup.okhttp.Response getDocumentResponse = amazon_client.newCall(getSignedDocumentRequest).execute();
                               if(getDocumentResponse.code()==403)
                               {  //System.out.println(getReportResponse.body().string());
                                   this.LogMessageInfo("token expired. will continue");
                                   Transaction tx_exception = session2.beginTransaction();
                                   expired.setValue("1");
                                   expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                   session2.update(expired);
                                   tx_exception.commit();
                                   throw new Exception("403");
                               }else if(getDocumentResponse.code()==404){
                                   throw new Exception("404");
                               }else if(getDocumentResponse.code()>=400 && getDocumentResponse.code()<500){
                                   throw new Exception("400");
                               }else if(getDocumentResponse.code() != 200){
                                   throw new Exception("500");
                               }else{
                                   JSONObject document_download_resp = new JSONObject(getDocumentResponse.body().string());
                                   document_download_url = document_download_resp.getString("url");
                                   if(document_download_resp.has("compressionAlgorithm") && document_download_resp.getString("compressionAlgorithm").toLowerCase().equals("gzip")){
                                     isGzip = true;
                                   }
                                   break;
                                  
                               }
                          } catch (UnknownHostException e) {
                              sleep(1000 * 5);
                              i--;
                              continue;
                          }
                      }
                       
                      String text="";
                      if(isGzip){
                          this.LogMessageInfo("Gzip processing");
                          if(this.downloadFile(document_download_url, File.separator +  gzip_folder + File.separator + gzip_input_file)){
                               GZIPInputStream  gis = new GZIPInputStream(new FileInputStream( File.separator + gzip_folder + File.separator + gzip_input_file));
                               FileOutputStream fos = new FileOutputStream( File.separator + gzip_folder + File.separator + gzip_output_file);
                               int len;
                               while((len = gis.read(buffer)) != -1){
                                  fos.write(buffer, 0, len);
                               }
                               fos.close();
                               gis.close();
                               text = new String( Files.readAllBytes(Paths.get(gzip_folder + File.separator + gzip_output_file)));
                               //System.out.println(text);
                          }
                      }else{
                          for (int i = 0; i < 15; i++) {
                              try {  
                                 com.squareup.okhttp.Request doucmentDownloadRequest =  new Request.Builder()
                                 .url(document_download_url)
                                 .build();
                                 com.squareup.okhttp.Response documentDownloadResponse = amazon_client.newCall(doucmentDownloadRequest).execute();
                                 text  = documentDownloadResponse.body().string();
                                 if(documentDownloadResponse.code() == 403){
                                    this.LogMessageInfo("token expired. will continue");
                                    Transaction tx_exception = session2.beginTransaction();
                                    expired.setValue("1");
                                    expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    session2.update(expired);
                                    tx_exception.commit();
                                    throw new Exception("403");
                                 }
                              } catch (UnknownHostException e) {
                                  sleep(1000 * 5);
                                  i--;
                                  continue;
                              }
                          }
                      }
                      
                      //cache all text file data to table
                      tx4 = session2.beginTransaction();
                      
                      TextReportProcessor trProcessor = new TextReportProcessor( text );
                      List<AmazonProductCache> cache_list = new ArrayList<AmazonProductCache>();
                      for (int i = 0; i < trProcessor.length(); i++) {

                               AmazonProductCache cache = new AmazonProductCache();
                               String item_name = trProcessor.get(i).column("item-name");
                               String item_desc = trProcessor.get(i).column("item-description");
                               String amazon_listing_id = trProcessor.get(i).column("listing-id");
                               String item_sku   = trProcessor.get(i).column("seller-sku");
                               String item_price = trProcessor.get(i).column("price");
                               String item_quantity = trProcessor.get(i).column("quantity");
                               String item_mrp      = trProcessor.get(i).column("maximum-retail-price");
                               String item_status   = trProcessor.get(i).column("status");
                               String item_asin     = trProcessor.get(i).column("asin1");
                               String item_launch_date = trProcessor.get(i).column("open-date");
                               Double price  = Double.valueOf("0.0");
                               Double mrp    = Double.valueOf("0.0");
                               Integer quantity = Integer.valueOf("0");
                               Timestamp launch_date = null;

                               if(item_price != null && !item_price.trim().isEmpty()){
                                  price = Double.valueOf( item_price.trim() );
                               }
                               if(item_mrp !=null && !item_mrp.trim().isEmpty()){
                                  mrp   = Double.valueOf( item_mrp.trim() );
                               }
                               if(item_quantity != null && !item_quantity.trim().isEmpty()){
                                  quantity = Integer.valueOf( item_quantity.trim() );
                               }
                               if(item_launch_date != null && !item_launch_date.trim().isEmpty()){
                                  LocalDateTime launch_date_ldt =  LocalDateTime.parse(item_launch_date.replace("IST", "").trim(),formatter1)
                                                                  .minusHours(5)
                                                                  .minusMinutes(30);
                                  launch_date =  Timestamp.valueOf(launch_date_ldt);
                               }

                            cache.setName(item_name);
                            cache.setDescription(item_desc);
                            cache.setAsin(item_asin);
                            cache.setSku(item_sku);;
                            cache.setMrp(mrp);;
                            cache.setPrice( price );
                            cache.setQuantity(quantity);
                            cache.setStatus( item_status);
                            cache.setAmazon_listing_id(amazon_listing_id);
                            cache.setLaunch_date( launch_date );
                            cache.setProcessed(false);
                            cache.setLog_id( log_id );
                            cache.setIs_outside(false);
                            session2.persist( cache );
                          
                      }

                    //   cache all text file data to table

                    JSONObject  log_progress    = new JSONObject(log.getProgress());
                    JSONObject  gzip_progress   = log_progress.getJSONObject("gzip_process");
                    JSONArray   progress_status = gzip_progress.getJSONArray("progress");
                    progress_status.put(
                         new JSONObject()
                        .put("status", "completed")
                        .put("time", 
                            LocalDateTime.now(ZoneId.of("Asia/Kolkata"))
                           .format(formatter1)
                         )
                    );

                    gzip_progress.put("progress", progress_status);
                    log_progress.put("gzip_process", gzip_progress);
                    log.setProgress(log_progress.toString());
                    session2.update(log);
                    tx4.commit();
                    tx4=null;
                    this.startAmazonProductCacheJob(client_id,log_id);
                    this.LogMessageInfo("Amazon product import Gzip process completed");
                       

                   }catch(Exception e){
                       e.printStackTrace();
                       if(tx2!=null)  tx2.rollback();
                       if(tx4!=null)  tx4.rollback();
                       if(tx3!=null)  tx3.rollback();
                       Transaction tx5 = session2.beginTransaction();
                       Log log = session2.load(Log.class, log_id);
                       JSONArray log_description = new JSONArray(log.getDescription());
                       JSONObject log_add = new JSONObject();
                       log_add = new JSONObject();
                       log_add.put("key", "event");
                       log_add.put("time",LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                       if(e.getMessage().equals("403") ){
                            log.setStatus("re-queued");
                            log_add.put("value","Info : Re-queue job due to Amazon token expired");
                       }else if(e.getMessage().equals("400")){
                            log.setStatus("failed");
                            log_add.put("value","Error : Amazon api bad request");
                       }else if(e.getMessage().equals("404")){
                            log.setStatus("failed");
                            log_add.put("value","Error : Amazon api unavailable");
                       }else{
                            log.setStatus("failed");
                            log_add.put("value","Error : "+e.getMessage());
                       }
                       log_description.put(log_add);
                       log.setDescription(log_description.toString());
                       log.setUpdated_at(Timestamp.valueOf(LocalDateTime.now()));
                       session2.update(log);
                       tx5.commit();
                       if(e.getMessage().equals("403")) {
                         kafkaproducer.sendMessage(message,current_topic);
                         System.out.println("Sent new message to kafka "+ message);
                       }
                   }finally{
                       if(session2 != null){
                          session2.close();
                       }
                       if(sessionFactory2 != null){
                          sessionFactory2.close();
                       }
                       if(session1!=null){
                         session1.close();
                       }
                       if(sessionFactory1 != null){
                         sessionFactory1.close();
                       }
                       FileUtils.deleteQuietly(new File(gzip_folder));
                       //FileUtils.deleteQuietly(new File(img_folder));
                      
                       
                   }
              }catch(Exception e){
                      e.printStackTrace();
                      if(session1!=null){
                         session1.close();
                      }
                      if(sessionFactory1 != null){
                        sessionFactory1.close();
                      }
                      this.LogMessageError("primary DB Issue: "+e.getMessage());
              }
           }catch(Exception e){
               //e.printStackTrace();
               this.LogMessageError("Error: "+e.getMessage());


           }
    }

   
   @KafkaListener(topics="gcp.store.products.amazon.import.cache_process.0", groupId="nano_group_id")
   public void AmazonProductImportCacheProcess(String message){
   

          this.LogMessageInfo(this.getClass().getName() + " Cache Process Started");
          SessionFactory sessionFactory1 = null, sessionFactory2 = null;
          Session     session1 = null,  session2 = null;
          Transaction tx1 = null, tx2 = null, tx3 = null, tx4 = null;
          String image_folder = null;

          try {
              String body = new JSONObject(message).getString("body");
              JSONObject json = new JSONObject(body);
              BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
              BigInteger log_id  = BigInteger.valueOf( json.getInt("log_id") );
              String job_key     = json.getString("job_key");
              String trigger_key = json.getString("trigger_key");
              String topic       = json.getString("topic");
              int min            = json.getInt("min");
      
              Configuration configuration = new Configuration();
              configuration.configure();
              configuration.setProperties(this.SetMasterDB());
              sessionFactory1 = configuration.buildSessionFactory();
              session1 = sessionFactory1.openSession();
              tx1 = session1.beginTransaction();
              Client client = session1.load(Client.class, client_id);
              OkHttpClient amazon_client = new OkHttpClient();
              this.setStorage();
      
              if(client == null){
                  throw new Exception("Client is not valid");
              }
              tx1.commit();
              tx1=null;
      
              try {
                  configuration.setProperties(this.SetClientDB(client.getDb_host(), client.getDb_host(), client.getDb_name(), client.getDb_user(), client.getDb_pass()));
                  sessionFactory2 = configuration.buildSessionFactory();
                  session2 = sessionFactory2.openSession();
                  tx2      = session2.beginTransaction();

                  Log log  = session2.load( Log.class, log_id);
                  if( log == null){
                      throw new Exception("Log is not valid");
                  }

                  tx2.commit();
                  tx2 = null;
                  
                 try{

                  JSONObject log_context = new JSONObject( log.getContext());
                  image_folder = log_context.getString("image_folder");
                  tx3 = session2.beginTransaction();
                      
                  String product_cache_hql   = "From AmazonProductCache APC WHERE APC.processed = 0 AND APC.product_type=:product_type AND APC.log_id=:log_id ORDER BY APC.id ASC";
                  Query  product_cache_query = session2.createQuery( product_cache_hql );
                  product_cache_query.setParameter("product_type", "product");
                  product_cache_query.setParameter("log_id", log.getId());
                  product_cache_query.setFirstResult(0);
                  product_cache_query.setMaxResults( this.MAX_DATA_INSERT );
                  List<AmazonProductCache> product_cache_list = product_cache_query.list();

                  List<String> old_product_asins = new ArrayList<String>();
                  List<String> old_item_asins = new ArrayList<String>();

                  JSONArray items_asins_json     = log_context.getJSONArray("old_items");
                  JSONArray products_asins_json  = log_context.getJSONArray("old_products");

                  for (int i = 0; i < products_asins_json.length(); i++) {
                      String sku = products_asins_json.getString(i);
                      old_product_asins.add( sku );
                  }

                  for (int i = 0; i < items_asins_json.length(); i++) {
                      String sku = items_asins_json.getString(i);
                      old_item_asins.add( sku );
                  }

                //   Query new_products_query =  session2
                //                              .createQuery("From Product P where P.asin NOT IN (:asin_list)")
                //                              .setParameterList("asin_list", old_product_asins);
                //   Query new_items_query =  session2
                //                           .createQuery("From Item I where I.asin NOT IN (:asin_list)")
                //                           .setParameterList("asin_list", old_item_asins);
                //   Query old_products_query = session2
                //                             .createQuery("From Product P where P.asin IN (:asin_list)")
                //                             .setParameterList("asin_list", old_product_asins);;
                //   Query old_items_query =  session2
                //                           .createQuery("From Item I where I.asin IN (:asin_list)")
                //                           .setParameterList("asin_list", old_item_asins);
                  Query all_items_query = session2.createQuery("From Item");

                  Query all_product_query = session2.createQuery("From Product");

                //   List<Product>  old_products  = old_products_query.list();
                //   List<Item>     old_items     = old_items_query.list();
                //   List<Product>  new_products  = new_products_query.list();
                //   List<Item>     new_items     = new_items_query.list();
                  List<Item>     all_items     = all_items_query.list();
                  List<Product>  all_products  = all_product_query.list();
                  //List<Product>  all_products  = Stream.concat( new_products.stream(), old_products.stream()).collect(Collectors.toList());
                  //List<Item>     all_items     = Stream.concat( new_items.stream(), old_items.stream()).collect(Collectors.toList());
                  ;

                  Query total_cache_query = session2.createQuery("From AmazonProductCache APC WHERE APC.log_id=:log_id");
                  total_cache_query.setParameter("log_id", log.getId());
                  List<AmazonProductCache> total_cache_list = total_cache_query.list();
                  Set<String> asins_chache = new HashSet<String>();

                  List<String> duplicate_asins =  total_cache_list
                                                 .stream()
                                                 .filter((AmazonProductCache cache) -> !asins_chache.add( cache.getAsin() ))
                                                 .map((AmazonProductCache cache) -> cache.getAsin() )
                                                 .collect( Collectors.toList() );

                //   String product_hql   = "From Product";
                //   Query  product_query = session2.createQuery( product_hql );
                //   List<Product> product_list = product_query.list();

                //   String item_hql   = "From Item";
                //   Query  item_query = session2.createQuery( cache_hql );
                //   List<AmazonProductCache> item_list = item_query.list();
                  //System.out.println("new products: " + new_products.size() + " new items: " + new_items.size());
                  //System.out.println("old products: " + old_products.size() + " new items: " + old_items.size());

                  List<Category> sub_category_list = new ArrayList<Category>();
                  List<Brand> brand_list = new ArrayList<Brand>();
                  List<Manufacturer> manufacturer_list = new ArrayList<Manufacturer>();
                  List<Country> country_list = new ArrayList<Country>();

                  if( log_context.has("stored_others") && 
                      log_context.getBoolean("stored_others") == true
                  ){
                       String sub_category_hql   = "From Category SCG WHERE SCG.parent_category_id != null";
                       Query  sub_category_query = session2.createQuery(sub_category_hql);
                       sub_category_list = sub_category_query.list();
     
                       String brand_hql       =  "From Brand";
                       Query  brand_query     =  session2.createQuery(brand_hql);
                       brand_list =  brand_query.list();
     
                       String manufacturer_hql = "From Manufacturer MR";
                       Query  manufacturer_query = session2.createQuery( manufacturer_hql );
                       manufacturer_list = manufacturer_query.list();
     
                       String country_hql   = "From Country";
                       Query  country_query = session2.createQuery(country_hql);
                       country_list = country_query.list();
                      
                  }else{

                       String all_cache_hql = "From AmazonProductCache APC WHERE APC.product_type in ('product','single') AND APC.log_id="+ log.getId() +" ORDER BY APC.id ASC";
                       Query all_cache_query = session2.createQuery(all_cache_hql);
                       List<AmazonProductCache> all_cache_list = all_cache_query.list();

                       String sub_category_hql   = "From Category SCG WHERE SCG.parent_category_id != null";
                       Query  sub_category_query = session2.createQuery(sub_category_hql);
                       sub_category_list = sub_category_query.list();
     
                       String brand_hql       =  "From Brand";
                       Query  brand_query     =  session2.createQuery(brand_hql);
                       brand_list =  brand_query.list();
     
                       String manufacturer_hql = "From Manufacturer MR";
                       Query  manufacturer_query = session2.createQuery( manufacturer_hql );
                       manufacturer_list = manufacturer_query.list();
     
                       String country_hql   = "From Country";
                       Query  country_query = session2.createQuery(country_hql);
                       country_list = country_query.list();

                    for(int k=0;k < all_cache_list.size(); k++){
                        AmazonProductCache cache = all_cache_list.get(k);
                        JSONObject json_response = new JSONObject( cache.getApi_response() );
                        String brand_name        = this.jsonToBrand( json_response );
                        String sub_category_name = this.jsonToSubCategory( json_response );
                        String country_name      = this.jsonToCountry( json_response );
                        String manufacturer_name = this.jsonToManufacturer( json_response );
   
                        Brand        search_brand        = this.searchBrand(brand_list, brand_name);
                        Category     search_sub_category = this.searchSubCategory( sub_category_list, sub_category_name);
                        Country      search_country      = this.searchCounrty( country_list, country_name );
                        Manufacturer search_manufacturer = this.searchManuFacturer( manufacturer_list, manufacturer_name);
   
                        if( search_brand == null ){
                             Brand new_brand = new Brand();
                             new_brand.setName( brand_name);
                             session2.persist( new_brand );
                             brand_list.add( new_brand );
                        }
   
                        if( search_manufacturer == null ){
                           Manufacturer new_manufacturer = new Manufacturer();
                           new_manufacturer.setName( manufacturer_name );
                           session2.persist( new_manufacturer );
                           manufacturer_list.add( new_manufacturer );
                        }
   
                        if( search_sub_category == null ){
                           Category new_category = new Category();
                           new_category.setName(sub_category_name);
                           session2.persist( new_category );
                           Category new_sub_category = new Category();
                           new_sub_category.setParent_category_id( new_category.getId() );
                           new_sub_category.setName( sub_category_name );
                           session2.persist( new_sub_category );
                           sub_category_list.add( new_sub_category );
                        }
    
                        if( search_country == null){
                           Country new_country = new Country();
                           new_country.setName( country_name );
                           session2.persist( new_country );
                           country_list.add( new_country );
                        }
   
                     }
                    log_context.put("stored_others", true);
                  }

                //   List<String> new_brands         = new ArrayList<String>();
                //   List<String> new_sub_categories = new ArrayList<String>();
                //   List<String> new_manufacturers  = new ArrayList<String>();
                //   List<String> new_countries      = new ArrayList<String>();

                  

                  tx3.commit();
                  tx3 = null;
                  
                  
                  JSONArray outputJsonArray = log.getCsv_buffer() == null? 
                  new JSONArray(): 
                  new JSONArray( log.getCsv_buffer());
               
                  tx4 = session2.beginTransaction();
                  if(product_cache_list.size() != 0 ){
            
                    for(int k=0;k <  product_cache_list.size(); k++){
                        
                           AmazonProductCache cache = product_cache_list.get(k);
                           //System.out.println("product: " + cache.getSku());
                           JSONObject json_response = new JSONObject( cache.getApi_response() );
                           String asin = cache.getAsin();
                           String sku  = cache.getSku() != null? cache.getSku(): this.jsonToSku(json_response);
                           String amazon_listing_id =  cache.getAmazon_listing_id() != null? cache.getAmazon_listing_id(): null;
                           String description       =  cache.getDescription() != null? cache.getDescription(): null;
                           String product_name      =  cache.getName() != null? cache.getName(): null;

                           Timestamp launch_date    =  cache.getLaunch_date();
                           description  =  !this.isBlank(description )? description: this.jsonToDesciption( json_response );
                           product_name =  !this.isBlank(product_name)? product_name:this.jsonToName( json_response );
  
                           String brand_name         = this.jsonToBrand( json_response );
                           String sub_category_name  = this.jsonToSubCategory( json_response );
                           String country_name       = this.jsonToCountry( json_response );
                           String manufacturer_name  = this.jsonToManufacturer( json_response );
  
                           Brand brand               = this.searchBrand( brand_list,  brand_name);
                           Category sub_category     = this.searchSubCategory( sub_category_list, sub_category_name );
                           Country country           = this.searchCounrty( country_list, country_name);
                           Manufacturer manufacturer = this.searchManuFacturer( manufacturer_list, manufacturer_name );
                           String pictures = this.jsonToPicture( json_response , client, log);
                           String ranks = this.jsonToRank( json_response );


                           try {
  
                           Product old_product =  all_products
                                                 .stream()
                                                 .filter(( Product product ) -> ((product.getAsin() !=null && product.getAsin().equals(asin)) || (product.getSku() !=null && product.getSku().equals(sku))))
                                                 .findAny()
                                                 .orElse(null);
                           if(duplicate_asins.contains(asin)){
                              cache.setProcessed(true);
                              session2.update(cache);
                              continue;
                           }

                           if( old_product != null ){

                                 //  List<Product> same_sku_products =  all_products
                                //                             .stream()
                                //                             .filter(( Product product ) -> ( product.getSku() !=null && product.getSku().equals(sku) ) )
                                //                             .collect(Collectors.toList());
                                // if(same_sku_products.size() != 0){

                                //       if(cache.getIs_outside() == true){
                                //         old_product.setSku(null);
                                //       }

                                //       if(cache.getIs_outside() == false){
                                //          for (Product matched_product : same_sku_products) {
                                //              matched_product.setSku(null);
                                //              session2.update(matched_product);
                                //          }
                                //       }
                                //  }

                                //  if(same_sku_products.size() == 0){
                                     
                                //  }
                                 old_product.setSku(sku);
                                 old_product.setName( product_name );
                                 old_product.setAsin(asin);
                                 old_product.setDescription( description );
                                 old_product.setBrand( brand );
                                 old_product.setCategory( sub_category );
                                 old_product.setManufacturer( manufacturer );
                                 old_product.setCountry( country );
                                 old_product.setPictures(pictures);
                                 old_product.setAmazon_listing_id( amazon_listing_id );
                                 old_product.setRank( ranks );
                                 old_product.setActive(Byte.parseByte("1"));
                                 old_product.setLaunch_date(launch_date);
                                 session2.update( old_product );
                                 cache.setProcessed(true);
                                 session2.update(cache);
                                 if(k % 20 == 0){
                                    session2.flush();
                                    session2.clear();
                                 }
                                 outputJsonArray.put( 
                                              new JSONObject()
                                              .put("asin", asin)
                                              .put("sku",  sku)
                                              .put("price", "")
                                              .put("quantity", "")
                                              .put("amazon_listing_id", amazon_listing_id)
                                              .put("variation", "product")
                                              .put("status", "successfully updated")
                                 );

                            }else{
                                
                                Product product = new Product();
                                product.setSku(sku);
                                product.setAsin( asin );
                                product.setName( product_name );
                                product.setDescription( description );
                                product.setBrand( brand );
                                product.setCategory( sub_category );
                                product.setManufacturer( manufacturer );
                                product.setCountry( country );
                                product.setPictures(pictures);
                                product.setAmazon_listing_id( amazon_listing_id );
                                product.setRank( ranks );
                                product.setActive(Byte.parseByte("1"));
                                product.setLaunch_date(launch_date);
                                session2.save( product );
                                cache.setProcessed(true);
                                session2.update(cache);

                                if(k % 20 == 0){
                                   session2.flush();
                                   session2.clear();
                                }
                                //product_asins.add( asin );
                                outputJsonArray.put( 
                                              new JSONObject()
                                              .put("asin", asin)
                                              .put("sku",  sku)
                                              .put("price", "")
                                              .put("quantity", "")
                                              .put("amazon_listing_id", amazon_listing_id)
                                              .put("variation", "product")
                                              .put("status", "successfully created")
                                );
                            }
                              
                          } catch (Exception dbex) {
                             dbex.printStackTrace();
                             outputJsonArray.put( 
                                              new JSONObject()
                                              .put("asin", asin)
                                              .put("sku",  sku)
                                              .put("price", "")
                                              .put("quantity", "")
                                              .put("amazon_listing_id", amazon_listing_id)
                                              .put("variation", "product")
                                              .put("status", "failed: " + dbex.getMessage())
                             );
                          }
                         }
                         this.LogMessageInfo(this.getClass().getName() + " Cache Process Completed");

                     }else{

                          Query item_cache_query = session2.createQuery("From AmazonProductCache APC WHERE APC.processed = 0 AND APC.product_type=:product_type AND APC.log_id=:log_id ORDER BY APC.id ASC");
                          item_cache_query.setParameter("product_type", "item");
                          item_cache_query.setParameter("log_id", log.getId());
                          item_cache_query.setFirstResult(0);
                          item_cache_query.setMaxResults(this.MAX_DATA_INSERT);

                          List<AmazonProductCache> item_cache_list = item_cache_query.list();

                        //   String product_hql   = "From Product";
                        //   Query  product_query = session2.createQuery( product_hql );
                        //   List<Product> product_list = product_query.list();

                        //   String item_hql   = "From Item";
                        //   Query  item_query = session2.createQuery( item_hql );
                        //   List<Item> item_list = item_query.list();

                        //   List<String> processed_items_asin = this.getAsinListofItems( item_list );


                          if(item_cache_list.size() != 0){
                                  for (int k = 0; k < item_cache_list.size(); k++) {
                                    
                                  
                                          AmazonProductCache item_cache = item_cache_list.get( k );
  
                                          String asin  = item_cache.getAsin() ;
                                          String sku   = item_cache.getSku()  ;
                                          Double mrp   = item_cache.getMrp() != null? item_cache.getMrp(): 0.0d;
                                          Double price = item_cache.getPrice() != null? item_cache.getPrice(): 0.0d;
                                          Integer quantity         = item_cache.getQuantity() != null? item_cache.getQuantity(): 0;
                                          JSONObject json_response = new JSONObject( item_cache.getApi_response() ); 
                                          String dimension_unit    = this.jsonToDimensionUnit( json_response );
                                          String amazon_listing_id = item_cache.getAmazon_listing_id();
                                          String ranks = this.jsonToRank( json_response );
                                          
        
                                          String parent_asin = new JSONArray( item_cache.getRelation() )
                                                               .getString(0);
        
                                          String variants = this.jsonToVariants( json_response );
                                          Double weight   = this.jsonToWidth(  json_response );
                                          Double length   = this.jsonToLength( json_response );
                                          Double height   = this.jsonToHeight( json_response );
                                          Double width    = this.jsonToWidth( json_response );
        
                                          String dimention = "" + length + "*" + width + "*" + height;
                                          String weight_unit =  this.jsonToWightUnit( json_response );
                                          Product product = this.findProductByAsin(all_products, parent_asin);
                                          try{
                                          
                                          Item old_item =  all_items
                                                          .stream()
                                                          .filter(( Item item ) -> ((item.getAsin() !=null && item.getAsin().equals(asin)) || (item.getSku() !=null && item.getSku().equals(sku))))
                                                          .findAny()
                                                          .orElse(null);
                                          if(duplicate_asins.contains(asin)){
                                            item_cache.setProcessed(true);
                                            session2.update( item_cache );
                                            continue;
                                          }
      
                                          if( old_item != null ){  
                                              old_item.setMrp( mrp );
                                              old_item.setPrice( price );
                                              old_item.setQuantity( quantity );
                                              old_item.setSku(sku);
                                              old_item.setAsin(asin);
                                              old_item.setDimentions( dimention );
                                              old_item.setDimension_unit( dimension_unit );
                                              old_item.setVariants( variants );
                                              old_item.setProduct( product );
                                              old_item.setAmazon_listing_id( amazon_listing_id );
                                              old_item.setWeight( weight.toString() );
                                              old_item.setWeight_unit( weight_unit );
                                              old_item.setSync_amazon( Byte.parseByte("1") );
                                              old_item.setRank( ranks );
                                              old_item.setActive(Byte.parseByte("1"));
                                              old_item.setFlipkart_active_listing(Byte.parseByte("0"));
                                              old_item.setInventory_type("0");
                                              old_item.setSync_flipcart(Byte.parseByte("0"));
                                              old_item.setAsin(asin);
                                              old_item.setNewly_added(false);
                                              session2.update( old_item );
                                              item_cache.setProcessed(true);
                                              session2.update( item_cache );
                                              if(k % 20 == 0){
                                                  session2.flush();
                                                  session2.clear();
                                              }
                                              outputJsonArray.put( 
                                              new JSONObject()
                                                  .put("asin", asin)
                                                  .put("sku",  sku)
                                                  .put("price", price)
                                                  .put("quantity", quantity)
                                                  .put("amazon_listing_id", amazon_listing_id)
                                                  .put("variation", "item")
                                                  .put("status", "successfully updated")
                                                  .put("parent_asin", product.getAsin())
                                              );
                                      }else{
                                          Item item = new Item();
                                          item.setSku(sku);
                                          item.setMrp( mrp );
                                          item.setPrice( price );
                                          item.setQuantity( quantity );
      
                                          item.setDimentions( dimention );
                                          item.setDimension_unit( dimension_unit );
                                          item.setVariants( variants );
                                          item.setProduct( product );
                                          item.setAmazon_listing_id( amazon_listing_id );
                                          item.setWeight( weight.toString() );
                                          item.setWeight_unit( weight_unit );
                                          item.setSync_amazon( Byte.parseByte("1") );
                                          item.setRank( ranks );
                                          item.setActive(Byte.parseByte("1"));
                                          item.setFlipkart_active_listing(Byte.parseByte("0"));
                                          item.setInventory_type("0");
                                          item.setSync_flipcart(Byte.parseByte("0"));
                                          item.setAsin(asin);
                                          item.setNewly_added(true);
                                          session2.save( item );
      
                                          item_cache.setProcessed(true);
                                          session2.update( item_cache );
      
                                          if(k % 20 == 0){
                                              session2.flush();
                                              session2.clear();
                                          }
                                          outputJsonArray.put( 
                                                    new JSONObject()
                                                   .put("asin", asin)
                                                   .put("sku",  sku)
                                                   .put("price", price)
                                                   .put("quantity", quantity)
                                                   .put("amazon_listing_id", amazon_listing_id)
                                                   .put("variation", "item")
                                                   .put("status", "successfully created")
                                                   .put("parent_asin", product.getAsin())
                                          );
      
                                      }
                                } catch (Exception dbex) {
                                     dbex.printStackTrace();
                                     outputJsonArray.put( 
                                                    new JSONObject()
                                                   .put("asin", asin)
                                                   .put("sku",  sku)
                                                   .put("price", price)
                                                   .put("quantity", quantity)
                                                   .put("amazon_listing_id", amazon_listing_id)
                                                   .put("variation", "item")
                                                   .put("status", "failed: " + dbex.getMessage())
                                                   .put("parent_asin", product.getAsin())
                                    );
                                }
                            }
                          }else{

                               Query single_cache_query = session2.createQuery("From AmazonProductCache APC WHERE APC.processed = 0 AND APC.product_type=:product_type AND APC.log_id=:log_id ORDER BY APC.id ASC");
                               single_cache_query.setParameter("product_type", "single");
                               single_cache_query.setParameter("log_id", log.getId());
                               
                               List<AmazonProductCache> single_cache_list = single_cache_query.list();

                               //List<String> processed_products_asin = this.getAsinListOfProducts( product_list );

                               if(single_cache_list.size() != 0 ){
                                     for (int k = 0; k < single_cache_list.size(); k++) {
                            
                                            AmazonProductCache cache = single_cache_list.get(k);
                                            String asin = cache.getAsin();
                                            String sku  = cache.getSku();
                                            String description       =  cache.getDescription() != null? cache.getDescription(): null;
                                            String product_name      =  cache.getName() != null? cache.getName(): null;
                 
                                            
                 
                                            Timestamp launch_date    =  cache.getLaunch_date();
                                            String amazon_listing_id =  cache.getAmazon_listing_id() != null? cache.getAmazon_listing_id(): null;
                                            JSONObject json_response = new JSONObject( cache.getApi_response() );

                                            description  =  !this.isBlank(description )? description: this.jsonToDesciption( json_response );
                                            product_name =  !this.isBlank(product_name)? product_name:this.jsonToName( json_response );
                   
                                            String brand_name         = this.jsonToBrand( json_response );
                                            String sub_category_name  = this.jsonToSubCategory( json_response );
                                            String country_name       = this.jsonToCountry( json_response );
                                            String manufacturer_name  = this.jsonToManufacturer( json_response );
                   
                                            Brand brand               = this.searchBrand( brand_list,  brand_name);
                                            Category sub_category     = this.searchSubCategory( sub_category_list, sub_category_name );
                                            Country country           = this.searchCounrty( country_list, country_name);
                                            Manufacturer manufacturer = this.searchManuFacturer( manufacturer_list, manufacturer_name );
                                            String pictures = this.jsonToPicture( json_response , client, log);
                                            String ranks = this.jsonToRank( json_response );
      
                                            Double  mrp   = cache.getMrp() != null? cache.getMrp(): 0.0d;
                                            Double  price = cache.getPrice() != null? cache.getPrice(): 0.0d;
                                            Integer quantity         = cache.getQuantity() != null? cache.getQuantity(): 0;
                                            String  dimension_unit   = this.jsonToDimensionUnit( json_response );
                                            String  variants = this.jsonToVariants( json_response );
                                            Double  weight   = this.jsonToWidth(  json_response );
                                            Double  length   = this.jsonToLength( json_response );
                                            Double  height   = this.jsonToHeight( json_response );
                                            Double  width    = this.jsonToWidth( json_response );
          
                                            String dimention = "" + length + "*" + width + "*" + height;
                                            String weight_unit =  this.jsonToWightUnit( json_response );
                                            try {
                                            Product old_product =   all_products
                                                                   .stream()
                                                                   .filter(( Product product ) -> ((product.getAsin() !=null && product.getAsin().equals(asin)) || (product.getSku() !=null && product.getSku().equals(sku))))
                                                                   .findAny()
                                                                   .orElse(null);
      
                                            Item old_item =   all_items
                                                             .stream()
                                                             .filter(( Item item ) -> ((item.getAsin() !=null && item.getAsin().equals(asin)) || (item.getSku() !=null && item.getSku().equals(sku))))
                                                             .findAny()
                                                             .orElse(null);
                                            if(duplicate_asins.contains(asin)){
                                               cache.setProcessed(true);
                                               session2.update(cache);
                                               continue;
                                            }
                                            
                                            if( old_product != null && old_item != null){
                                                 old_product.setSku(sku);
                                                 old_product.setAsin( asin );
                                                 old_product.setName( product_name );
                                                 old_product.setDescription( description );
                                                 old_product.setBrand( brand );
                                                 old_product.setCategory( sub_category );
                                                 old_product.setManufacturer( manufacturer );
                                                 old_product.setCountry( country );
                                                 old_product.setPictures(pictures);
                                                 old_product.setAmazon_listing_id( amazon_listing_id );
                                                 old_product.setRank( ranks );
                                                 old_product.setActive(Byte.parseByte("1"));
                                                 old_product.setLaunch_date(launch_date);
                                                 session2.update( old_product );
      
                                                 old_item.setSku( sku ) ;  
                                                 old_item.setAsin(asin); 
                                                 old_item.setMrp( mrp );
                                                 old_item.setPrice( price );
                                                 old_item.setQuantity( quantity );
           
                                                 old_item.setDimentions( dimention );
                                                 old_item.setDimension_unit( dimension_unit );
                                                 old_item.setVariants( variants );
                                                 old_item.setProduct( old_product );
                                                 old_item.setAmazon_listing_id( amazon_listing_id );
                                                 old_item.setWeight( weight.toString() );
                                                 old_item.setWeight_unit( weight_unit );
                                                 old_item.setSync_amazon( Byte.parseByte("1") );
                                                 old_item.setRank( ranks );
                                                 old_item.setActive(Byte.parseByte("1"));
                                                 old_item.setFlipkart_active_listing(Byte.parseByte("0"));
                                                 old_item.setInventory_type("0");
                                                 old_item.setSync_flipcart(Byte.parseByte("0"));
                                                 old_item.setNewly_added(false);
                                                 session2.update(old_item);
                                                 cache.setProcessed(true);
                                                 session2.update(cache);
                                                 outputJsonArray.put( 
                                                           new JSONObject()
                                                          .put("asin", asin)
                                                          .put("sku",  sku)
                                                          .put("price", price)
                                                          .put("quantity", quantity)
                                                          .put("amazon_listing_id", amazon_listing_id)
                                                          .put("variation", "single")
                                                          .put("status", "successfully updated")
                                                );
      
                                             }else {
      
                                                 Product product = new Product();
                                                 product.setAsin( asin );
                                                 product.setSku(sku);
                                                 product.setName( product_name );
                                                 product.setDescription( description );
                                                 product.setBrand( brand );
                                                 product.setCategory( sub_category );
                                                 product.setManufacturer( manufacturer );
                                                 product.setCountry( country );
                                                 product.setPictures(pictures);
                                                 product.setAmazon_listing_id( amazon_listing_id );
                                                 product.setRank( ranks );
                                                 product.setActive(Byte.parseByte("1"));
                                                 product.setLaunch_date(launch_date);
                                                 session2.save( product );
      
                                                 if(old_item != null){
                                                     old_item.setAsin(asin);
                                                     old_item.setSku( sku ) ;   
                                                     old_item.setMrp( mrp );
                                                     old_item.setPrice( price );
                                                     old_item.setQuantity( quantity );
      
                                                     old_item.setDimentions( dimention );
                                                     old_item.setDimension_unit( dimension_unit );
                                                     old_item.setVariants( variants );
                                                     old_item.setProduct( product );
                                                     old_item.setAmazon_listing_id( amazon_listing_id );
                                                     old_item.setWeight( weight.toString() );
                                                     old_item.setWeight_unit( weight_unit );
                                                     old_item.setSync_amazon( Byte.parseByte("1") );
                                                     old_item.setRank( ranks );
                                                     old_item.setActive(Byte.parseByte("1"));
                                                     old_item.setFlipkart_active_listing(Byte.parseByte("0"));
                                                     old_item.setInventory_type("0");
                                                     old_item.setSync_flipcart(Byte.parseByte("0"));
                                                     old_item.setAsin(asin);
                                                     old_item.setNewly_added(false);
                                                     session2.update( old_item );
                                                     outputJsonArray.put( 
                                                           new JSONObject()
                                                          .put("asin", asin)
                                                          .put("sku",  sku)
                                                          .put("price",  price)
                                                          .put("quantity", quantity)
                                                          .put("amazon_listing_id", amazon_listing_id)
                                                          .put("variation", "single")
                                                          .put("status", "successfully created")
                                                    );
                                                 }else{
                                                     Item item = new Item();
                                                     item.setAsin(asin);
                                                     item.setSku( sku ) ;   
                                                     item.setMrp( mrp );
                                                     item.setPrice( price );
                                                     item.setQuantity( quantity );
                                          
                                                     item.setDimentions( dimention );
                                                     item.setDimension_unit( dimension_unit );
                                                     item.setVariants( variants );
                                                     item.setProduct( product );
                                                     item.setAmazon_listing_id( amazon_listing_id );
                                                     item.setWeight( weight.toString() );
                                                     item.setWeight_unit( weight_unit );
                                                     item.setSync_amazon( Byte.parseByte("1") );
                                                     item.setRank( ranks );
                                                     item.setActive(Byte.parseByte("1"));
                                                     item.setFlipkart_active_listing(Byte.parseByte("0"));
                                                     item.setInventory_type("0");
                                                     item.setSync_flipcart(Byte.parseByte("0"));
                                                     item.setAsin(asin);
                                                     item.setNewly_added(true);
                                                     session2.save( item );
                                                     outputJsonArray.put( 
                                                        new JSONObject()
                                                       .put("asin", asin)
                                                       .put("sku",  sku)
                                                       .put("price", price)
                                                       .put("quantity", quantity)
                                                       .put("amazon_listing_id", amazon_listing_id)
                                                       .put("variation", "single")
                                                       .put("status", "successfully created")
                                                     );
                                                 }
                                                 
                                                 cache.setProcessed(true);
                                                 session2.update(cache);
      
                                             }
                                             if(k % 20 == 0){
                                              session2.flush();
                                              session2.clear();
                                             }

                                        } catch (Exception dbex) {
                                              dbex.printStackTrace();
                                              outputJsonArray.put( 
                                                new JSONObject()
                                               .put("asin", asin)
                                               .put("sku",  sku)
                                               .put("price",  price)
                                               .put("quantity", quantity)
                                               .put("amazon_listing_id", amazon_listing_id)
                                               .put("variation", "single")
                                               .put("status", "failed: " + dbex.getMessage())
                                              );
                                        }
                                    }
                               }else{

                                   //file upload
                                   log.setStatus("completed");
                                   String csv_file     = this.getRandStr(12) + ".csv";
                                   String local_folder = this.getRandStr(12);
                                   Files.createDirectories( Paths.get(local_folder));
                                   FileWriter outputfile = new FileWriter( local_folder + File.separator + csv_file);
                                   CSVWriter writer = new CSVWriter(outputfile);
                                   writer.writeNext(new String[]{"asin","sku","quantity","price","type","status"});
                                   JSONArray jsonProducts = new JSONArray( log.getCsv_buffer());
                                   jsonProducts = this.processProductJsonArray(jsonProducts);
                                   for (int k = 0; k < jsonProducts.length(); k++) {
                                        JSONObject jsonProduct = jsonProducts.getJSONObject(k);
                                        writer.writeNext(new String[]{
                                              jsonProduct.has("asin")? jsonProduct.getString("asin"): "",
                                              jsonProduct.has("sku")?  jsonProduct.getString("sku"): "" ,
                                              jsonProduct.has("quantity")? jsonProduct.getString("quantity"): "",
                                              jsonProduct.has("price")? jsonProduct.getString("price"): "",
                                              jsonProduct.has("variation")? jsonProduct.getString("variation"): "",
                                              jsonProduct.has("status")? jsonProduct.getString("status"): "",
                                        });
                                   }
                                   writer.close();
                                   outputfile.close();
                                   this.uploadLogFile(client, local_folder + File.separator +csv_file );
                                   FileUtils.deleteQuietly( new File(local_folder + File.separator + csv_file));

                                   JSONArray output_files_array = new JSONArray();
                                   JSONObject output_file_object= new JSONObject();
                                   output_file_object.put("gcp_file_name",csv_file);
                                   output_file_object.put("gcp_log_folder_name", "logs");
                                   output_file_object.put("gcp_task_folder_name",local_folder);

                                   output_files_array.put( output_file_object );
                                   log.setOutput_files( output_files_array.toString() );

                                   JSONArray log_description1 = new JSONArray(log.getDescription());
                                   JSONObject log_add1= new JSONObject();
                                   log_add1.put("key","event");
                                   log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                                   log_add1.put("value","Task Completed");
       
                                   log_description1.put( log_add1 );
                                   log.setDescription( log_description1.toString() );
                                   log.setUpdated_at( Timestamp.valueOf( java.time.LocalDateTime.now() ) );
                                   session2.persist( log );
                                   Query q = session2.createQuery("delete AmazonProductCache APC WHERE APC.log_id=:log_id");
                                   q.setParameter("log_id", log.getId());
                                   q.executeUpdate();
                                   FileUtils.deleteQuietly( new File(image_folder));
                                   this.stopAmazonProductProcessJob(client_id, log_id, job_key, trigger_key);
                                   this.LogMessageInfo(this.getClass().getName() + " Cache Process Shutting down");

                               }
                           }
                        }
                        log.setCsv_buffer(outputJsonArray.toString());
                        log.setContext(log_context.toString());
                        log.setJob_running(false);
                        session2.update(log);
                        tx4.commit();
                        tx4 = null;
                        this.LogMessageInfo("Successfuly Amazon Product Import final process completed");
                    }catch(Exception e){

                        e.printStackTrace();
                        if(tx4 != null ){
                            tx4.rollback();
                            tx4 = null;
                         }
                         if(tx3 != null ){
                            tx3.rollback();
                            tx3 = null;
                         }
                        if(log!=null && log.getId()!=null) {
                            tx4  = session2.beginTransaction();
                            log  = session2.load( Log.class, log.getId());
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
                            Query q = session2.createQuery("delete AmazonProductCache APC WHERE APC.log_id=:log_id");
                            q.setParameter("log_id", log.getId());
                            q.executeUpdate();
                            tx4.commit();
                            FileUtils.deleteQuietly( new File(image_folder));
                            this.stopAmazonProductProcessJob(client_id, log_id, job_key, trigger_key);
                        } 
                    }
                  
              } catch (Exception e) {
                  
                   e.printStackTrace();

                   if(tx4 != null ){
                      tx4.rollback();
                   }
                   if(tx2 != null ){
                      tx2.rollback();
                   }
                   if(tx3 != null ){
                      tx3.rollback();
                   }
                   this.LogMessageError("Error: " + e.getMessage());
      
              }finally{
                   
                   
      
              }
      
          }catch (Exception e) {
               
                this.LogMessageError("Error: " + e.getMessage());
      
          }finally{
      
               if(sessionFactory1 != null){
                   sessionFactory1.close();
               }
               if(sessionFactory2 != null){
                   sessionFactory2.close();
               }
               if(session1 != null){
                   session1.close();
               }
               if(session2 != null){
                   session2.close();
               }
               if(tx1 != null ){
                   tx1.commit();
               }
          }
      
   }

   public boolean isBlank( String value){
        if( value != null && !value.isEmpty()){
            return false;
        }
        return true;
   }
   public JSONArray processProductJsonArray(JSONArray jsonProducts ) throws JSONException{

       JSONArray jsonOutput = new JSONArray();
       for (int i = 0; i < jsonProducts.length(); i++) {
               JSONObject p_product = jsonProducts.getJSONObject(i);
               if( p_product.getString("variation").equalsIgnoreCase("product")){
                    jsonOutput.put( p_product );
                    for (int j = 0; j < jsonProducts.length(); j++) {
                         JSONObject c_product = jsonProducts.getJSONObject(j);
                         if(  c_product.getString("variation").equalsIgnoreCase("item") &&
                              c_product.getString("parent_asin").equalsIgnoreCase( p_product.getString("asin"))){
                              jsonOutput.put(c_product);
                         }
                    }
               }
       }
       for (int i = 0; i < jsonProducts.length(); i++) {
              JSONObject s_product = jsonProducts.getJSONObject(i);
              if( s_product.getString("variation").equalsIgnoreCase("single")){
                 jsonOutput.put( s_product );
              }
       }
       return jsonOutput;
   }

   public List<String> getAsinListofItems(List<Item> items){
       List<String> asins = new ArrayList<>();
       for (Item item : items) {
          asins.add( item.getAsin() );
       }
       return asins;
   }

   public List<String> getAsinListOfProducts(List<Product> products){
      List<String> asins = new ArrayList<>();
      for (Product product : products) {
          asins.add( product.getAsin() );
      }
      return asins;
  }
   public Product findProductByAsin(List<Product> products, String asin){

       for (Product product : products) {
           if(product.getAsin() != null && product.getAsin().equalsIgnoreCase(asin)){
              return product;
           }
       }
       return null;
   }

   public Product findProductBySku(List<Product> products, String asin){

       for (Product product : products) {
           if(product.getSku() != null && product.getSku().equalsIgnoreCase(asin)){
              return product;
           }
       }
       return null;
   }

   public String jsonToPicture(JSONObject item_json, Client client, Log log) throws Exception{

       String pictures = "{}";
       String img_name = getRandStr(12);

       JSONObject context = new JSONObject( log.getContext() );

       String IMAGE_FOLDER = context.getString("image_folder");
       String IMAGE_BUCKET = env.getProperty("spring.gcp.product-image-bucket");
   
       if( item_json.has("images") && 
           item_json.getJSONArray("images").length()!=0 && 
           item_json.getJSONArray("images").getJSONObject(0).has("images") &&
           item_json.getJSONArray("images").getJSONObject(0).getJSONArray("images").length()!=0
       ){
   
           String main_img = null, general_img = null, main_img_extension = null, general_img_extension = null;
           String main_img_url = null, general_img_url = null;
           JSONArray pictures_json_arr =  item_json
                                         .getJSONArray("images")
                                         .getJSONObject(0)
                                         .getJSONArray("images");
   
           for(int l=0 ; l< pictures_json_arr.length(); l++){
                JSONObject picture_json_obj = pictures_json_arr.getJSONObject(l);
                String img_variant = picture_json_obj.getString("variant");
                int img_height = picture_json_obj.getInt("height");
                int img_width = picture_json_obj.getInt("width");
                if(img_variant.equals("MAIN") && (img_height>=1000) && img_width>=1000){
                     main_img_url = picture_json_obj.getString("link");
                     main_img_extension = this.getExtention(main_img_url);
                     main_img = img_name+"."+main_img_extension;
                }
                if(img_variant.equals("MAIN") && (img_height<1000) && img_width<1000){
                     general_img_url = picture_json_obj.getString("link");
                     general_img_extension = this.getExtention(general_img_url);
                     general_img = img_name+"-general."+general_img_extension;
                }
            }
            if(main_img!=null && general_img!=null){
                if(this.downloadFile(main_img_url, File.separator+ IMAGE_FOLDER +File.separator+main_img) && this.downloadFile(general_img_url, File.separator+ IMAGE_FOLDER +File.separator + general_img)){
                     BlobId blobId1 = BlobId.of(IMAGE_BUCKET, client.getName() + File.separator+IMAGE_FOLDER +File.separator+main_img);
                     BlobInfo blobInfo1 = BlobInfo.newBuilder(blobId1).build();
                     this.getStorage().create(blobInfo1, Files.readAllBytes(Paths.get(File.separator+IMAGE_FOLDER +File.separator+main_img)));
                     this.getStorage().createAcl(blobId1, Acl.of(Acl.User.ofAllUsers(), Acl.Role.READER));
   
                     BlobId blobId2 = BlobId.of(IMAGE_BUCKET, client.getName() + File.separator+IMAGE_FOLDER +File.separator+general_img);
                     BlobInfo blobInfo2 = BlobInfo.newBuilder(blobId2).build();
                     this.getStorage().create(blobInfo2, Files.readAllBytes(Paths.get(File.separator+IMAGE_FOLDER +File.separator+general_img)));
                     this.getStorage().createAcl(blobId2, Acl.of(Acl.User.ofAllUsers(), Acl.Role.READER));
                     pictures = new JSONObject()
                     .put("bucket",IMAGE_BUCKET)
                     .put("client", client.getName() )
                     .put("folder",  IMAGE_FOLDER )
                     .put("images", new JSONArray()
                     .put(  new JSONObject()
                            .put("general_image_name", general_img)
                            .put("thumbnail_image_name", main_img)
                            .put("actual_image_name", main_img)
                      )).toString();
                }
             }
     }
     return pictures;
   }


   
   public Brand searchBrand(List<Brand> brands, String brand_name){
       
       for (Brand brand : brands) {
              if(brand.getName().equalsIgnoreCase(brand_name)){
                  return brand; 
              }
       }

       return null;
   }

   public Category searchSubCategory(List<Category> sub_categories, String category_name){

      for (Category category : sub_categories) {
        if(category.getName().equalsIgnoreCase(category_name)){
            return category;
        }
      }
      return null;
   }

   public Manufacturer searchManuFacturer(List<Manufacturer> manufacturers, String manufactuter_name ){

      for (Manufacturer manufacturer : manufacturers) {
           if(manufacturer.getName().equalsIgnoreCase(manufactuter_name)){
             return manufacturer;
           }
      }
      return null;
   }

   public Country searchCounrty(List<Country> countries, String country_name){
       
      for (Country country : countries) {
          if(country.getName().equalsIgnoreCase( country_name )){
                   return country;
          }
      }
      return null;

   }

   public String jsonToSku(JSONObject item_json) throws JSONException{
      String sku = null;
      if(item_json.has("attributes")){
        if(item_json.getJSONObject("attributes").has("part_number")){
            if(item_json.getJSONObject("attributes").getJSONArray("part_number").length() != 0){
               sku = item_json.getJSONObject("attributes").getJSONArray("part_number").getJSONObject(0).getString("value");
            }
        }
      }
      return sku;
   }

   public String jsonToBrand( JSONObject item_json ) throws Exception {

       String brand_name = "Uncategorised" ;

       if(item_json.has("summaries")){
               if(item_json.getJSONArray("summaries").length()!=0){
                   if(item_json.getJSONArray("summaries").getJSONObject(0).has("brandName")){
                       brand_name = item_json.getJSONArray("summaries").getJSONObject(0).getString("brandName");
                   }
               }
       }

       return brand_name;
   }

   public String jsonToName( JSONObject item_json ) throws Exception {

        String name = "unknown" ;
    
        if(item_json.has("summaries")){
                if(item_json.getJSONArray("summaries").length()!=0){
                    if(item_json.getJSONArray("summaries").getJSONObject(0).has("brandName")){
                        name = item_json.getJSONArray("summaries").getJSONObject(0).getString("itemName");
                    }
                }
        }
    
        return name;
   }

   public String jsonToDesciption( JSONObject item_json ) throws Exception {

        String desc = "unknown" ;
    
        if(item_json.has("attributes")){
                if(item_json.getJSONObject("attributes").has("product_description")){
                    if(item_json.getJSONObject("attributes").getJSONArray("product_description").length() != 0){
                       desc = item_json.getJSONObject("attributes").getJSONArray("product_description").getJSONObject(0).getString("value");
                    }
                }
        }
    
        return desc;
    }


   public String jsonToManufacturer( JSONObject item_json ) throws Exception {

       String manufacturer_name = "Uncategorised";
       if( item_json.has("attributes") && 
           (item_json.getJSONObject("attributes")
           .has("manufacturer")) &&
           (item_json.getJSONObject("attributes").getJSONArray("manufacturer").length() != 0)
           ){
           manufacturer_name = item_json.getJSONObject("attributes").getJSONArray("manufacturer").getJSONObject(0).getString("value");
       }
   
       return manufacturer_name;
  }

  public String jsonToSubCategory( JSONObject item_json ) throws Exception {

    String sub_category_name =  "Uncategorised";
    if(item_json.has("productTypes")){
      if(item_json.getJSONArray("productTypes").length()!=0){
          sub_category_name = item_json.getJSONArray("productTypes").getJSONObject(0).getString("productType");
      }
    }

    return sub_category_name;

  }

  public String jsonToCountry( JSONObject item_json ) throws Exception {

     return "India";

  }


  public String jsonToRank(JSONObject item_json) throws Exception{

    String ranks = null;
    if( item_json.has("ranks") && 
        item_json.getJSONArray("ranks").length()!=0 && 
        item_json.getJSONArray("ranks").getJSONObject(0).has("ranks") &&
        item_json.getJSONArray("ranks").getJSONObject(0).getJSONArray("ranks").length()!=0
    ){
          ranks = item_json.getJSONArray("ranks").getJSONObject(0).getJSONArray("ranks").toString();
     
     }
     return ranks;
  }

  public String jsonToColor( JSONObject item_json ) throws Exception{
      String color = "none";
      if( item_json.has("attributes") && 
          item_json.getJSONObject("attributes").has("color") &&
          item_json.getJSONObject("attributes").getJSONArray("color").length()!=0
       ){
             color = item_json.getJSONObject("attributes").getJSONArray("color").getJSONObject(0).getString("value");
       }
       return color;
  }

  public Double jsonToWight( JSONObject item_json ) throws Exception{
    Double weight = 0.0d;
    if( item_json.has("attributes") && 
        item_json.getJSONObject("attributes").has("item_package_weight") &&
        item_json.getJSONObject("attributes").getJSONArray("item_package_weight").length()!=0
    ){
          weight = item_json.getJSONObject("attributes").getJSONArray("item_package_weight").getJSONObject(0).getDouble("value");
    }
    return weight;
  }

  public String jsonToWightUnit( JSONObject item_json ) throws Exception{
    String weight_unit  =  "grams";
    if( item_json.has("attributes") && 
        item_json.getJSONObject("attributes").has("item_package_weight") &&
        item_json.getJSONObject("attributes").getJSONArray("item_package_weight").length()!=0
    ){
          weight_unit = item_json.getJSONObject("attributes").getJSONArray("item_package_weight").getJSONObject(0).getString("unit");
    }
     return weight_unit;
  }

  public String jsonToVariants( JSONObject item_json ) throws Exception{
    String size  =  "grams";
    String color =  "none";

    if(   item_json.has("summaries") && 
          item_json.getJSONArray("summaries").length()!=0
    ){
          if(item_json.getJSONArray("summaries").getJSONObject(0).has("sizeName")){
             size = item_json.getJSONArray("summaries").getJSONObject(0).getString("sizeName");
          }
    }

    if( item_json.has("attributes") && 
        item_json.getJSONObject("attributes").has("color") &&
        item_json.getJSONObject("attributes").getJSONArray("color").length()!=0
     ){
           color = item_json.getJSONObject("attributes").getJSONArray("color").getJSONObject(0).getString("value");
     }
     return new JSONArray()
                .put(new JSONObject().put("key","color").put("value", color))
                .put(new JSONObject().put("key","size").put("value", size))
                .toString();
  }


   public Double jsonToWidth(JSONObject item_json ) throws Exception {

        Double width  = 0.0d;
        if( item_json.has("attributes") && 
            item_json.getJSONObject("attributes")
            .has("item_dimensions") && 
            item_json.getJSONObject("attributes")
            .getJSONArray("item_dimensions").length()!=0 
        ){
            width  = item_json.getJSONObject("attributes")
                    .getJSONArray("item_dimensions")
                    .getJSONObject(0)
                    .getJSONObject("width")
                    .getDouble("value");
        }
    
        return width;

   }


   public Double jsonToHeight(JSONObject item_json ) throws Exception {

        Double height = 0.0d;
        if( item_json.has("attributes") && 
            item_json.getJSONObject("attributes")
            .has("item_dimensions") && 
            item_json.getJSONObject("attributes")
            .getJSONArray("item_dimensions").length()!=0 
        ){
            
            height = item_json.getJSONObject("attributes")
                              .getJSONArray("item_dimensions")
                              .getJSONObject(0)
                              .getJSONObject("height")
                              .getDouble("value");
        }

       return height;
   
   }

   public Double jsonToLength(JSONObject item_json ) throws Exception{

        Double length = 0.0d;
        if( item_json.has("attributes") && 
            item_json.getJSONObject("attributes")
            .has("item_dimensions") && 
            item_json.getJSONObject("attributes")
            .getJSONArray("item_dimensions").length()!=0 
        ){
            length = item_json.getJSONObject("attributes").getJSONArray("item_dimensions").getJSONObject(0).getJSONObject("length").getDouble("value"); 
        }
        return length;
   }

   public String jsonToDimensionUnit( JSONObject item_json ) throws Exception{

    String dimension_unit = "inches";
    if( item_json.has("attributes") && 
        item_json.getJSONObject("attributes")
        .has("item_dimensions") && 
        item_json.getJSONObject("attributes")
        .getJSONArray("item_dimensions").length()!=0 
    ){
        dimension_unit =  item_json.getJSONObject("attributes")
                                   .getJSONArray("item_dimensions")
                                   .getJSONObject(0)
                                   .getJSONObject("length")
                                   .getString("unit");
    }

    return dimension_unit;

   }

   @KafkaListener(topics="gcp.store.products.amazon.import.api_cache.0", groupId="nano_group_id")
   public void AmazonProductImportApiCache( String message ){


          SessionFactory sessionFactory1 = null, sessionFactory2 = null;
          Session     session1 = null,  session2 = null;
          Transaction tx1 = null, tx2 = null, tx3 = null, tx4 = null;
          String[] includedData = {"variations","attributes","productTypes","salesRanks","summaries","images"};

          try {
              String body = new JSONObject(message).getString("body");
              JSONObject json = new JSONObject(body);
              BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
              BigInteger log_id  = BigInteger.valueOf(json.getInt("log_id"));
              String job_key     = json.getString("job_key");
              String trigger_key = json.getString("trigger_key");
              String topic       = json.getString("topic");
              int min            = json.getInt("min");

              Configuration configuration = new Configuration();
              configuration.configure();
              configuration.setProperties(this.SetMasterDB());
              sessionFactory1 = configuration.buildSessionFactory();
              session1 = sessionFactory1.openSession();
              tx1 = session1.beginTransaction();
              Client client = session1.load(Client.class, client_id);
    
              OkHttpClient amazon_client = new OkHttpClient();

              if(client == null){
                  throw new Exception("Client is not valid");
              }
              tx1.commit();
              tx1=null;

              try {
                  configuration.setProperties(this.SetClientDB(client.getDb_host(), client.getDb_host(), client.getDb_name(), client.getDb_user(), client.getDb_pass()));
                  sessionFactory2 = configuration.buildSessionFactory();
                  session2 = sessionFactory2.openSession();
                  tx2      = session2.beginTransaction();
                  Log log  = session2.load( Log.class, log_id);
                 try{
                  if( log != null){
                  Setting verified = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.verified"));
                  if(verified==null || verified.getValue().equals("0")) {
                      throw new Exception("Amazon Integration is not verified");
                  }

                  Setting active = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.active"));
                  if(active==null || active.getValue().equals("0")) {
                      throw new Exception("Amazon Integration is not activated");
                  }

                  Setting token = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.token_key"));
                  if(token==null || token.getValue().equals("0")) {
                        throw new Exception("Amazon auth token is not found");
                  }

                  Setting refresh_token = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.refresh_token_key"));
                  if(refresh_token==null || refresh_token.getValue().equals("0")) {
                        throw new Exception("Amazon auth refresh token is not found");
                  }
                  
                  if(this.IsMaxTokenCreateFailWithCount("amazon", session2, false)){
                      this.StopSyncJob(session2, "AMAZON_ORDER_SYNC", false, client.getId(), false);
                      this.StopSyncJob(session2, "AMAZON_FLIPKART_INVENTORY_SYNC", false, client_id, false);
                      throw new Exception("Token creation failed max time. Sync Job Stopped");
                  }

                  Setting expire_in = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.token_expire_in"));
                    
                  if(expire_in==null || expire_in.getValue().equals("0")) {
                          
                      throw new Exception("Amazon expire in not found");
                  }

                  Setting expired = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.expired"));
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
                            
                            // System.out.println("generating new token");
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
                            session2.update(token);
                            expired.setValue("0");
                            expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session2.update(expired);
                            expire_in.setValue(token_expire_in.toString());
                            expire_in.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session2.update(expire_in);

                        }
                        catch (Exception ex) {
                            tx2.rollback();
                            tx2 = null;
                            Transaction tx_exception = session2.beginTransaction();
                            expired.setValue("1");
                            expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session2.update(expired);
                            this.SetMaxTokenCreateFail("amazon", session2, false);
                            tx_exception.commit();
                            throw new Exception("Issue generating amazon token :" + ex.getMessage());
                        }
                      }

                      tx2.commit();
                      tx2 = null;
                      tx3 = session2.beginTransaction();
                      
                      String hql   = "From AmazonProductCache APC WHERE APC.api_response IS NULL AND APC.log_id=:log_id ORDER BY APC.id ASC";
                      Query  query = session2.createQuery( hql );
                      query.setParameter("log_id", log.getId());
                      query.setFirstResult(0);
                      query.setMaxResults(this.MAX_CACHE_PER_TASK);

                      List<AmazonProductCache> cache_list = query.list();


                      Query query1 = session2.createQuery("From AmazonProductCache APC where APC.log_id=:log_id");
                      query1.setParameter("log_id", log.getId());
                      List<AmazonProductCache> all_cache = query1.list();
                      List<String> all_available_asins   = new ArrayList<String>();
                      for (int i = 0; i < all_cache.size(); i++) {
                          all_available_asins.add( all_cache.get(i).getAsin() );
                      }
    
                      if(cache_list.size() != 0){
                          for (int i = 0; i < cache_list.size(); i++) {
                               AmazonProductCache single_cache = cache_list.get(i);
                               String item_asin = cache_list.get(i).getAsin();
                               HttpUrl.Builder getCatelogUrlBuilder = HttpUrl.parse(env.getProperty("spring.integration.amazon.feed_api_url")+"/catalog/2020-12-01/items/" + item_asin )
                               .newBuilder();
                               getCatelogUrlBuilder.addQueryParameter("marketplaceIds", env.getProperty("spring.integration.amazon.marketplace_id"));
                               getCatelogUrlBuilder.addQueryParameter("includedData", this.join(includedData, ","));
                              
                               com.squareup.okhttp.Request getCatalogRequest = new Request.Builder()
                              .addHeader("x-amz-access-token",token.getValue())
                              .url(getCatelogUrlBuilder.build().toString())
                              .build();
    
                              com.squareup.okhttp.Request getSignedCatelogRequest = this.getAwsSigned(getCatalogRequest);
                              com.squareup.okhttp.Response getCatelogResponse = amazon_client.newCall(getSignedCatelogRequest).execute();
                              JSONObject json_resp = new JSONObject(getCatelogResponse.body().string());

                              if(getCatelogResponse.code() == 403){
                                  throw new ExpiredTokenException("token expired");
                              }

                              if(getCatelogResponse.isSuccessful()){
                                 single_cache.setApi_response( json_resp.toString() );
                                 single_cache.setProcessed(false);
                                 JSONArray relations  = new JSONArray();
                                 JSONObject variation = new JSONObject();
                                 String variation_type   = "";
                                 JSONArray related_asins = new JSONArray();

                                 if( json_resp.has("variations") && 
                                     json_resp.getJSONArray("variations").length() != 0 ){
                                      
                                     relations = json_resp.getJSONArray("variations");
                                     variation = relations.getJSONObject(0);
                                     variation_type = variation.has("variationType")? variation.getString("variationType"): "";
                                     related_asins  = variation.has("asins")? variation.getJSONArray("asins"): new JSONArray();
                                 }


                                //  if( json_resp.has("variations")){
                                //      JSONArray relations = json_resp.getJSONArray("variations");
                                //  if(  relations.length() != 0 ){
                                //        JSONObject variation = relations.getJSONObject(0);
                                       if( 
                                           variation_type.equalsIgnoreCase("CHILD")
                                        ){
                                             single_cache.setProduct_type("item");
                                             for (int j = 0; j < related_asins.length(); j++) {
                                                 String asin = related_asins.getString(j);
                                                 if(!all_available_asins.contains(asin)){
                                            
                                                    AmazonProductCache cache = new AmazonProductCache();
                                                    cache.setAsin(asin);; 
                                                    cache.setLog_id(log_id);
                                                    cache.setIs_outside(true);
                                                    cache.setMrp(Double.parseDouble("0.0"));                   
                                                    cache.setPrice(Double.parseDouble("0.0"));
                                                    cache.setQuantity(Integer.parseInt("0"));
                                                    cache.setProcessed(false);
                                                    cache.setProduct_type("product");
                                                    session2.persist(cache);
                                                    all_available_asins.add(asin);
                                                 }
                                             }
                                       }
                                       if(variation_type.equalsIgnoreCase("PARENT")){
                                           single_cache.setProduct_type("product");
                                       }
                                       if(variation_type.equalsIgnoreCase("")){
                                          single_cache.setProduct_type("single");
                                       }
                                       single_cache.setRelation(related_asins.toString());
                                       session2.persist(single_cache);
                              }else{
                                    if( getCatelogResponse.code() == 404){
                                           
                                              session2.remove( single_cache );
                                    }

                              }
                          }
                      }else{
                          this.removeAmazonProductCacheJob(client_id, log_id, job_key, trigger_key);
                          this.startAmazonProductProcessJob(client_id, log_id);
                      }
                      log.setJob_running(false);
                      session2.update(log);
                      tx3.commit();
                      tx3 = null;
                      this.LogMessageInfo("Successfuly a Amazon Product Import Phrase completed");

                     }
                   }catch(ExpiredTokenException etx){
                          this.LogMessageInfo("Token is expired. will continue.");
                          if(tx2 != null ){
                              tx2.rollback();
                              tx2 = null;
                          }
                          if(tx3 != null ){
                             tx3.rollback();
                             tx3 = null;
                          }
                          tx4 = session2.beginTransaction();
                          Setting expired = session2.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.expired"));
                          expired.setValue("1");
                          expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                          session2.update(expired);
                          Log new_log = session2.load(Log.class, log.getId());
                          new_log.setJob_running(false);
                          session2.update(new_log);
                          tx4.commit();
                   }
                   catch (Exception e) {

                      e.printStackTrace();
                      if(tx2 != null ){
                          tx2.rollback();
                          tx2 = null;
                      }
                      if(tx3 != null ){
                         tx3.rollback();
                         tx3 = null;
                      }

                      if(log!=null && log.getId()!=null) {
                          tx4 = session2.beginTransaction();
                          log = session2.load(Log.class, log.getId());
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
                          Query q = session2.createQuery("delete AmazonProductCache APC WHERE APC.log_id=:log_id");
                          q.setParameter("log_id", log.getId());
                          q.executeUpdate();
                          tx4.commit();
                          this.removeAmazonProductCacheJob(client_id, log_id, job_key, trigger_key);
                      }
                   }
                  
              } catch (Exception e) {
                  
                   e.printStackTrace();
                   this.LogMessageError("Error: " + e.getMessage());

              }finally{
                   
                   if(tx2 != null ){
                       tx2.rollback();
                   }
                   if(tx3 != null ){
                      tx3.rollback();
                   }

              }

          }catch (Exception e) {
                e.printStackTrace();
                this.LogMessageError("Error: " + e.getMessage());

          }finally{

               if(sessionFactory1 != null){
                   sessionFactory1.close();
               }
               if(sessionFactory2 != null){
                   sessionFactory2.close();
               }
               if(session1 != null){
                   session1.close();
               }
               if(session2 != null){
                   session2.close();
               }
               if(tx1 != null ){
                   tx1.commit();
               }
          }

   }

    private Request getAwsSigned(Request request){
         AWSAuthenticationCredentials awsAuthenticationCredentials = AWSAuthenticationCredentials.builder()
        .accessKeyId(env.getProperty("spring.integration.amazon.iam_access_keyID"))
        .secretKey(env.getProperty("spring.integration.amazon.iam_access_secretKey"))
        .region(env.getProperty("spring.integration.amazon.iam_region"))
        .build();
         return new AWSSigV4Signer(awsAuthenticationCredentials)
                                                .sign(request);

    }

    public String getByIndex(String[] arr, int index) {
        try {
            String value = arr[index];
            return value;
        } catch (IndexOutOfBoundsException e) {
            return null;
        }

        
    }

    public String join(String[] arr,String sep){
            String str="";
            for(int i=0; i < arr.length; i++){
                 str+=arr[i]+(((i+1)!=arr.length)?sep:"");
            }
            return str;
    }

    public String getRandStr(int n)
    {
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                    + "0123456789"
                                    + "abcdefghijklmnopqrstuvxyz";
        StringBuilder sb = new StringBuilder(n);
  
        for (int i = 0; i < n; i++) {
            int index
                = (int)(AlphaNumericString.length()
                        * Math.random());
            sb.append(AlphaNumericString
                          .charAt(index));
        }
        return sb.toString();
    }

    public String getExtention(String fileName){
        int index = fileName.lastIndexOf('.');
        if(index > 0) {
          String extension = fileName.substring(index + 1);
          return extension;
        }
        return null;
    }

    public Boolean downloadFile(String inputFile,String outputFile) {
        try{
           File file = new File(outputFile);
           Path path=Paths.get(outputFile.split(File.separator)[1]);
           //    file.getParentFile().mkdirs();
           Files.createDirectories(path);
           file.createNewFile();
           URL website = new URL(inputFile);
           ReadableByteChannel rbc = Channels.newChannel(website.openStream());
           FileOutputStream fos = new FileOutputStream(outputFile);
           fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
           fos.close();
           return true;
        }catch(Exception e){
               e.printStackTrace();
               System.out.println("Error: "+e.getMessage());
               return false;
        }
        
    }

    @KafkaListener(topics="gcp.store.products.aliases.import.0", groupId="nano_group_id")
    public void setProductAliasUpdate(String message){

        try{

            String body = new JSONObject(message).getString("body");
            JSONObject json = new JSONObject(body);
            BigInteger client_id= BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id= BigInteger.valueOf(json.getInt("log_id"));
            Configuration configuration = new Configuration();
            configuration.configure();
            configuration.setProperties(this.SetMasterDB());
            SessionFactory sessionFactory1 = null;
            Session session1 = null;
            sessionFactory1 = configuration.buildSessionFactory();
            session1 = sessionFactory1.openSession();
            Transaction tx1 = session1.beginTransaction();
            String local_folder=new GenerateRandomString(10).toString();

            Client client = session1.load(Client.class, client_id);
            if(client == null){
                throw new Exception("Client not found");
            }
            tx1.commit();
            configuration.setProperties(this.SetClientDB(client.getDb_host(), client.getDb_port(), client.getDb_name(), client.getDb_user(), client.getDb_pass())) ;
            SessionFactory sessionFactory2 = configuration.buildSessionFactory();
            Session session2 = sessionFactory2.openSession();
            Transaction tx2 = session2.beginTransaction();
            Transaction tx3 = null;
            Log log = session2.load(Log.class, log_id);
            if(log == null){
                throw new Exception("Log not found");
            }
            
            BufferedReader br = null;
            FileWriter outputfile = null;
            CSVReader  cr = null;
            FileWriter fw = null;
            CSVWriter  cw = null;
            FileReader fr = null;

            try {
                JSONArray log_description = new JSONArray(log.getDescription());
                JSONObject log_add = new JSONObject();
                log.setStatus("started");
                log_add.put("key", "Event");
                log_add.put("time", LocalDateTime.now(ZoneId.of("Asia/Kolkata")));
                log_add.put("value", "Task Started");
                log_description.put(log_add);
                log.setDescription(log_description.toString());
                log.setUpdated_at(Timestamp.valueOf(LocalDateTime.now()));
                session2.update(log);
                tx2.commit();

                tx3 = session2.beginTransaction();
                JSONArray input_files_array = new JSONArray(log.getInput_files());
                if(input_files_array.get(0) !=null ){
                    JSONObject input_file = new JSONObject(input_files_array.get(0).toString());
                    if(input_file.get("gcp_file_name")==null || input_file.get("gcp_log_folder_name")==null || input_file.get("gcp_task_folder_name")==null) {
                        throw new Exception("Not valid file information to process");
                    }
                    Storage storage = StorageOptions.newBuilder()
                                             .setProjectId(env.getProperty("spring.gcp.project-id"))
                                             .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                             .build()
                                             .getService();
                    Blob blob = storage.get(env.getProperty("spring.gcp.bucket-name"),client.getName()+"/"+input_file.get("gcp_log_folder_name").toString()+"/"+input_file.get("gcp_task_folder_name").toString()+"/"+input_file.get("gcp_file_name").toString());
                    // Create Random directory to store the file
                    Files.createDirectories(Paths.get(local_folder));
                    String csv_file=local_folder+"/"+input_file.get("gcp_file_name").toString();
                    String csv_file_report=local_folder+"/report-"+input_file.get("gcp_file_name").toString();
                    blob.downloadTo(Paths.get(csv_file));
                    fw = new FileWriter(csv_file_report);
                    cw = new CSVWriter(fw);
                    fr = new FileReader(csv_file);
                    cr = new CSVReader(fr);

                    // List<String[]> csvInputData = cr.readAll();
                    String[] csvInputHeaders;
                    if((csvInputHeaders = cr.readNext()) != null){

                        String[] csvInputHeadersLowercase = new String[csvInputHeaders.length];
                        String[] requiredHeaders = {"sku", "amazon_sku", "flipkart_sku"};
                        HashMap<String,Integer> headerIndex = new HashMap<>();
    
                        for (int i=0; i<csvInputHeaders.length; i++) {
                            csvInputHeadersLowercase[i] = csvInputHeaders[i].toLowerCase().replace(" ", "_");
                        }

                        Arrays.sort(requiredHeaders);

                        Integer count = 0;
                        System.out.println( csvInputHeadersLowercase[0] );

                        for (String header : requiredHeaders) {
                            if(!Arrays.asList(csvInputHeadersLowercase).contains(header))
                            {
                                throw new Exception("Column not found: " + header);
                            }
                            headerIndex.put(header, Integer.valueOf( Arrays.asList(csvInputHeadersLowercase).indexOf(header)));
                            count++;  
                        }
                        String[] csvInputRow;
                        // List<Item> processedItems = new ArrayList<>();
                        List<HashMap<String,Object>> csvOutputData = new ArrayList<HashMap<String,Object>>();

                        while((csvInputRow = cr.readNext()) != null ){
                            String  sku          = csvInputRow[ headerIndex.get("sku") ];
                            String  amazon_sku   = csvInputRow[ headerIndex.get("amazon_sku") ];
                            String  flipkart_sku = csvInputRow[ headerIndex.get("flipkart_sku") ];

                            if(sku.isEmpty()){
                                HashMap<String,Object> outputRow = new HashMap<>();
                                outputRow.put("sku", sku);
                                outputRow.put("amazon_sku", "");
                                outputRow.put("flipkart_sku", "");
                                outputRow.put("status", "fail: sku is empty");
                                csvOutputData.add(outputRow);
                                continue;
                            }

                            Item item = session2.bySimpleNaturalId(Item.class).load(sku);
                            if(item == null){
                                    HashMap<String,Object> outputRow =  new HashMap<>();
                                    outputRow.put("sku", sku);
                                    outputRow.put("amazon_sku", "");
                                    outputRow.put("flipkart_sku", "");
                                    outputRow.put("status", "fail: sku not found");
                                    csvOutputData.add(outputRow);
                                                            
                                
                            }else{
                                    item.setAmazon_sku_alias(amazon_sku);
                                    item.setFlipkart_sku_alias(flipkart_sku);
                                    session2.update(item);
                                    HashMap<String,Object> outputRow = new HashMap<>();
                                    outputRow.put("sku", item.getSku());
                                    outputRow.put("amazon_sku", amazon_sku);
                                    outputRow.put("flipkart_sku", flipkart_sku);
                                    outputRow.put("status", "success");
                                    csvOutputData.add(outputRow);
                            }
                        }
                        cw.writeNext(new String[]{"SKU",    "Amazon SKU",	"Flipkar SKU",	"Status"});
                        
                        for (int i = 0; i < csvOutputData.size(); i++) {
                            HashMap<String,Object> csvOutputRow = csvOutputData.get(i);
                            cw.writeNext(new String[]{ 
                                 csvOutputRow.get("sku").toString(),
                                 csvOutputRow.get("amazon_sku").toString(),
                                 csvOutputRow.get("flipkart_sku").toString(),
                                 csvOutputRow.get("status").toString()
                            });
                        }
                        cw.close();
                        cw = null;
                        fw.close();
                        fw = null;
                        BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+input_file.get("gcp_log_folder_name").toString()+"/"+input_file.get("gcp_task_folder_name").toString()+"/"+"report-"+input_file.get("gcp_file_name").toString());
                        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                        storage.create(blobInfo, Files.readAllBytes(Paths.get(csv_file_report)));
                        JSONArray output_files_array = new JSONArray();
                        JSONObject output_file_object= new JSONObject();
                        output_file_object.put("gcp_file_name","report-"+input_file.get("gcp_file_name").toString());
                        output_file_object.put("gcp_log_folder_name", input_file.get("gcp_log_folder_name").toString());
                        output_file_object.put("gcp_task_folder_name",input_file.get("gcp_task_folder_name").toString());
                        output_files_array.put(output_file_object);
                        log.setOutput_files(output_files_array.toString());
                        session2.update(log);
                   }
                } 
                
                JSONObject log_add1 = new JSONObject();
                log_add1.put("key", "Event");
                log_add1.put("time", LocalDateTime.now(ZoneId.of("Asia/Kolkata")));
                log_add1.put("value", "Task Completed");
                log_description.put(log_add1);
                log.setStatus("completed");
                log.setDescription(log_description.toString());
                log.setUpdated_at(Timestamp.valueOf(LocalDateTime.now()));
                session2.update(log);
                tx3.commit();
                
            } catch (Exception e) {
                e.printStackTrace();
                if(tx3 != null ) tx3.rollback();
                Transaction tx4 = session2.beginTransaction();
                JSONArray log_description = new JSONArray(log.getDescription());
                JSONObject log_add = new JSONObject();
                log.setStatus("failed");
                log_add.put("key", "Event");
                log_add.put("time", LocalDateTime.now(ZoneId.of("Asia/Kolkata")));
                log_add.put("value", "Error: " + e.getMessage());
                log_description.put(log_add);
                log.setDescription(log_description.toString());
                log.setUpdated_at(Timestamp.valueOf(LocalDateTime.now()));
                session2.update(log);
                tx4.commit();

            }finally{
                if(cw != null) cw.close();
                if(fw != null) fw.close();
                if(cr != null) cr.close();
                if(fr != null) fr.close();

                session1.close();
                sessionFactory1.close();
                session2.close();
                sessionFactory2.close();

            }

        }catch(Exception e){
            e.printStackTrace();
            this.LogMessageError("Error: " + e.getMessage());
        }

    }

    public JSONObject startAmazonProductCacheJob(BigInteger client_id, BigInteger log_id) throws Exception{

           JSONObject json = new JSONObject();
           json.put("client_id", client_id);
           json.put("log_id", log_id);
           json.put("topic", "gcp.store.products.amazon.import.api_cache.0");
           json.put("min", this.API_CACHE_JOB_INTERVAL);
           json.put("start", true);
           RequestBody body    = RequestBody.create( MediaType.parse("application/json"), json.toString());
           OkHttpClient client = new OkHttpClient();
           client.setConnectTimeout(2000,TimeUnit.SECONDS);
           client.setReadTimeout(2000,TimeUnit.SECONDS);
           Request request = new Request.Builder()
           .url( env.getProperty("spring.integration.system.AUTOMATE_JOB_HOST") + "/jobs/temporary-job" )
           .post(body)
           .build();      
           Response response = client.newCall(request).execute();
           if(response.isSuccessful()){
               return new JSONObject(response.body().string());
           }else{
               throw new Exception("Failed start Amazon product cache job");
           }
    }

    public JSONObject removeAmazonProductCacheJob(BigInteger client_id, BigInteger log_id, String jobKey, String triggerKey) throws Exception{

        JSONObject json = new JSONObject();
        json.put("client_id", client_id);
        json.put("log_id", log_id);
        json.put("topic", "gcp.store.products.amazon.import.api_cache.0");
        json.put("min", 0);
        json.put("job_key", jobKey);
        json.put("trigger_key", triggerKey);
        json.put("start", false);

        RequestBody body    = RequestBody.create( MediaType.parse("application/json"), json.toString());
        OkHttpClient client = new OkHttpClient();
        client.setConnectTimeout(2000,TimeUnit.SECONDS);
        client.setReadTimeout(2000,TimeUnit.SECONDS);
        Request request = new Request.Builder()
        .url( env.getProperty("spring.integration.system.AUTOMATE_JOB_HOST") + "/jobs/temporary-job" )
        .post(body)
        .build();      
        Response response = client.newCall(request).execute();
        if(response.isSuccessful()){
            return new JSONObject(response.body().string());
        }else{
            throw new Exception("Failed start Amazon product cache job");
        }
    }


    public JSONObject startAmazonProductProcessJob(BigInteger client_id, BigInteger log_id ) throws Exception{

        JSONObject json = new JSONObject();
        json.put("client_id", client_id);
        json.put("log_id", log_id);
        json.put("topic", "gcp.store.products.amazon.import.cache_process.0");
        json.put("min", this.CACHE_PROCESS_JOB_INTERVAL );
        json.put("start", true);

        RequestBody body    = RequestBody.create( MediaType.parse("application/json"), json.toString());
        OkHttpClient client = new OkHttpClient();
        client.setConnectTimeout(2000,TimeUnit.SECONDS);
        client.setReadTimeout(2000,TimeUnit.SECONDS);
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


    public JSONObject stopAmazonProductProcessJob(BigInteger client_id, BigInteger log_id,  String jobKey, String triggerKey) throws Exception{

        JSONObject json = new JSONObject();
        json.put("client_id", client_id);
        json.put("log_id", log_id);
        json.put("topic", "gcp.store.products.amazon.import.cache_process.0");
        json.put("min", 0);
        json.put("job_key", jobKey);
        json.put("trigger_key", triggerKey);
        json.put("start", false);
        
        RequestBody body    = RequestBody.create( MediaType.parse("application/json"), json.toString());
        OkHttpClient client = new OkHttpClient();
        client.setConnectTimeout(2000,TimeUnit.SECONDS);
        client.setReadTimeout(2000,TimeUnit.SECONDS);
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
    
    
}
