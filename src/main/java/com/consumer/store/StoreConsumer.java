package com.consumer.store;

import com.amazon.SellingPartnerAPIAA.AWSAuthenticationCredentials;
import com.amazon.SellingPartnerAPIAA.AWSSigV4Signer;
import com.consumer.store.helper.GenerateRandomString;
import com.consumer.store.Exceptions.InvalidRefreshTokenException;
import com.consumer.store.helper.GenerateOrderRef;
import com.consumer.store.model.*;
import com.consumer.store.service.KafkaProducerService;
import com.google.api.client.util.DateTime;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import com.opencsv.CSVWriter;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
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
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

@Service
public class StoreConsumer extends BaseController {
    
    @Autowired
    private Environment env;
    @Autowired
    private final KafkaProducerService kafkaproducer;

    public StoreConsumer(KafkaProducerService kafkaproducer) {
        this.kafkaproducer = kafkaproducer;
    }

    public KafkaProducerService getKafkaProducer(){
     
        return this.kafkaproducer;
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
        return properties;
    }

    public Environment getEnv(){
        return this.env;
    }


    @KafkaListener(topics="gcp.store.inventory.fct.upload.0", groupId="nano_group_id")
    public void getInventoryUpload(String message) throws InterruptedException {
        try {
            // Extract JSON body of the messaget
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
            String local_folder=new GenerateRandomString(10).toString();
            Boolean client_valid=false;
            Log log=null;
            Boolean is_folder_created = false;
            String inventory_download_topic = "gcp.store.inventory.fct.download.0";
            Boolean is_inventory_download_required = false;
            JSONObject downloadMessage = new JSONObject();
            Boolean key_not_found_occured=false;

            try {
                tx = session.beginTransaction();
                Client client= (Client) session.get(Client.class,client_id);
                if(client!=null) {
                    // Set client as valid
                    client_valid=true;
                }
                else {
                    this.LogMessageWarning("Client not found : "+ message);
                }
                tx.commit();
                if(client_valid) {
                    // Switch to client DB
                    // <!-- Database connection settings -->
                    configuration.setProperties(this.SetClientDB(client.getDb_host(),client.getDb_port(),client.getDb_name(),client.getDb_user(),client.getDb_pass()));
                    SessionFactory sessionFactory1 = configuration.buildSessionFactory();
                    Session session1 = sessionFactory1.openSession();
                    Transaction tx1 = null;
                    Transaction tx2 = null;
                    Transaction tx3 = null;
                    BufferedReader br = null;
                    FileWriter outputfile =null;
                    CSVWriter writer =null;
                    try {
                        String line = "";
                        String cvsSplitBy = ",";
                        tx1 = session1.beginTransaction();
                        //Get Log details
                        log = session1.get(Log.class,log_id);
                        if(log!=null) {
                            Setting is_inventory_downloaded = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.system.is_inventory_downloaded"));
                            if(is_inventory_downloaded == null){
                                key_not_found_occured=true;
                                throw new Exception("is_inventory_downloaded key not exist");
                            }
                            Setting running_with_inventory_upload = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.system.running_with_inventory_upload"));
                            if(running_with_inventory_upload == null){
                                key_not_found_occured = true;
                                throw new Exception("is_inventory_downloaded key not exist");
                            }
                            if(is_inventory_downloaded.getValue().equals("0")){
                                downloadMessage = new JSONObject();
                                Log download_log = new Log();
                                download_log.setTopic(inventory_download_topic);
                                download_log.setName("INVENTORY_DOWNLOAD");
                                JSONArray download_log_desc = new JSONArray();
                                download_log.setStatus("queued");
                                JSONObject download_log_add= new JSONObject();
                                download_log_add.put("key","event");
                                download_log_add.put("time", Instant.now().toString());
                                download_log_add.put("value","Task added in queue");
                                download_log_desc.put(download_log_add);
                                download_log.setDescription(download_log_desc.toString());
                                download_log.setCreated_at(Timestamp.valueOf(LocalDateTime.now()));
                                download_log.setUpdated_at(Timestamp.valueOf(LocalDateTime.now()));
                                session1.persist(download_log);
                                JSONObject log_details = new JSONObject();
                                log_details.put("client_id",client_id);
                                log_details.put("log_id",download_log.getId());
                                log_details.put("pre_log_id",log_id);
                                downloadMessage.put("headers",new JSONArray());
                                downloadMessage.put("body",log_details);
                                running_with_inventory_upload.setValue("1");
                                session1.update(running_with_inventory_upload);
                                tx1.commit();
                                is_inventory_download_required = true;
                                System.out.println("inventory download will be triggered");

                            }else if(is_inventory_downloaded.getValue().equals("1")){
                            // Update status & description
                                 System.out.println("inventory upload main process running");
                                 log.setStatus("started");
                                 JSONArray log_description = new JSONArray(log.getDescription());
                                 JSONObject log_add= new JSONObject();
                                 log_add.put("key","event");
                                 log_add.put("time", Instant.now().toString());
                                 log_add.put("value","Task Started");
     
                                 log_description.put(log_add);
                                 log.setDescription(log_description.toString());
                                 session1.update(log);
                                 is_inventory_downloaded.setValue("0");
                                 session1.update(is_inventory_downloaded);
                                 running_with_inventory_upload.setValue("0");
                                 session1.update(running_with_inventory_upload);
                                 tx1.commit();

                                 tx2 = session1.beginTransaction();
                                 // parse json
                                 JSONArray input_files_array = new JSONArray(log.getInput_files());
                                 if(input_files_array.get(0)!=null) {
                                     JSONObject input_file = new JSONObject(input_files_array.get(0).toString()); // get the first file to process for this task
                                     // Check all valid file details
                                     if(input_file.get("gcp_file_name")==null || input_file.get("gcp_log_folder_name")==null || input_file.get("gcp_task_folder_name")==null) {
                                         throw new Exception("Not valid file information to process");
                                     }
                                     // Download CSV file to process
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
                                     is_folder_created=true;
                                     br = new BufferedReader(new FileReader(csv_file));
                                     // create FileWriter object with file as parameter
                                     outputfile = new FileWriter(csv_file_report);
                                     // create CSVWriter object filewriter object as parameter
                                     writer = new CSVWriter(outputfile);
                                     // adding header to csv
                                     String[] header = { "sku", "price", "qty","location","status","report" };
                                     writer.writeNext(header);
                                     Integer count=0;
                                     while ((line = br.readLine()) != null) {
                                         if(count>0) {
                                             String[] row= null;
                                             // use comma as separator
                                             String[] item_row = line.split(cvsSplitBy);
                                             // process / clean fields
                                             String sku = null;
                                             String price = null;
                                             String qty=null;
                                             String location=null;
                                             String status=null;
                                             if(item_row[0]!=null) {
                                                 sku=item_row[0].replace("\"","").trim();
                                             }
                                             if(item_row[1]!=null) {
                                                 price=item_row[1].replace("\"","").trim();
                                             }
                                             if(item_row[2]!=null) {
                                                 qty=item_row[2].replace("\"","").trim();
                                             }
                                             if(item_row[3]!=null) {
                                                 location=item_row[3].replace("\"","").trim();
                                             }
                                             if(item_row[4]!=null) {
                                                 status=item_row[4].replace("\"","").trim();
                                             }
                                             if(sku.isEmpty() || price.isEmpty() || qty.isEmpty() || status.isEmpty()) {
                                                 row = new String[]{sku,price, qty, location,status, "failed : validation error"};
                                             }
                                             if(Double.valueOf(price)<=Double.valueOf("0.0") || Integer.valueOf(qty)<0){
                                                row = new String[]{sku,price, qty, location,status, "failed : validation error"};
                                             }
                                             else {
                                                 // find Item by sku
                                                 Item item = session1.bySimpleNaturalId(Item.class).load(sku);
     
                                                 if(item!=null) {
                                                     try {
                                                         //System.out.println(item.getId());
                                                         // Update price , Qty , location , status (extra validation may require)
                                                         item.setPrice(Double.valueOf(price));
                                                         item.setQuantity(Integer.valueOf(qty));
                                                         item.setInventory_location(location);
                                                         item.setNewly_added(false);
                                                         if(Integer.valueOf(status) == 1) {
                                                             item.setActive(Byte.valueOf("1"));
     
                                                         }
                                                         else {
                                                             item.setActive(Byte.valueOf("0"));
                                                         }
                                                         item.setInventory_updated_at(Timestamp.valueOf(LocalDateTime.now()));
                                                         session1.update(item);
                                                         row = new String[]{sku,price, qty, location,status, "success"};
                                                     }
                                                     catch (Exception e) {
                                                         row = new String[]{sku,price, qty, location,status, "failed : error in field"};
                                                         //e.printStackTrace();
                                                     }
                                                 }
                                                 else {
                                                     //System.out.println("sku not found : "+item_row[0]);
                                                     row = new String[]{sku,price, qty, location,status, "failed : sku not found"};
                                                 }
                                             }
     
                                             writer.writeNext(row);
                                         }
     
                                         count++;
     
                                     }
                                     // close report file
                                     if(writer!=null) {
                                         //closing writer connection
                                         writer.close();
                                         outputfile.close();
                                         writer=null;
                                     }
     
                                     // Generate log & upload to GCS
     
                                     // upload report to gcs
                                     BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+input_file.get("gcp_log_folder_name").toString()+"/"+input_file.get("gcp_task_folder_name").toString()+"/"+"report-"+input_file.get("gcp_file_name").toString());
                                     BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                                     storage.create(blobInfo, Files.readAllBytes(Paths.get(csv_file_report)));
     
                                     // Update output file in db
                                     JSONArray output_files_array = new JSONArray();
                                     JSONObject output_file_object= new JSONObject();
                                     output_file_object.put("gcp_file_name","report-"+input_file.get("gcp_file_name").toString());
                                     output_file_object.put("gcp_log_folder_name", input_file.get("gcp_log_folder_name").toString());
                                     output_file_object.put("gcp_task_folder_name",input_file.get("gcp_task_folder_name").toString());
     
                                     output_files_array.put(output_file_object);
                                     log.setOutput_files(output_files_array.toString());
                                     session1.update(log);
     
                                 }
                                 // Update status & description
                                 // Update task as completed
                                 log.setStatus("completed");
                                 //JSONArray log_description1 = new JSONArray(log.getDescription());
                                 JSONObject log_add1= new JSONObject();
                                 log_add1.put("key","event");
                                 log_add1.put("time", Instant.now().toString());
                                 log_add1.put("value","Task Completed");
     
                                 log_description.put(log_add1);
                                 log.setDescription(log_description.toString());
                                 Log new_log=log.getClone();
                                 new_log.setCreated_at(Timestamp.valueOf(LocalDateTime.now()));
                                 new_log.setUpdated_at(Timestamp.valueOf(LocalDateTime.now()));
                                 session1.delete(log);
                                 session1.persist(new_log);
                                 tx2.commit();
                              }
                         }
                        
                        //System.out.println("Done");
                        // Success
                        this.LogMessageInfo("processed : "+ message);
                    } catch (Exception e) {
                        if (tx2 != null) tx2.rollback();
                        if(key_not_found_occured==true)tx1.commit();
                        //e.printStackTrace();
                        // Log client DB issue
                        if(log!=null && log.getId()!=null) {
                            tx3 = session1.beginTransaction();
                            log.setStatus("failed");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", Instant.now().toString());
                            log_add.put("value","Error : "+ e.getMessage());

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                            tx3.commit();
                        }
                        else {
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

                        // Delete downloaded file after successfully processed - recursively
                        if(is_folder_created){
                           FileUtils.deleteQuietly(new File(local_folder));
                        }
                        session1.close();
                    }
                }
            }

            catch (Exception e) {
                //if (tx!=null) tx.rollback();
                //e.printStackTrace();
                // log primary DB issue
                this.LogMessageError("Primary DB issue : "+e.getMessage());
            } finally {
                session.close();
                if(is_inventory_download_required!=false && downloadMessage.length()!=0){
                    kafkaproducer.sendMessage(downloadMessage.toString(),inventory_download_topic);
                }
            }

        } catch (Exception e) {
            //e.printStackTrace();
            // Store Exception for future ref & update message as failed
            this.LogMessageError("Error in message : "+ message+" :Error: "+ e.getMessage());
        }
    }

    @KafkaListener(topics="gcp.store.inventory.fct.download.0", groupId="nano_group_id")
    public void getInventoryDownload(String message) throws InterruptedException {
        try {
            // Extract JSON body of the message
            String body = new JSONObject(message).getString("body");
            JSONObject json = new JSONObject(body);
            BigInteger client_id= BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id= BigInteger.valueOf(json.getInt("log_id"));
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");  
            // Search DB to get client DB access details
            Configuration configuration = new Configuration();
            configuration.configure();
            // set master db from env setup
            configuration.setProperties(this.SetMasterDB());
            SessionFactory sessionFactory = configuration.buildSessionFactory();
            Session session = sessionFactory.openSession();
            Transaction tx = null;
            String local_folder=new GenerateRandomString(10).toString();
            String local_file=  "inventory-" + LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter) + "-"  + new GenerateRandomString(5).toString()+".csv";
            Boolean client_valid=false;
            Log log=null;
            Boolean is_running_with_inventory_upload = false;
            String inventory_upload_topic = "gcp.store.inventory.fct.upload.0";
            JSONObject uploadMessage = new JSONObject();
            Boolean key_not_found_occured = false;


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
                    Transaction tx1 = null;
                    Transaction tx2 = null;
                    Transaction tx3 = null;
                    BufferedReader br = null;
                    FileWriter outputfile =null;
                    CSVWriter writer =null;
                    try {
                        String line = "";
                        String cvsSplitBy = ",";
                        tx1 = session1.beginTransaction();
                        //Get Log details
                        log = session1.get(Log.class,log_id);
                        if(log!=null) {
                            // Update status & description
                            Setting is_inventory_downloaded = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.system.is_inventory_downloaded"));
                            if(is_inventory_downloaded == null){
                                key_not_found_occured=true;
                                throw new Exception("is_inventory_downloaded key not exist");
                            }
                            Setting running_with_inventory_upload = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.system.running_with_inventory_upload"));
                            if(running_with_inventory_upload == null){
                                key_not_found_occured=true;
                                throw new Exception("is_inventory_downloaded key not exist");
                            }
                            if(running_with_inventory_upload.getValue().equals("1")){
                                BigInteger pre_log_id= BigInteger.valueOf(json.getInt("pre_log_id"));
                                is_running_with_inventory_upload=true;
                                JSONObject log_details = new JSONObject();
                                log_details.put("client_id",client_id);
                                log_details.put("log_id",pre_log_id);
                                uploadMessage.put("header",new JSONArray());
                                uploadMessage.put("body",log_details);
                            }
                            log.setStatus("started");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", Instant.now().toString());
                            log_add.put("value","Task Started");

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                            tx1.commit();
                            tx2 = session1.beginTransaction();

                            Storage storage = StorageOptions.newBuilder()
                                    .setProjectId(env.getProperty("spring.gcp.project-id"))
                                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                    .build()
                                    .getService();

                            // Create Random directory to store the file
                            Files.createDirectories(Paths.get(local_folder));
                            // generate file name
                            String csv_file_report=local_folder+"/"+ local_file;
                            outputfile = new FileWriter(csv_file_report);
                            // create CSVWriter object filewriter object as parameter
                            writer = new CSVWriter(outputfile);
                            // adding header to csv
                            String[] header = { "sku", "price", "qty","location","status"};
                            writer.writeNext(header);
                            Integer count=0;
                            Integer pull=100;
                            Query query = session1.createQuery("From Item where sku!=:sku ORDER BY id ASC");
                            query.setParameter("sku", "undefined");
                            
                            while(true) {
                                String[] row = null;
                                query.setFirstResult(count);
                                query.setMaxResults(pull);
                                List<Item> item_list = query.list();
                                if(item_list.isEmpty()) {
                                    break;
                                }
                                // loop through item list
                                Integer row_count=0;
                                for(Item item: item_list) {
                                    row = new String[]{item.getSku(), item.getPrice().toString(), item.getQuantity().toString(), item.getInventory_location()==null? "" : item.getInventory_location(), item.getActive().toString()};
                                    writer.writeNext(row);
                                    row_count++;
                                }
                                   count+=row_count;
                            }

                                // close report file
                                if(writer!=null) {
                                    //closing writer connection
                                    writer.close();
                                    outputfile.close();
                                    writer=null;
                                }

                                // Generate log & upload to GCS

                                // upload report to gcs
                                BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+env.getProperty("spring.gcp.log-folder-name")+"/"+local_folder+"/"+local_file);
                                BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                                storage.create(blobInfo, Files.readAllBytes(Paths.get(csv_file_report)));

                                // Update output file in db
                                JSONArray output_files_array = new JSONArray();
                                JSONObject output_file_object= new JSONObject();
                                output_file_object.put("gcp_file_name",local_file);
                                output_file_object.put("gcp_log_folder_name", "logs");
                                output_file_object.put("gcp_task_folder_name",local_folder);

                                output_files_array.put(output_file_object);
                                log.setOutput_files(output_files_array.toString());
                                session1.update(log);


                            // Update status & description
                            // Update task as completed
                            log.setStatus("completed");
                            //JSONArray log_description1 = new JSONArray(log.getDescription());
                            JSONObject log_add1= new JSONObject();
                            log_add1.put("key","event");
                            log_add1.put("time", Instant.now().toString());
                            log_add1.put("value","Task Completed");

                            log_description.put(log_add1);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                            is_inventory_downloaded.setValue("1");
                            session1.update(is_inventory_downloaded);
                            tx2.commit();
                            
                        }
                        
                        //System.out.println("Done");
                        // success
                        this.LogMessageInfo("processed : "+ message);
                    } catch (Exception e) {
                        if (tx2 != null) tx2.rollback();
                        if(key_not_found_occured==true)tx1.commit();
                        //e.printStackTrace();
                        // Log client DB issue
                        if(log!=null && log.getId()!=null) {
                            tx3 = session1.beginTransaction();
                            log.setStatus("failed");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", Instant.now().toString());
                            log_add.put("value","Error : "+ e.getMessage());

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                            tx3.commit();
                        }
                        else {
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

                        // Delete downloaded file after successfully processed - recursively
                        FileUtils.deleteQuietly(new File(local_folder));
                        session1.close();
                        
                    }
                }
            }

            catch (Exception e) {
                //if (tx!=null) tx.rollback();
                //e.printStackTrace();
                // log primary DB issue
                this.LogMessageError("Primary DB issue : "+e.getMessage());

            } finally {
                session.close();
                if(is_running_with_inventory_upload == true && uploadMessage.length()!=0){
                    kafkaproducer.sendMessage(uploadMessage.toString(),inventory_upload_topic);
                }
            }


        } catch (Exception e) {
            //e.printStackTrace();
            // Store Exception for future ref & update message as failed
            this.LogMessageError("Error in message : "+ message+" :Error: "+ e.getMessage());
        }
    }

    @KafkaListener(topics="gcp.store.order.fct.upload.0", groupId="nano_group_id")
    public void getOrderUpload(String message) throws InterruptedException {
        try {
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
            String local_folder=new GenerateRandomString(10).toString();
            Boolean client_valid=false;
            Log log=null;
            String client_state=null;

            try {
                tx = session.beginTransaction();
                Client client= (Client) session.get(Client.class,client_id);
                if(client!=null) {
                    // Set client as valid
                    client_valid=true;
                    client_state= client.getState();
                }
                else {
                    this.LogMessageWarning("Client not found : "+ message);
                }
                tx.commit();
                if(client_valid) {
                    // Switch to client DB
                    // <!-- Database connection settings -->
                    configuration.setProperties(this.SetClientDB(client.getDb_host(),client.getDb_port(),client.getDb_name(),client.getDb_user(),client.getDb_pass()));
                    SessionFactory sessionFactory1 = configuration.buildSessionFactory();
                    Session session1 = sessionFactory1.openSession();
                    Transaction tx1 = null;
                    Transaction tx2 = null;
                    Transaction tx3 = null;
                    BufferedReader br = null;
                    FileWriter outputfile =null;
                    CSVWriter writer =null;
                    try {
                        String line = "";
                        String cvsSplitBy = ",";
                        tx1 = session1.beginTransaction();
                        //Get Log details
                        log = session1.get(Log.class,log_id);
                        if(log!=null) {
                            // Update status & description
                            log.setStatus("started");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", Instant.now().toString());
                            log_add.put("value","Task Started");

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                            tx1.commit();
                            tx2 = session1.beginTransaction();
                            // parse json
                            JSONArray input_files_array = new JSONArray(log.getInput_files());
                            if(input_files_array.get(0)!=null) {
                                JSONObject input_file = new JSONObject(input_files_array.get(0).toString()); // get the first file to process for this task
                                // Check all valid file details
                                if(input_file.get("gcp_file_name")==null || input_file.get("gcp_log_folder_name")==null || input_file.get("gcp_task_folder_name")==null) {
                                    throw new Exception("Not valid file information to process");
                                }
                                // Download CSV file to process
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
                                br = new BufferedReader(new FileReader(csv_file));

                                Integer count=0;
                                List<Order> orders= new ArrayList<>();
                                while ((line = br.readLine()) != null) {
                                    if(count>0 && !line.trim().isEmpty()) {
                                        //String[] row= null;
                                        // use comma as separator
                                        String[] item_row = line.split(cvsSplitBy);
                                        // process / clean fields
                                        String order_ref = null;
                                        String invoice_number = null;
                                        String invoice_date=null;
                                        String order_date=null;
                                        String customer_phone=null;
                                        String customer_email=null;
                                        String customer_name=null;
                                        String customer_pincode = null;
                                        String customer_address = null;
                                        String customer_state=null;
                                        Boolean prime_customer=false;
                                        String customer_gst=null;
                                        String buyer_name=null;
                                        String buyer_address=null;
                                        String buyer_pincode=null;
                                        String buyer_state=null;
                                        String buyer_phone=null;
                                        String item_sku = null;
                                        String unit_price = null;
                                        String qty=null;
                                        String item_discount=null;
                                        String sales_channel=null;
                                        String shipping_cost="0.0";
                                        String tracking_info=null;
                                        String order_type=null;
                                        Boolean priority=false;
                                        String dispatch_by=null;
                                        String customer_type = null;

                                        if(item_row[0]!=null) {
                                            order_ref=item_row[0].replace("\"","").trim();
                                        }
                                        if(item_row[1]!=null) {
                                            invoice_number=item_row[1].replace("\"","").trim();
                                        }
                                        if(item_row[2]!=null) {
                                            invoice_date=item_row[2].replace("\"","").trim();
                                        }
                                        if(item_row[3]!=null) {
                                            order_date=item_row[3].replace("\"","").trim();
                                            //format date
                                            String new_order_date_time[]=order_date.split(" ");
                                            if(new_order_date_time.length==1) {
                                                String new_order_date[]=new_order_date_time[0].split("-");
                                                if (new_order_date.length==3) {
                                                    order_date= new_order_date_time[0] +" 00:00:00";
                                                }
                                                else {
                                                    order_date=null;
                                                }
                                            }
                                            else {
                                                String new_order_date[]=new_order_date_time[0].split("-");
                                                if (new_order_date.length!=3) {
                                                    order_date=null;
                                                }
                                            }
                                        }
                                        if(item_row[4]!=null) {
                                            customer_phone=item_row[4].replace("\"","").trim();
                                        }
                                        if(item_row[5]!=null) {
                                            customer_email=item_row[5].replace("\"","").trim();
                                        }
                                        if(item_row[6]!=null) {
                                            customer_name=item_row[6].replace("\"","").trim();
                                        }
                                        if(item_row[7]!=null) {
                                            customer_pincode=item_row[7].replace("\"","").trim();
                                        }
                                        if(item_row[8]!=null) {
                                            customer_address=item_row[8].replace("\"","").trim();
                                        }
                                        if(item_row[9]!=null) {
                                            customer_state=item_row[9].replace("\"","").trim();
                                        }

                                        if(item_row[10]!=null) {
                                            customer_gst=item_row[10].replace("\"","").trim();
                                        }

                                        if(item_row[11]!=null) {
                                            customer_type=item_row[11].replace("\"","").trim();
                                            if( customer_type.equalsIgnoreCase("b2b") || customer_type.equalsIgnoreCase("b2c")){
                                            }else{
                                                customer_type = null;
                                            }
                                        }

                                        prime_customer=customer_gst=="" || customer_gst==null ? false : true;

                                        if(item_row[12]!=null) {
                                            buyer_name=item_row[12].replace("\"","").trim();
                                        }
                                        if(item_row[13]!=null) {
                                            buyer_address=item_row[13].replace("\"","").trim();
                                        }
                                        if(item_row[14]!=null) {
                                            buyer_pincode=item_row[14].replace("\"","").trim();
                                        }
                                        if(item_row[15]!=null) {
                                            buyer_state=item_row[15].replace("\"","").trim();
                                        }
                                        if(item_row[16]!=null) {
                                            buyer_phone=item_row[16].replace("\"","").trim();
                                        }

                                        if(item_row[17]!=null) {
                                            item_sku=item_row[17].replace("\"","").trim();
                                        }
                                        if(item_row[18]!=null) {
                                            unit_price=item_row[18].replace("\"","").trim();
                                        }
                                        if(item_row[19]!=null) {
                                            qty=item_row[19].replace("\"","").trim();
                                        }
                                        if(item_row[20]!=null) {
                                            item_discount=item_row[20].replace("\"","").trim();
                                        }
                                        if(item_row[21]!=null) {
                                            sales_channel=item_row[21].replace("\"","").trim();
                                        }

                                        if(item_row[22]!=null && !item_row[22].isEmpty()) {
                                            shipping_cost=item_row[22].replace("\"","").trim();
                                        }
                                        if(item_row[23]!=null) {
                                            tracking_info=item_row[23].replace("\"","").trim();
                                        }
                                        if(item_row[24]!=null) {
                                            order_type=item_row[24].replace("\"","").trim();
                                        }
                                        if(item_row[25]!=null) {
                                            dispatch_by=item_row[25].replace("\"","").trim();
                                            //format date
                                            String new_order_date_time[]=dispatch_by.split(" ");
                                            if(new_order_date_time.length==1) {
                                                String new_order_date[]=new_order_date_time[0].split("-");
                                                if (new_order_date.length==3) {
                                                    dispatch_by= new_order_date_time[0] +" 00:00:00";
                                                }
                                                else {
                                                    dispatch_by=null;
                                                }
                                            }
                                            else {
                                                String new_order_date[]=new_order_date_time[0].split("-");
                                                if (new_order_date.length!=3) {
                                                    dispatch_by=null;
                                                }
                                            }
                                        }
                                        if(item_row[26]!=null) {
                                            priority=item_row[26].replace("\"","").trim().toUpperCase()=="FALSE"?false:true;
                                        }
                                        Order order = new Order();
                                        order.setCsv_row_number(count);
                                        order.setActual_csv_row_data(line);
                                        //System.out.println(count);
                                        if(order_ref.isEmpty()) {
                                            //row = Arrays.copyOf(item_row, item_row.length + 1);
                                            //row[row.length-1] = "failed : validation error";
                                            order.setOrder_ref(order_ref);
                                            order.setMessage("Failed : Validation error");
                                            order.setIs_valid(false);
                                            orders.add(order);
                                        }
                                        else if (order_date.isEmpty() || customer_phone.isEmpty() || customer_name.isEmpty() || item_sku.isEmpty() || unit_price.isEmpty() || qty.isEmpty() || sales_channel.isEmpty() || order_type.isEmpty() || dispatch_by.isEmpty() ) {
                                            //System.out.println(count);
                                            String finalOrder_ref = order_ref;
                                            order = orders.stream()
                                                    .filter(order_check -> finalOrder_ref.equals(order_check.getOrder_ref()))
                                                    .findAny()
                                                    .orElse(null);
                                            //System.out.println(order);
                                            if(order==null) {
                                                order= new Order();
                                                order.setOrder_ref(order_ref);
                                                order.setLineitems(new ArrayList<>()); // add blank array for sku
                                                order.setMessage("failed : validation error");
                                                order.setIs_valid(false);
                                                LineItem lineItem= new LineItem();
                                                lineItem.setCsv_row_number(count);
                                                lineItem.setIs_valid(false);
                                                order.getLineitems().add(lineItem);
                                                orders.add(order);
                                            }
                                            else {
                                                //System.out.println("Error - "+ count);
                                                Integer position= orders.indexOf(order);
                                                orders.get(position).setIs_valid(false);
                                                LineItem lineItem= new LineItem();
                                                lineItem.setCsv_row_number(count);
                                                lineItem.setActual_csv_row_data(line);
                                                lineItem.setMessage("failed : validation error");
                                                lineItem.setIs_valid(false);
                                                orders.get(position).getLineitems().add(lineItem);
                                                orders.get(position).setMessage("failed : validation error");
                                            }
                                        }
                                        else {
                                            //Associate line item to the order
                                            // Add line item
                                            LineItem lineItem= new LineItem();
                                            Item item= new Item();
                                            item.setSku(item_sku);
                                            lineItem.setItem(item);
                                            lineItem.setQuantity(Integer.valueOf(qty));
                                            lineItem.setUnit_price(Double.valueOf(unit_price));
                                            lineItem.setCsv_row_number(count);
                                            // Search customer based on phone number
                                            Customer customer = session1.bySimpleNaturalId(Customer.class).load(customer_phone);
                                            if(customer == null) {
                                                //set customer
                                                customer= new Customer();
                                                customer.setCustomer_type("store");
                                                customer.setName(customer_name);
                                                customer.setEmail(customer_email);
                                                customer.setPhone(customer_phone);
                                                customer.setAddress("[{\"key\": \"address\", \"value\": \""+ customer_address+"\"},{\"key\": \"pin\", \"value\":\""+ customer_pincode +"\"}]");
                                                customer.setGstin(customer_gst);
                                                customer.setState(customer_state);
                                                customer.setPin_code(customer_pincode);
                                                customer.setIs_prime(prime_customer);
                                                customer.setCustomer_type( customer_type );
                                            }

                                            String finalOrder_ref = order_ref;
                                            order = orders.stream()
                                                    .filter(order_check -> finalOrder_ref.equals(order_check.getOrder_ref()))
                                                    .findAny()
                                                    .orElse(null);

                                            if(order==null) {
                                                    order= new Order();
                                                    order.setOrder_ref(order_ref);
                                                    order.setOrder_date(Timestamp.valueOf(order_date));
                                                    order.setSales_channel(sales_channel);
                                                    order.setStatus("unshipped");
                                                    order.setDelivery_amount(Double.valueOf(shipping_cost));
                                                    order.setBuyer_name(buyer_name);
                                                    order.setBuyer_phone(buyer_phone);
                                                    order.setBuyer_address(buyer_address);
                                                    order.setBuyer_pincode(buyer_pincode);
                                                    order.setBuyer_state(buyer_state);
                                                    order.setInvoice_number(invoice_number);
                                                    order.setOrder_type(order_type);
                                                    order.setPriority(priority);
                                                    order.setDispatch_by(Timestamp.valueOf(dispatch_by));
                                                    order.setShipping("[{\"ref\": \""+ tracking_info+"\"}]");
                                                    order.setLineitems(new ArrayList<>());
                                                    //order.getLineitems().add(lineItem);
                                                    order.setCustomer(customer);
                                                    order.setIs_valid(true);
                                                    orders.add(order);
                                            }
                                            // Add line item as it already present
                                            Integer order_position= orders.indexOf(order);
                                            //orders.get(order_position).getLineitems().add(lineItem);
                                            // loop through all existing lineitm & add new one
                                            //this.LogMessageInfo("order_position : "+ order_position);
                                            //this.LogMessageInfo("orders : "+ orders.size());
                                            List<LineItem> existing_line= orders.get(order_position).getLineitems();
                                            existing_line.add(lineItem);
                                            orders.get(order_position).setLineitems(existing_line);
                                            // validate SKU & Stock
                                            Item item_check = session1.bySimpleNaturalId(Item.class).load(item_sku);
                                            Integer lineitem_position= orders.get(order_position).getLineitems().indexOf(lineItem);
                                            if(item_check!=null) {
                                                orders.get(order_position).getLineitems().get(lineitem_position).setItem(item_check);
                                               if(item_check.getQuantity() >= Integer.valueOf(qty)) {
                                                   orders.get(order_position).getLineitems().get(lineitem_position).setIs_valid(true);
                                                   Double total_with_tax = lineItem.getUnit_price()* lineItem.getQuantity();
                                                   //Double lineitem_tax= total_without_tax * (Double.valueOf(item_check.getProduct().getTax_rate()) / 100);
                                                   Double lineitem_tax= total_with_tax-(total_with_tax * (100/(100+Double.valueOf(item_check.getProduct().getTax_rate()))));
                                                   // recalculate GST
                                                   Double total_igst=0.0;
                                                   Double total_cgst=0.0;
                                                   Double total_sgst=0.0;
                                                   Double sum_total=0.0;
                                                   // check shipping state & seller state is same or not
                                                   if(client_state.strip().equalsIgnoreCase(customer_state.strip())) {
                                                       // calculate cgst & sgst both
                                                       total_cgst= orders.get(order_position).getTotal_cgst() + (lineitem_tax/2);
                                                       total_sgst = orders.get(order_position).getTotal_sgst() + (lineitem_tax/2);

                                                   }
                                                   else {
                                                       // calculate igst
                                                       total_igst= orders.get(order_position).getTotal_igst() + lineitem_tax;
                                                   }
                                                   sum_total= orders.get(order_position).getSum_total() + total_with_tax ;
                                                   orders.get(order_position).setTotal_cgst(total_cgst);
                                                   orders.get(order_position).setTotal_sgst(total_sgst);
                                                   orders.get(order_position).setTotal_igst(total_igst);
                                                   orders.get(order_position).setSum_total(sum_total);
                                               }
                                               else {
                                                   orders.get(order_position).getLineitems().get(lineitem_position).setIs_valid(false);
                                                   orders.get(order_position).getLineitems().get(lineitem_position).setMessage("Error : Low Stock");
                                                   orders.get(order_position).setIs_valid(false);
                                                   orders.get(order_position).setMessage("Error : Child item is not valid");
                                               }
                                            }
                                            else {
                                                orders.get(order_position).getLineitems().get(lineitem_position).setIs_valid(false);
                                                orders.get(order_position).getLineitems().get(lineitem_position).setMessage("Error : SKU not found");
                                                orders.get(order_position).setIs_valid(false);
                                                orders.get(order_position).setMessage("Error : Child item is not valid");
                                            }

                                        }

                                       // writer.writeNext(row);
                                    }

                                    count++;

                                }

                                // loop again to Process orders
                                count=0;

                                for(Order order : orders) {
                                    // Process order only that is valid
                                    if(order.getIs_valid()) {
                                        // Process each order & flag success or fail
                                        Order order_subject = session1.bySimpleNaturalId(Order.class).load(order.getOrder_ref());
                                        if(order_subject==null) {
                                            // Insert order with line items
                                            try {
                                                session1.persist(order);
                                                // deduct stock for the item when success
                                                for(LineItem litm: order.getLineitems()) {
                                                    Item itm= litm.getItem();
                                                    itm.setQuantity(itm.getQuantity()-litm.getQuantity());
                                                    itm.setInventory_updated_at(Timestamp.valueOf(LocalDateTime.now()));
                                                    session1.update(itm);
                                                }

                                                orders.get(count).setIs_success(true);
                                                orders.get(count).setMessage("Success");
                                            }
                                            catch (Exception ex) {
                                                //ex.printStackTrace();
                                                //System.out.println(ex.getMessage());
                                                orders.get(count).setIs_success(false);
                                                orders.get(count).setMessage(ex.getMessage());
                                            }
                                        }
                                        else {
                                            orders.get(count).setIs_success(false);
                                            orders.get(count).setMessage("Error : Duplicate record");

                                        }
                                    }

                                    count++;
                                }


                                // write report
                                // create FileWriter object with file as parameter
                                outputfile = new FileWriter(csv_file_report);
                                // create CSVWriter object filewriter object as parameter
                                writer = new CSVWriter(outputfile);
                                // adding header to csv
                                String[] header = { "order ref", "actual row number", "item sku" , "is valid", "success", "message" };
                                writer.writeNext(header);
                                // loop through orders
                                for(Order order : orders) {
                                    // loop through line item
                                    for(LineItem lt: order.getLineitems()) {
                                        String[] row = {order.getOrder_ref(),lt.getCsv_row_number().toString(), lt.getIs_valid() ? lt.getItem().getSku() : "" , lt.getIs_valid().toString(), order.getIs_success().toString(),order.getMessage()+ "/" + lt.getMessage()};
                                        writer.writeNext(row);
                                    }
                                }
                                // close report file
                                if(writer!=null) {
                                    //closing writer connection
                                    writer.close();
                                    outputfile.close();
                                    writer=null;
                                }


                                // Generate log & upload to GCS

                                // upload report to gcs
                                BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+input_file.get("gcp_log_folder_name").toString()+"/"+input_file.get("gcp_task_folder_name").toString()+"/"+"report-"+input_file.get("gcp_file_name").toString());
                                BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                                storage.create(blobInfo, Files.readAllBytes(Paths.get(csv_file_report)));

                                // Update output file in db
                                JSONArray output_files_array = new JSONArray();
                                JSONObject output_file_object= new JSONObject();
                                output_file_object.put("gcp_file_name","report-"+input_file.get("gcp_file_name").toString());
                                output_file_object.put("gcp_log_folder_name", input_file.get("gcp_log_folder_name").toString());
                                output_file_object.put("gcp_task_folder_name",input_file.get("gcp_task_folder_name").toString());

                                output_files_array.put(output_file_object);
                                log.setOutput_files(output_files_array.toString());
                                session1.update(log);


                            }

                            // Update status & description
                            // Update task as completed
                            log.setStatus("completed");
                            //JSONArray log_description1 = new JSONArray(log.getDescription());
                            JSONObject log_add1= new JSONObject();
                            log_add1.put("key","event");
                            log_add1.put("time", Instant.now().toString());
                            log_add1.put("value","Task Completed");

                            log_description.put(log_add1);
                            log.setDescription(log_description.toString());
                            session1.update(log);

                        }
                        tx2.commit();
                        //System.out.println("Done");
                        // Success
                        this.LogMessageInfo("processed : "+ message);
                    } catch (Exception e) {
                        if (tx2 != null) tx2.rollback();
                        e.printStackTrace();
                        // Log client DB issue
                        if(log!=null && log.getId()!=null) {
                            tx3 = session1.beginTransaction();
                            log.setStatus("failed");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", Instant.now().toString());
                            log_add.put("value","Error : "+ e.getMessage());

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                            tx3.commit();
                        }
                        else {
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

                        // Delete downloaded file after successfully processed - recursively
                        FileUtils.deleteQuietly(new File(local_folder));
                        session1.close();
                    }
                }
            }

            catch (Exception e) {
                //if (tx!=null) tx.rollback();
                //e.printStackTrace();
                // log primary DB issue
                this.LogMessageError("Primary DB issue : "+e.getMessage());
            } finally {
                session.close();
            }

        } catch (Exception e) {
            //e.printStackTrace();
            // Store Exception for future ref & update message as failed
            this.LogMessageError("Error in message : "+ message+" :Error: "+ e.getMessage());
        }
    }
    @KafkaListener(topics="gcp.store.transaction.fct.upload.0", groupId="nano_group_id")
    public void getTransactionUpload(String message) throws InterruptedException {
        try {
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
            String local_folder=new GenerateRandomString(10).toString();
            Boolean client_valid=false;
            Log log=null;
            String client_state=null;

            try {
                tx = session.beginTransaction();
                Client client= (Client) session.get(Client.class,client_id);
                if(client!=null) {
                    // Set client as valid
                    client_valid=true;
                    client_state= client.getState();
                }
                else {
                    this.LogMessageWarning("Client not found : "+ message);
                }
                tx.commit();
                if(client_valid) {
                    // Switch to client DB
                    // <!-- Database connection settings -->
                    configuration.setProperties(this.SetClientDB(client.getDb_host(),client.getDb_port(),client.getDb_name(),client.getDb_user(),client.getDb_pass()));
                    SessionFactory sessionFactory1 = configuration.buildSessionFactory();
                    Session session1 = sessionFactory1.openSession();
                    Transaction tx1 = null;
                    Transaction tx2 = null;
                    Transaction tx3 = null;
                    BufferedReader br = null;
                    FileWriter outputfile =null;
                    CSVWriter writer =null;
                    try {
                        String line = "";
                        String cvsSplitBy = ",";
                        tx1 = session1.beginTransaction();
                        //Get Log details
                        log = session1.get(Log.class, log_id);
                        if (log != null) {
                            // Update status & description
                            log.setStatus("started");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add = new JSONObject();
                            log_add.put("key", "event");
                            log_add.put("time", Instant.now().toString());
                            log_add.put("value", "Task Started");

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                            tx1.commit();
                            tx2 = session1.beginTransaction();
                            // parse json
                            JSONArray input_files_array = new JSONArray(log.getInput_files());
                            if (input_files_array.get(0) != null) {
                                JSONObject input_file = new JSONObject(input_files_array.get(0).toString()); // get the first file to process for this task
                                // Check all valid file details
                                if (input_file.get("gcp_file_name") == null || input_file.get("gcp_log_folder_name") == null || input_file.get("gcp_task_folder_name") == null) {
                                    throw new Exception("Not valid file information to process");
                                }
                                // Download CSV file to process
                                Storage storage = StorageOptions.newBuilder()
                                        .setProjectId(env.getProperty("spring.gcp.project-id"))
                                        .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                        .build()
                                        .getService();

                                Blob blob = storage.get(env.getProperty("spring.gcp.bucket-name"), client.getName() + "/" + input_file.get("gcp_log_folder_name").toString() + "/" + input_file.get("gcp_task_folder_name").toString() + "/" + input_file.get("gcp_file_name").toString());

                                // Create Random directory to store the file
                                Files.createDirectories(Paths.get(local_folder));
                                String csv_file = local_folder + "/" + input_file.get("gcp_file_name").toString();
                                String csv_file_report = local_folder + "/report-" + input_file.get("gcp_file_name").toString();
                                blob.downloadTo(Paths.get(csv_file));
                                br = new BufferedReader(new FileReader(csv_file));

                                Integer count = 0;
                                Integer actual_row_count = 0;
                                List<Order> orders = new ArrayList<>();
                                List<OrderTransaction> transactions = new ArrayList<>();
                                while ((line = br.readLine()) != null) {
                                    if (count > 0 && !line.isEmpty()) {
                                        //String[] row= null;
                                        // use comma as separator
                                        String[] item_row = line.trim().split(cvsSplitBy);
                                        //System.out.println(item_row.length);
                                        //System.out.println(Arrays.stream(item_row).toArray());
                                        // process / clean fields
                                        String order_ref = null;
                                        String type = null;
                                        String transaction_date = null;
                                        String settlement_id = null;
                                        String lineitem_id = null;
                                        String sku = null;
                                        String description = null;
                                        Double transaction_amount = 0.0;
                                        String older_order_id = null;
                                        Integer quantity=0;

                                        if (item_row[0] != null) {
                                            order_ref = item_row[0].replace("\"", "").trim();
                                        }
                                        if (item_row[1] != null) {
                                            type = item_row[1].replace("\"", "").trim();
                                        }
                                        if (item_row[2] != null) {
                                            transaction_date = item_row[2].replace("\"", "").trim();
                                            //format date
                                            String new_order_date_time[] = transaction_date.split(" ");
                                            if (new_order_date_time.length == 1) {
                                                String new_order_date[] = new_order_date_time[0].split("-");
                                                if (new_order_date.length == 3) {
                                                    transaction_date = new_order_date_time[0] + " 00:00:00";
                                                } else {
                                                    transaction_date = null;
                                                }
                                            } else {
                                                String new_order_date[] = new_order_date_time[0].split("-");
                                                if (new_order_date.length != 3) {
                                                    transaction_date = null;
                                                }
                                            }
                                        }
                                        if (item_row[3] != null) {
                                            settlement_id = item_row[3].replace("\"", "").trim();
                                        }
                                        if (item_row[4] != null) {
                                            lineitem_id = item_row[4].replace("\"", "").trim();
                                        }
                                        if (item_row[5] != null) {
                                            sku = item_row[5].replace("\"", "").trim();
                                        }
                                        if (item_row[6] != null) {
                                            description = item_row[6].replace("\"", "").trim();
                                        }
                                        if (item_row[7] != null) {
                                            older_order_id = item_row[7].replace("\"", "").trim();
                                        }
                                        if (item_row[8] != null) {
                                            quantity = Integer.valueOf(item_row[8].replace("\"", "").trim());
                                        }
                                        if (item_row[9] != null) {
                                            transaction_amount = Double.valueOf(item_row[9].replace("\"", "").trim());
                                        }

                                        OrderTransaction transaction = new OrderTransaction();
                                        actual_row_count++;
                                        transaction.setCsv_row_number(actual_row_count);
                                        transaction.setActual_csv_row_data(line);
                                        if (order_ref.isEmpty() || transaction_date.isEmpty() || type.isEmpty() || settlement_id.isEmpty() || transaction_amount == 0) {
                                            transaction.setOrder_ref(order_ref);
                                            transaction.setMessage("failed : validation error");
                                            transaction.setIs_valid(false);
                                            transactions.add(transaction);
                                            continue;
                                        } else {

                                            // validate order ref
                                            String finalOrder_ref = order_ref;
                                            Order order = orders.stream()
                                                    .filter(order_check -> finalOrder_ref.equals(order_check.getOrder_ref()))
                                                    .findAny()
                                                    .orElse(null);
                                            if (order == null) {
                                                order = session1.bySimpleNaturalId(Order.class).load(order_ref);
                                                // if order is not present in our system
                                                if (order == null) {
                                                    transaction.setOrder_ref(order_ref);
                                                    transaction.setMessage("failed : Order is not present");
                                                    transaction.setIs_valid(false);
                                                    transactions.add(transaction);
                                                    continue;
                                                } else {
                                                    orders.add(order);
                                                }
                                            }

                                            // initialize transactions
                                            transaction.setOrder(order);
                                            transaction.setOrder_ref(order_ref);
                                            transaction.setSettlement_id(settlement_id);
                                            transaction.setInitiate_date(Timestamp.valueOf(transaction_date));
                                            transaction.setService_type(type);
                                            transaction.setSum_total(transaction_amount);
                                            transaction.setNotes(description);
                                            transaction.setQuantity(quantity);
                                            // check & add line item
                                            Boolean is_vald_sku = false;
                                            BigInteger actual_lineitem_id = null;
                                            if (!sku.isEmpty()) {
                                                // find lineitem id from sku
                                                for (LineItem lineitem : order.getLineitems()) {
                                                    if (sku.equals(lineitem.getItem().getSku())) {
                                                        is_vald_sku = true;
                                                        actual_lineitem_id = lineitem.getId();
                                                        transaction.setLineitem(lineitem);
                                                        if(transaction.getService_type().equals("refund")) {
                                                            transaction.setInitiate_return(true);
                                                        }
                                                        break;
                                                    }
                                                }
                                            }
                                            if (!sku.isEmpty() && !is_vald_sku) {
                                                transaction.setMessage("failed : SKU is not present");
                                                transaction.setIs_valid(false);
                                                transactions.add(transaction);
                                                continue;
                                            }
                                            // validate transaction type debit or credit & make amount positive
                                            if (transaction_amount < 0) {
                                                transaction.setIs_debit(Byte.valueOf("1"));
                                                transaction.setSum_total(transaction_amount * (-1));
                                            }

                                            // check if linked order id is present
                                            if (!older_order_id.isEmpty() && transaction.getService_type().equals("order")) {
                                                Order old_order = session1.bySimpleNaturalId(Order.class).load(older_order_id);
                                                if (old_order == null) {
                                                    transaction.setMessage("failed : Older order ref is invalid");
                                                    transaction.setIs_valid(false);
                                                    transactions.add(transaction);
                                                    continue;
                                                }
                                                // set parent reconcile
                                                transaction.setParent_order_present(true);
                                                transaction.setParent_order(old_order);
                                                transaction.setParent_order_id(old_order.getId());
                                            }
                                            // validate transaction (if present)
                                            Query query = session1.createQuery("From OrderTransaction OT WHERE OT.settlement_id = :settlement_id AND OT.initiate_date = :transaction_date AND OT.service_type = :service_type AND OT.sum_total = :transaction_amount");
                                            query.setParameter("settlement_id", transaction.getSettlement_id());
                                            query.setParameter("transaction_date", transaction.getInitiate_date());
                                            query.setParameter("service_type", transaction.getService_type());
                                            query.setParameter("transaction_amount", transaction.getSum_total());
                                            List<OrderTransaction> transaction_list = query.list();
                                            if (transaction_list.size() > 0) {
                                                transaction.setMessage("failed : Transaction exist");
                                                transaction.setIs_valid(false);
                                                transactions.add(transaction);
                                                continue;
                                            }

                                            //success validation
                                            transaction.setIs_valid(true);
                                            transactions.add(transaction);
                                        }
                                    }

                                    count++;

                                }

                                // loop again to Process orders
                                count = 0;

                                for (OrderTransaction transaction : transactions) {
                                    // Process order only that is valid
                                    if (transaction.getIs_valid()) {

                                        // Insert transaction
                                        try {
                                            session1.persist(transaction);
                                            // check if return need to initiate
                                            if(transaction.getInitiate_return()) {
                                                Return return_order= new Return();
                                                return_order.setInitiate_date(transaction.getInitiate_date());
                                                return_order.setQuantity(transaction.getQuantity());
                                                return_order.setOrder(transaction.getOrder());
                                                return_order.setLineitem(transaction.getLineitem());
                                                return_order.setReason(transaction.getNotes());
                                                session1.persist(return_order);
                                            }
                                            // check if parent order id need to reconcile
                                            if(transaction.getParent_order_present()) {
                                                Order update_order= transaction.getOrder();
                                                update_order.setParent_order(transaction.getParent_order());
                                                update_order.setParent_reconcile_date(transaction.getInitiate_date());
                                                session1.update(update_order);
                                            }
                                            transactions.get(count).setIs_success(true);
                                            transactions.get(count).setMessage("Success");
                                        } catch (Exception ex) {
                                            //ex.printStackTrace();
                                            //System.out.println(ex.getMessage());
                                            transactions.get(count).setIs_success(false);
                                            transactions.get(count).setMessage(ex.getMessage());
                                        }
                                    }
                                    count++;
                                }

                            // write report
                            // create FileWriter object with file as parameter
                            outputfile = new FileWriter(csv_file_report);
                            // create CSVWriter object filewriter object as parameter
                            writer = new CSVWriter(outputfile);
                            // adding header to csv
                            String[] header = {"order ref", "actual row number", "type", "date", "settlement id", "transaction amount","is valid", "success", "message"};
                            writer.writeNext(header);
                            // loop through orders
                            for (OrderTransaction transaction : transactions) {
                                // loop through line item
                                String[] row = {transaction.getOrder_ref(), transaction.getCsv_row_number().toString(),transaction.getService_type() , transaction.getInitiate_date().toString() , transaction.getSettlement_id() , String.valueOf(transaction.getIs_debit().equals(Byte.valueOf("1")) ? ((-1) * transaction.getSum_total()) : transaction.getSum_total()), transaction.getIs_valid().toString(), transaction.getIs_success().toString(), transaction.getMessage() };
                                writer.writeNext(row);

                            }
                            // close report file
                            if (writer != null) {
                                //closing writer connection
                                writer.close();
                                outputfile.close();
                                writer = null;
                            }


                            // Generate log & upload to GCS

                            // upload report to gcs
                            BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName() + "/" + input_file.get("gcp_log_folder_name").toString() + "/" + input_file.get("gcp_task_folder_name").toString() + "/" + "report-" + input_file.get("gcp_file_name").toString());
                            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                            storage.create(blobInfo, Files.readAllBytes(Paths.get(csv_file_report)));

                            // Update output file in db
                            JSONArray output_files_array = new JSONArray();
                            JSONObject output_file_object = new JSONObject();
                            output_file_object.put("gcp_file_name", "report-" + input_file.get("gcp_file_name").toString());
                            output_file_object.put("gcp_log_folder_name", input_file.get("gcp_log_folder_name").toString());
                            output_file_object.put("gcp_task_folder_name", input_file.get("gcp_task_folder_name").toString());

                            output_files_array.put(output_file_object);
                            log.setOutput_files(output_files_array.toString());
                            session1.update(log);


                        }

                            // Update status & description
                            // Update task as completed
                            log.setStatus("completed");
                            //JSONArray log_description1 = new JSONArray(log.getDescription());
                            JSONObject log_add1= new JSONObject();
                            log_add1.put("key","event");
                            log_add1.put("time", Instant.now().toString());
                            log_add1.put("value","Task Completed");

                            log_description.put(log_add1);
                            log.setDescription(log_description.toString());
                            session1.update(log);

                        }
                        tx2.commit();
                        //System.out.println("Done");
                        // Success
                        this.LogMessageInfo("processed : "+ message);
                    } catch (Exception e) {
                        if (tx2 != null) tx2.rollback();
                        e.printStackTrace();
                        // Log client DB issue
                        if(log!=null && log.getId()!=null) {
                            tx3 = session1.beginTransaction();
                            log.setStatus("failed");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", Instant.now().toString());
                            log_add.put("value","Error : "+ e.getMessage());

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                            tx3.commit();
                        }
                        else {
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

                        // Delete downloaded file after successfully processed - recursively
                        FileUtils.deleteQuietly(new File(local_folder));
                        session1.close();
                    }
                }
            }

            catch (Exception e) {
                //if (tx!=null) tx.rollback();
                //e.printStackTrace();
                // log primary DB issue
                this.LogMessageError("Primary DB issue : "+e.getMessage());
            } finally {
                session.close();
            }

        } catch (Exception e) {
            //e.printStackTrace();
            // Store Exception for future ref & update message as failed
            this.LogMessageError("Error in message : "+ message+" :Error: "+ e.getMessage());
        }
    }
    @KafkaListener(topics="gcp.store.return.fct.upload.0", groupId="nano_group_id")
    public void getReturnUpload(String message) throws InterruptedException {
        try {
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
            String local_folder=new GenerateRandomString(10).toString();
            Boolean client_valid=false;
            Log log=null;
            String client_state=null;

            try {
                tx = session.beginTransaction();
                Client client= (Client) session.get(Client.class,client_id);
                if(client!=null) {
                    // Set client as valid
                    client_valid=true;
                    client_state= client.getState();
                }
                else {
                    this.LogMessageWarning("Client not found : "+ message);
                }
                tx.commit();
                if(client_valid) {
                    // Switch to client DB
                    // <!-- Database connection settings -->
                    configuration.setProperties(this.SetClientDB(client.getDb_host(),client.getDb_port(),client.getDb_name(),client.getDb_user(),client.getDb_pass()));
                    SessionFactory sessionFactory1 = configuration.buildSessionFactory();
                    Session session1 = sessionFactory1.openSession();
                    Transaction tx1 = null;
                    Transaction tx2 = null;
                    Transaction tx3 = null;
                    BufferedReader br = null;
                    FileWriter outputfile =null;
                    CSVWriter writer =null;
                    try {
                        String line = "";
                        String cvsSplitBy = ",";
                        tx1 = session1.beginTransaction();
                        //Get Log details
                        log = session1.get(Log.class, log_id);
                        if (log != null) {
                            // Update status & description
                            log.setStatus("started");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add = new JSONObject();
                            log_add.put("key", "event");
                            log_add.put("time", Instant.now().toString());
                            log_add.put("value", "Task Started");

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                            tx1.commit();
                            tx2 = session1.beginTransaction();
                            // parse json
                            JSONArray input_files_array = new JSONArray(log.getInput_files());
                            if (input_files_array.get(0) != null) {
                                JSONObject input_file = new JSONObject(input_files_array.get(0).toString()); // get the first file to process for this task
                                // Check all valid file details
                                if (input_file.get("gcp_file_name") == null || input_file.get("gcp_log_folder_name") == null || input_file.get("gcp_task_folder_name") == null) {
                                    throw new Exception("Not valid file information to process");
                                }
                                // Download CSV file to process
                                Storage storage = StorageOptions.newBuilder()
                                        .setProjectId(env.getProperty("spring.gcp.project-id"))
                                        .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                        .build()
                                        .getService();

                                Blob blob = storage.get(env.getProperty("spring.gcp.bucket-name"), client.getName() + "/" + input_file.get("gcp_log_folder_name").toString() + "/" + input_file.get("gcp_task_folder_name").toString() + "/" + input_file.get("gcp_file_name").toString());

                                // Create Random directory to store the file
                                Files.createDirectories(Paths.get(local_folder));
                                String csv_file = local_folder + "/" + input_file.get("gcp_file_name").toString();
                                String csv_file_report = local_folder + "/report-" + input_file.get("gcp_file_name").toString();
                                blob.downloadTo(Paths.get(csv_file));
                                br = new BufferedReader(new FileReader(csv_file));

                                Integer count = 0;
                                Integer actual_row_count = 0;
                                List<Order> orders = new ArrayList<>();
                                List<Return> returns = new ArrayList<>();
                                while ((line = br.readLine()) != null) {
                                    if (count > 0 && !line.isEmpty()) {
                                        //String[] row= null;
                                        // use comma as separator
                                        String[] item_row = line.trim().split(cvsSplitBy);
                                        //System.out.println(item_row.length);
                                        //System.out.println(Arrays.stream(item_row).toArray());
                                        // process / clean fields
                                        String order_ref = null;
                                        String received_date = null;
                                        String lineitem_id = null;
                                        String sku = null;
                                        String description = null;
                                        Integer quantity=0;
                                        Byte item_condition=Byte.valueOf("0");
                                        Boolean is_adjustment=false;

                                        if (item_row[0] != null) {
                                            order_ref = item_row[0].replace("\"", "").trim();
                                        }
                                        if (item_row[1] != null) {
                                            sku = item_row[1].replace("\"", "").trim();
                                        }
                                        if (item_row[2] != null) {
                                            lineitem_id = item_row[2].replace("\"", "").trim();
                                        }
                                        if (item_row[3] != null) {
                                            quantity = Integer.valueOf(item_row[3].replace("\"", "").trim());
                                        }
                                        if (item_row[4] != null) {
                                            if(item_row[4].replace("\"", "").trim().equals("1")) {
                                                item_condition=Byte.valueOf("1");
                                                is_adjustment=true;
                                            }

                                        }

                                        if (item_row[5] != null) {
                                            description = item_row[5].replace("\"", "").trim();
                                        }

                                        if (item_row[6] != null) {
                                            received_date = item_row[6].replace("\"", "").trim();
                                            //format date
                                            String new_order_date_time[] = received_date.split(" ");
                                            if (new_order_date_time.length == 1) {
                                                String new_order_date[] = new_order_date_time[0].split("-");
                                                if (new_order_date.length == 3) {
                                                    received_date = new_order_date_time[0] + " 00:00:00";
                                                } else {
                                                    received_date = null;
                                                }
                                            } else {
                                                String new_order_date[] = new_order_date_time[0].split("-");
                                                if (new_order_date.length != 3) {
                                                    received_date = null;
                                                }
                                            }
                                        }


                                        Return return_item = new Return();
                                        actual_row_count++;
                                        return_item.setCsv_row_number(actual_row_count);
                                        return_item.setActual_csv_row_data(line);
                                        if (order_ref.isEmpty() || received_date.isEmpty() || sku.isEmpty() || quantity==0) {
                                            return_item.setOrder_ref(order_ref);
                                            return_item.setMessage("Failed : Validation Error");
                                            return_item.setIs_valid(false);
                                            returns.add(return_item);
                                            continue;
                                        } else {

                                            // validate order ref
                                            String finalOrder_ref = order_ref;
                                            Order order = orders.stream()
                                                    .filter(order_check -> finalOrder_ref.equals(order_check.getOrder_ref()))
                                                    .findAny()
                                                    .orElse(null);
                                            if (order == null) {
                                                order = session1.bySimpleNaturalId(Order.class).load(order_ref);
                                                // if order is not present in our system
                                                if (order == null) {
                                                    return_item.setOrder_ref(order_ref);
                                                    return_item.setMessage("Failed : Order is not present");
                                                    return_item.setIs_valid(false);
                                                    returns.add(return_item);
                                                    continue;
                                                } else {
                                                    orders.add(order);
                                                }
                                            }

                                            // initialize transactions
                                            return_item.setOrder(order);
                                            return_item.setOrder_ref(order_ref);
                                            return_item.setReceived_date(Timestamp.valueOf(received_date));

                                            return_item.setNotes(description);
                                            return_item.setQuantity_received(quantity);
                                            // check & add line item
                                            Boolean is_vald_sku = false;
                                            BigInteger actual_lineitem_id = null;
                                            if (!sku.isEmpty()) {
                                                // find lineitem id from sku
                                                for (LineItem lineitem : order.getLineitems()) {
                                                    if (sku.equals(lineitem.getItem().getSku())) {
                                                        is_vald_sku = true;
                                                        actual_lineitem_id = lineitem.getId();
                                                        return_item.setLineitem(lineitem);
                                                        if(is_adjustment) {
                                                            return_item.setIs_adjustment(true);
                                                        }
                                                        break;
                                                    }
                                                }
                                            }
                                            if (!sku.isEmpty() && !is_vald_sku) {
                                                return_item.setMessage("failed : SKU is not present");
                                                return_item.setIs_valid(false);
                                                returns.add(return_item);
                                                continue;
                                            }
                                            // validate transaction (if present)
                                            Query query = session1.createQuery("From Return R WHERE R.order = :order_id AND R.lineitem = :lineitem_id");
                                            query.setParameter("order_id", return_item.getOrder());
                                            query.setParameter("lineitem_id", return_item.getLineitem());
                                            List<Return> return_list = query.list();
                                            if(return_list.size()==1 && return_list.get(0).getIs_received().equals(Byte.valueOf("0"))) {
                                                return_list.get(0).setReceived_date(return_item.getReceived_date());
                                                return_list.get(0).setQuantity_received(return_item.getQuantity_received());
                                                return_list.get(0).setNotes(return_item.getNotes());
                                                return_list.get(0).setIs_good_inventory(return_item.getIs_adjustment() ? Byte.valueOf("1") : Byte.valueOf("0"));
                                                return_list.get(0).setIs_inventory_adjusted(return_item.getIs_adjustment() ? Byte.valueOf("1") : Byte.valueOf("0"));
                                                return_list.get(0).setInventory_adjusted_date(return_item.getIs_adjustment() ? Timestamp.valueOf(java.time.LocalDateTime.now()) : null);
                                                return_list.get(0).setIs_received(Byte.valueOf("1"));
                                                return_list.get(0).setCsv_row_number(return_item.getCsv_row_number());
                                                return_list.get(0).setActual_csv_row_data(return_item.getActual_csv_row_data());
                                                return_list.get(0).setIs_valid(true);
                                            }
                                            else {
                                                return_item.setMessage("Failed : Return already received or not available");
                                                return_item.setIs_valid(false);
                                                returns.add(return_item);
                                                continue;
                                            }

                                            //success validation
                                            returns.add(return_list.get(0));
                                        }
                                    }

                                    count++;

                                }

                                // loop again to Process orders
                                count = 0;

                                for (Return item_return : returns) {
                                    // Process order only that is valid
                                    if (item_return.getIs_valid()) {

                                        // Insert transaction
                                        try {
                                            session1.update(item_return);
                                            // adjust inventory if good inventory received
                                            if(item_return.getIs_adjustment()) {
                                                Item updated_inventory= item_return.getLineitem().getItem();
                                                updated_inventory.setQuantity( updated_inventory.getQuantity()+item_return.getQuantity_received());
                                                updated_inventory.setInventory_updated_at(Timestamp.valueOf(LocalDateTime.now()));
                                                session1.update(updated_inventory);
                                            }
                                            returns.get(count).setIs_success(true);
                                            returns.get(count).setMessage("Success");
                                            //System.out.println("successfully updated return .......");
                                        } catch (Exception ex) {
                                            //ex.printStackTrace();
                                            //System.out.println(ex.getMessage());
                                            returns.get(count).setIs_success(false);
                                            returns.get(count).setMessage(ex.getMessage());
                                        }
                                    }
                                    count++;
                                }

                                // write report
                                // create FileWriter object with file as parameter
                                outputfile = new FileWriter(csv_file_report);
                                // create CSVWriter object filewriter object as parameter
                                writer = new CSVWriter(outputfile);
                                // adding header to csv
                                String[] header = {"order ref", "actual row number", "sku", "quantity", "condition", "received date","is valid", "success", "message"};
                                writer.writeNext(header);
                                // loop through orders
                                for (Return item_return : returns) {
                                    // loop through line item
                                    String[] row = {item_return.getOrder().getOrder_ref(), item_return.getCsv_row_number().toString(),item_return.getLineitem().getItem().getSku() , item_return.getQuantity_received().toString() , item_return.getIs_good_inventory().toString() , item_return.getReceived_date().toString(), item_return.getIs_valid().toString(), item_return.getIs_success().toString(), item_return.getMessage() };
                                    writer.writeNext(row);

                                }
                                // close report file
                                if (writer != null) {
                                    //closing writer connection
                                    writer.close();
                                    outputfile.close();
                                    writer = null;
                                }


                                // Generate log & upload to GCS

                                // upload report to gcs
                                BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName() + "/" + input_file.get("gcp_log_folder_name").toString() + "/" + input_file.get("gcp_task_folder_name").toString() + "/" + "report-" + input_file.get("gcp_file_name").toString());
                                BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                                storage.create(blobInfo, Files.readAllBytes(Paths.get(csv_file_report)));

                                // Update output file in db
                                JSONArray output_files_array = new JSONArray();
                                JSONObject output_file_object = new JSONObject();
                                output_file_object.put("gcp_file_name", "report-" + input_file.get("gcp_file_name").toString());
                                output_file_object.put("gcp_log_folder_name", input_file.get("gcp_log_folder_name").toString());
                                output_file_object.put("gcp_task_folder_name", input_file.get("gcp_task_folder_name").toString());

                                output_files_array.put(output_file_object);
                                log.setOutput_files(output_files_array.toString());
                                session1.update(log);


                            }

                            // Update status & description
                            // Update task as completed
                            log.setStatus("completed");
                            //JSONArray log_description1 = new JSONArray(log.getDescription());
                            JSONObject log_add1= new JSONObject();
                            log_add1.put("key","event");
                            log_add1.put("time", Instant.now().toString());
                            log_add1.put("value","Task Completed");

                            log_description.put(log_add1);
                            log.setDescription(log_description.toString());
                            session1.update(log);

                        }
                        tx2.commit();
                        //System.out.println("Done");
                        // Success
                        this.LogMessageInfo("processed : "+ message);
                    } catch (Exception e) {
                        if (tx2 != null) tx2.rollback();
                        e.printStackTrace();
                        // Log client DB issue
                        if(log!=null && log.getId()!=null) {
                            tx3 = session1.beginTransaction();
                            log.setStatus("failed");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", Instant.now().toString());
                            log_add.put("value","Error : "+ e.getMessage());

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                            tx3.commit();
                        }
                        else {
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

                        // Delete downloaded file after successfully processed - recursively
                        FileUtils.deleteQuietly(new File(local_folder));
                        session1.close();
                    }
                }
            }

            catch (Exception e) {
                //if (tx!=null) tx.rollback();
                //e.printStackTrace();
                // log primary DB issue
                this.LogMessageError("Primary DB issue : "+e.getMessage());
            } finally {
                session.close();
            }

        } catch (Exception e) {
            //e.printStackTrace();
            // Store Exception for future ref & update message as failed
            this.LogMessageError("Error in message : "+ message+" :Error: "+ e.getMessage());
        }
    }

    
    // Flipkart Integration, sync SKUs
    @KafkaListener(topics="gcp.store.inventory.flipkart.extract.0", groupId="nano_group_id")
    public void getFlipkartInventoryExtract(String message) throws InterruptedException {
        try {
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
            String local_folder=new GenerateRandomString(10).toString();
            String local_file=new GenerateRandomString(10).toString()+".csv";
            Boolean client_valid=false;
            Log log=null;
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


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
                    Transaction tx1 = null;
                    Transaction tx2 = null;
                    Transaction tx3 = null;
                    BufferedReader br = null;
                    FileWriter outputfile =null;
                    CSVWriter writer =null;
                    try {
                        String line = "";
                        String cvsSplitBy = ",";
                        tx1 = session1.beginTransaction();
                        //Get Log details
                        log = session1.get(Log.class,log_id);
                        if(log!=null) {
                            // Update status & description
                            log.setStatus("started");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                            log_add.put("value","Task Started");

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                            tx1.commit();
                            

                            // check flipkart integration active & token valid?
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
                            Setting default_location = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.default_location_key"));
                            if(default_location==null || default_location.getValue().equals("0")) {
                                throw new Exception("Flipkart default inventory location id not found");
                            }
                            Setting expire_in = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.token_expire_in"));
                            if(expire_in==null || expire_in.getValue().equals("0")) {
                                throw new Exception("Flipkart auth refresh token is not found");
                            }

                            if(this.IsMaxTokenCreateFailWithCount("flipkart", session1, true)){
                                this.StopSyncJob(session1, "FLIPKART_ORDER_SYNC", false, client.getId(), true);
                                this.StopSyncJob(session1, "AMAZON_FLIPKART_INVENTORY_SYNC", false, client_id, true);
                                throw new Exception("Token creation failed max time. Sync Job Stopped");
                            }
                            tx2 = session1.beginTransaction();
                            // calculate token validity
                            Timestamp token_generated= expire_in.getUpdated_at();
                            Timestamp current_timestamp = Timestamp.valueOf(java.time.LocalDateTime.now());
                            long time_differnce_sec = (current_timestamp.getTime() - token_generated.getTime())/1000;
                            Boolean valid_token =false;
                            if((Integer.valueOf(expire_in.getValue()) - 86400) > time_differnce_sec) {
                                valid_token = true;
                            }

                            Setting expired = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.expired"));
                            if(expired==null || expired.getValue().equals("1") || !valid_token) {
                               // generate new token and continue
                                try {
                                    HttpResponse<JsonNode> token_response = Unirest.get(env.getProperty("spring.integration.flipkart.token_url"))
                                            .basicAuth(env.getProperty("spring.integration.flipkart.app_id"), env.getProperty("spring.integration.flipkart.app_secret"))
                                            .header("accept", "application/json")
                                            .queryString("redirect_uri", env.getProperty("spring.integration.flipkart.redirect_url"))
                                            .queryString("grant_type", "refresh_token")
                                            .queryString("state", env.getProperty("spring.integration.flipkart.state"))
                                            .queryString("refresh_token", refresh_token.getValue())
                                            .asJson();
                                    // retrieve the parsed JSONObject from the response
                                    if(!token_response.isSuccess()){
                                        throw new Exception("Issue generating flipkart access token using existing refresh token.May be re auth needed");
                                    }
                                    kong.unirest.json.JSONObject token_response_object = token_response.getBody().getObject();
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
                                    Transaction tx_exception = exception_session.beginTransaction();
                                    expired.setValue("1");
                                    expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    exception_session.update(expired);
                                    this.SetMaxTokenCreateFail("flipkart", exception_session, false);
                                    tx_exception.commit();
                                    throw new Exception("Issue generating flipkart token :" + ex.getMessage());
                                }

                            }

                            Storage storage = StorageOptions.newBuilder()
                                    .setProjectId(env.getProperty("spring.gcp.project-id"))
                                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                    .build()
                                    .getService();

                            // Create Random directory to store the file
                            Files.createDirectories(Paths.get(local_folder));
                            // generate file name
                            String csv_file_report=local_folder+"/"+ local_file;
                            outputfile = new FileWriter(csv_file_report);
                            // create CSVWriter object filewriter object as parameter
                            writer = new CSVWriter(outputfile);
                            // adding header to csv
                            String[] header = { "sku", "flipkart listing id", "inventory","status","message"};
                            writer.writeNext(header);
                            //Integer count=0;
                            Integer pull=10;
                            Query query = session1.createQuery("From Item IT where IT.sku!=:sku and flipkart_listing_id=null and flipkart_product_id=null and flipkart_mrp=null and flipkart_selling_price=null");
                            query.setParameter("sku","undefined");
                            // log local time
                            Timestamp execution_start_timestamp = Timestamp.valueOf(java.time.LocalDateTime.now());
                            // while(true) {
                            //     // Timeout if it passed 10 min of processing
                            //     Timestamp current_start_timestamp = Timestamp.valueOf(java.time.LocalDateTime.now());
                            //     if((current_start_timestamp.getTime()-execution_start_timestamp.getTime())/1000 > Integer.valueOf(env.getProperty("spring.integration.flipkart.force_exit_time"))) {
                            //         throw new Exception("Flipkart sync takes too long time to execute");
                            //     }
                            //     String[] row = null;
                            //     query.setFirstResult(count);
                            //     query.setMaxResults(pull);
                            //     List<Item> item_list = query.list();
                            //     if(item_list.size()==0) {
                            //         break;
                            //     }

                            //     // loop through item list
                            //     String sku_ids = null;
                            //     Integer row_count=0;
                            //     for(Item item: item_list) {
                            //         //row = new String[]{item.getSku(), item.getPrice().toString(), item.getQuantity().toString(), item.getInventory_location()==null? "" : item.getInventory_location(), item.getActive().toString()};
                            //         //writer.writeNext(row);
                            //         if(row_count>0) {
                            //             sku_ids +=","+item.getSku();
                            //         }
                            //         else {
                            //             sku_ids = item.getSku();
                            //         }
                            //         row_count++;
                            //     }
                            //     kong.unirest.json.JSONObject listing_response_object =null;
                            //     for(int i=1; i<=10;i++ ) {
                            //         try {
                            //             // adjust api call 10 in each min (Flipkart call limit 1000/24hrs)
                            //             sleep(1000*Integer.valueOf(env.getProperty("spring.integration.flipkart.general_wait_time")));
                            //             // Call flipkart API to get listing details
                            //             HttpResponse<JsonNode> listing_response = Unirest.get(env.getProperty("spring.integration.flipkart.api_url")+"/listings/v3/"+sku_ids)
                            //                     .header("Authorization", "Bearer "+ token.getValue())
                            //                     .header("accept", "application/json")
                            //                     .asJson();
                            //             // retrieve the parsed JSONObject from the response
                            //             //System.out.println(listing_response.getBody().toString());
                            //             listing_response_object  = listing_response.getBody().getObject();
                            //             if(listing_response.getStatus()==400 || listing_response.getStatus()==500) {
                            //                 throw new Exception("Flipkart API unavailable or api syntax error");
                            //             }
                            //             if(listing_response.getStatus()==401) {
                            //                 Transaction tx_exception = exception_session.beginTransaction();
                            //                 expired.setValue("1");
                            //                 expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            //                 exception_session.update(expired);
                            //                 tx_exception.commit();
                            //                 i=10; // eject from trying new call
                            //                 throw new Exception("Issue using flipkart token");
                            //             }
                            //             break;
                            //         }
                            //         catch (Exception ex) {
                            //             if(i<10) {
                            //                 sleep(1000*i*Integer.valueOf(env.getProperty("spring.integration.flipkart.ex_wait_time")));
                            //                 continue;
                            //             }
                            //             else {
                            //                 throw new Exception("Flipkart API unavailable for long time or "+ ex.getMessage());
                            //             }
                            //         }
                            //     }


                            //     // Loop through item to get
                            //     for(Item item: item_list) {
                            //         kong.unirest.json.JSONObject available_itm=null;
                            //         if(listing_response_object.has("available") && listing_response_object.getJSONObject("available").has(item.getSku())) {
                            //             available_itm= listing_response_object.getJSONObject("available").getJSONObject(item.getSku());
                            //         }
                            //         if(available_itm!=null) {
                            //             // valid SKU & available
                            //             String listing_id =available_itm.getString("listing_id");
                            //             String product_id =available_itm.getString("product_id");
                            //             String listing_status =available_itm.getString("listing_status");
                            //             Double itm_mrp = available_itm.getJSONObject("price").getDouble("mrp");
                            //             Double itm_selling_price = available_itm.getJSONObject("price").getDouble("selling_price");
                            //             String itm_tax_code = available_itm.getJSONObject("tax").getString("tax_code");
                            //             String itm_hsn_code = available_itm.getJSONObject("tax").getString("hsn");
                            //             kong.unirest.json.JSONArray itm_locations= available_itm.getJSONArray("locations");
                            //             Boolean inventory_found=false;
                            //             int inventory=0;
                            //             for(int i = 0; i<itm_locations.length(); i++) {
                            //                 if(default_location.getValue().equals(itm_locations.getJSONObject(i).getString("id"))) {
                            //                     inventory=itm_locations.getJSONObject(i).getInt("inventory");
                            //                     inventory_found=true;
                            //                     break;
                            //                 }
                            //             }
                            //             // Check available value to update listing / sku / item
                            //             if(listing_id==null || product_id==null || listing_status==null || itm_mrp==null || itm_selling_price==null || itm_tax_code==null || itm_hsn_code==null || !inventory_found){
                            //              //
                            //                 row = new String[]{item.getSku(), listing_id==null ? "" : listing_id , inventory_found ? String.valueOf(inventory) : "" , "Failed" , "Required parameter is not available from api"};
                            //             }
                            //             else {
                            //                 //Update Item in DB & flag as sync with flipkart, export success report
                            //                 item.setFlipkart_listing_id(listing_id);
                            //                 item.setFlipkart_product_id(product_id);
                            //                 item.setFlipkart_mrp(itm_mrp);
                            //                 item.setFlipkart_quantity(inventory);
                            //                 item.setFlipkart_selling_price(itm_selling_price);
                            //                 if (listing_status.equals("ACTIVE")) {
                            //                     item.setFlipkart_active_listing(Byte.valueOf("1"));
                            //                 } else {
                            //                     item.setFlipkart_active_listing(Byte.valueOf("0"));
                            //                 }
                            //                 if(item.getProduct().getTax_rate()==null || item.getProduct().getTax_rate()<=0) {
                            //                     // calculate correct tax rate
                            //                     switch(itm_tax_code) {
                            //                         case "GST_APPAREL" :
                            //                             if (itm_selling_price > 1000) {
                            //                                 item.getProduct().setTax_rate(12.0);
                            //                             } else {
                            //                                 item.getProduct().setTax_rate(5.0);
                            //                             }
                            //                             break;
                            //                         case "GST_28" :  item.getProduct().setTax_rate(28.0);break;
                            //                         case "GST_18" :  item.getProduct().setTax_rate(18.0);break;
                            //                         case "GST_12" :  item.getProduct().setTax_rate(12.0);break;
                            //                         case "GST_5" :  item.getProduct().setTax_rate(5.0);break;
                            //                         case "GST_3" :  item.getProduct().setTax_rate(3.0);break;
                            //                         default : item.getProduct().setTax_rate(0.0);break;
                            //                     }
                            //                }


                            //                 if(item.getProduct().getHsn_code()==null || item.getProduct().getHsn_code().equals("")) {
                            //                     item.getProduct().setHsn_code(itm_hsn_code);
                            //                 }

                            //                 item.setFlipkart_last_sync_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            //                 item.setSync_flipcart(Byte.valueOf("1"));

                            //                 // update items along with products
                            //                 session1.update(item);
                            //                 row = new String[]{item.getSku(), listing_id==null ? "" : listing_id , inventory_found ? String.valueOf(inventory) : "" , "Success" , "Item Successfully Synchronized"};
                            //             }

                            //         }
                            //         else {
                            //             List invalid_itm =null;
                            //             if(listing_response_object.has("invalid")) {
                            //                 invalid_itm= listing_response_object.getJSONArray("invalid").toList();
                            //             }
                            //             if(invalid_itm.contains(item.getSku())) {
                            //                 // Mark as invalid
                            //                 row = new String[]{item.getSku(), "", "" , "Failed" , "SKU Invalid"};
                            //             }
                            //             else {
                            //                 List unavailable_itm=null;
                            //                 if(listing_response_object.has("unavailable")) {
                            //                     unavailable_itm= listing_response_object.getJSONArray("unavailable").toList();
                            //                 }
                            //                 if(unavailable_itm.contains(item.getSku())) {
                            //                     // Mark as unavailable
                            //                     row = new String[]{item.getSku(), "", "" , "Failed" , "SKU Unavailable"};
                            //                 }
                            //                 else {
                            //                     // Not found in api response. System issue
                            //                     row = new String[]{item.getSku(), "", "" , "Failed" , "System Error"};
                            //                 }
                            //             }

                            //         }

                            //         writer.writeNext(row);
                            //     }

                            //     count+=row_count;
                            // }
                            List<Item> item_list = query.list();
                            List<Item> item_pool = new ArrayList<Item>();
                            Integer pool_size = 10;
                            int count = 1;
                            for (Item item : item_list) {

                                if(count % pool_size == 0){

                                    List<String> sku_list = item_pool.stream().map((Item map_item) -> map_item.getSku()).collect(Collectors.toList());
                                    // perform operation after collect pool
                                     kong.unirest.json.JSONObject listing_response_object =null;
                                     for(int i=1; i<=10;i++ ) {
                                         try {
                                             // adjust api call 10 in each min (Flipkart call limit 1000/24hrs)
                                             sleep(1000*Integer.valueOf(env.getProperty("spring.integration.flipkart.general_wait_time")));
                                             // Call flipkart API to get listing details
                                             HttpResponse<JsonNode> listing_response = Unirest.get(env.getProperty("spring.integration.flipkart.api_url") + "/listings/v3/" + String.join(",", sku_list))
                                                     .header("Authorization", "Bearer "+ token.getValue())
                                                     .header("accept", "application/json")
                                                     .asJson();
                                             // retrieve the parsed JSONObject from the response
                                             //System.out.println(listing_response.getBody().toString());
                                             listing_response_object  = listing_response.getBody().getObject();
                                             if(listing_response.getStatus()==400 || listing_response.getStatus()==500) {
                                                 throw new Exception("Flipkart API unavailable or api syntax error");
                                             }
                                             if(listing_response.getStatus()==401) {
                                                 Transaction tx_exception = exception_session.beginTransaction();
                                                 expired.setValue("1");
                                                 expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                                 exception_session.update(expired);
                                                 tx_exception.commit();
                                                 i=10; // eject from trying new call
                                                 throw new Exception("Issue using flipkart token");
                                             }
                                             break;
                                         }
                                         catch (Exception ex) {
                                             if(i<10) {
                                                 sleep(1000*i*Integer.valueOf(env.getProperty("spring.integration.flipkart.ex_wait_time")));
                                                 continue;
                                             }
                                             else {
                                                 throw new Exception("Flipkart API unavailable for long time or "+ ex.getMessage());
                                             }
                                         }
                                     }
                                     for(Item single_item: item_pool) {
                                        try {
                                                kong.unirest.json.JSONObject available_itm=null;
                                                if(listing_response_object.has("available") && listing_response_object.getJSONObject("available").has(single_item.getSku())) {
                                                    available_itm= listing_response_object.getJSONObject("available").getJSONObject(single_item.getSku());
                                                }
                                                if(available_itm!=null) {
                                                    // valid SKU & available
                                                    String listing_id =available_itm.getString("listing_id");
                                                    String product_id =available_itm.getString("product_id");
                                                    String listing_status =available_itm.getString("listing_status");
                                                    Double itm_mrp = available_itm.getJSONObject("price").getDouble("mrp");
                                                    Double itm_selling_price = available_itm.getJSONObject("price").getDouble("selling_price");
                                                    String itm_tax_code = available_itm.getJSONObject("tax").getString("tax_code");
                                                    String itm_hsn_code = available_itm.getJSONObject("tax").getString("hsn");
                                                    kong.unirest.json.JSONArray itm_locations= available_itm.getJSONArray("locations");
                                                    Boolean inventory_found=false;
                                                    int inventory=0;
                                                    for(int i = 0; i<itm_locations.length(); i++) {
                                                        if(default_location.getValue().equals(itm_locations.getJSONObject(i).getString("id"))) {
                                                            inventory=itm_locations.getJSONObject(i).getInt("inventory");
                                                            inventory_found=true;
                                                            break;
                                                        }
                                                    }
                                                    // Check available value to update listing / sku / item
                                                    if(listing_id==null || product_id==null || listing_status==null || itm_mrp==null || itm_selling_price==null || itm_tax_code==null || itm_hsn_code==null || !inventory_found){
                                                     //
                                                        writer.writeNext( new String[]{item.getSku(), listing_id==null ? "" : listing_id , inventory_found ? String.valueOf(inventory) : "" , "Failed" , "Required parameter is not available from api"} );
                                                    }
                                                    else {
                                                        //Update Item in DB & flag as sync with flipkart, export success report
                                                        single_item.setFlipkart_listing_id(listing_id);
                                                        single_item.setFlipkart_product_id(product_id);
                                                        single_item.setFlipkart_mrp(itm_mrp);
                                                        single_item.setFlipkart_quantity(inventory);
                                                        single_item.setFlipkart_selling_price(itm_selling_price);
                                                        if (listing_status.equals("ACTIVE")) {
                                                            single_item.setFlipkart_active_listing(Byte.valueOf("1"));
                                                        } else {
                                                            single_item.setFlipkart_active_listing(Byte.valueOf("0"));
                                                        }
                                                        if(single_item.getProduct().getTax_rate()==null || single_item.getProduct().getTax_rate()<=0) {
                                                            // calculate correct tax rate
                                                            switch(itm_tax_code) {
                                                                case "GST_APPAREL" :
                                                                    if (itm_selling_price > 1000) {
                                                                        single_item.getProduct().setTax_rate(12.0);
                                                                    } else {
                                                                        single_item.getProduct().setTax_rate(5.0);
                                                                    }
                                                                    break;
                                                                case "GST_28" :  single_item.getProduct().setTax_rate(28.0);break;
                                                                case "GST_18" :  single_item.getProduct().setTax_rate(18.0);break;
                                                                case "GST_12" :  single_item.getProduct().setTax_rate(12.0);break;
                                                                case "GST_5"  :  single_item.getProduct().setTax_rate(5.0);break;
                                                                case "GST_3"  :  single_item.getProduct().setTax_rate(3.0);break;
                                                                default : single_item.getProduct().setTax_rate(0.0);break;
                                                            }
                                                       }
            
            
                                                        if(single_item.getProduct().getHsn_code()==null || single_item.getProduct().getHsn_code().equals("")) {
                                                            single_item.getProduct().setHsn_code(itm_hsn_code);
                                                        }
            
                                                        single_item.setFlipkart_last_sync_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                                        single_item.setSync_flipcart(Byte.valueOf("1"));
            
                                                        // update items along with products
                                                        session1.update(single_item);
                                                        writer.writeNext( new String[]{single_item.getSku(), listing_id==null ? "" : listing_id , inventory_found ? String.valueOf(inventory) : "" , "Success" , "Item Successfully Synchronized"});
                                                    }
            
                                                }
                                                else {
                                                    List invalid_itm =null;
                                                    if(listing_response_object.has("invalid")) {
                                                        invalid_itm= listing_response_object.getJSONArray("invalid").toList();
                                                    }
                                                    if(invalid_itm.contains(single_item.getSku())) {
                                                        // Mark as invalid
                                                        writer.writeNext(new String[]{single_item.getSku(), "", "" , "Failed" , "SKU Invalid"});
                                                    }
                                                    else {
                                                        List unavailable_itm=null;
                                                        if(listing_response_object.has("unavailable")) {
                                                            unavailable_itm= listing_response_object.getJSONArray("unavailable").toList();
                                                        }
                                                        if(unavailable_itm.contains(single_item.getSku())) {
                                                            // Mark as unavailable
                                                            writer.writeNext( new String[]{single_item.getSku(), "", "" , "Failed" , "SKU Unavailable"});
                                                        }
                                                        else {
                                                            // Not found in api response. System issue
                                                            writer.writeNext( new String[]{single_item.getSku(), "", "" , "Failed" , "System Error"} );
                                                        }
                                                    }
            
                                                }

                                            
                                        } catch (Exception e) {
                                            writer.writeNext( new String[]{single_item.getSku(), "", "" , "Failed" , "System Error"});
                                        }
                                    }
                                }
                                // initiate new pool
                                if(count % pool_size == 0){
                                   item_pool = new ArrayList<Item>() ;
                                }
                                item_pool.add(item) ;
                                count++ ;
                            }

                            // close report file
                            if(writer!=null) {
                                //closing writer connection
                                writer.close();
                                outputfile.close();
                                writer=null;
                            }

                            // Generate log & upload to GCS

                            // upload report to gcs
                            BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+env.getProperty("spring.gcp.log-folder-name")+"/"+local_folder+"/"+local_file);
                            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                            storage.create(blobInfo, Files.readAllBytes(Paths.get(csv_file_report)));

                            // Update output file in db
                            JSONArray output_files_array = new JSONArray();
                            JSONObject output_file_object= new JSONObject();
                            output_file_object.put("gcp_file_name",local_file);
                            output_file_object.put("gcp_log_folder_name", "logs");
                            output_file_object.put("gcp_task_folder_name",local_folder);

                            output_files_array.put(output_file_object);
                            log.setOutput_files(output_files_array.toString());
                            session1.update(log);


                            // Update status & description
                            // Update task as completed
                            log.setStatus("completed");
                            //JSONArray log_description1 = new JSONArray(log.getDescription());
                            JSONObject log_add1= new JSONObject();
                            log_add1.put("key","event");
                            log_add1.put("time", LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                            log_add1.put("value","Task Completed");

                            log_description.put(log_add1);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                        }
                        tx2.commit();
                        //System.out.println("Done");
                        // success
                        this.LogMessageInfo("processed : "+ message);
                    } catch (Exception e) {
                        if (tx2 != null) tx2.rollback();
                        e.printStackTrace();
                        // Log client DB issue
                        if(log!=null && log.getId()!=null) {
                            tx3 = session1.beginTransaction();
                            log.setStatus("failed");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                            log_add.put("value","Error : "+ e.getMessage());

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            session1.update(log);
                            tx3.commit();
                        }
                        else {
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

                        // Delete downloaded file after successfully processed - recursively
                        FileUtils.deleteQuietly(new File(local_folder));
                        session1.close();
                    }
                }
            }

            catch (Exception e) {
                //if (tx!=null) tx.rollback();
                //e.printStackTrace();
                // log primary DB issue
                this.LogMessageError("Primary DB issue : "+e.getMessage());

            } finally {
                session.close();
            }


        } catch (Exception e) {
            // e.printStackTrace();
            // Store Exception for future ref & update message as failed
            this.LogMessageError("Error in message : "+ message+" :Error: "+ e.getMessage());
        }
    }

    /*
    // for amazon order pull
    @KafkaListener(topics="gcp.store.order.amazon.sync.0", groupId="nano_group_id")
    public void getAmazonOrderSync(String message) throws InterruptedException {
        try {
            String current_topic="gcp.store.order.amazon.sync.0";
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
            String local_folder=new GenerateRandomString(10).toString();
            String local_file=new GenerateRandomString(10).toString()+".csv";
            Boolean client_valid=false;
            Log log=null;
            BigInteger order_import_count = new BigInteger("0");

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
                    FileWriter outputfile =null;
                    CSVWriter writer =null;
                    try {
                        String line = "";
                        String cvsSplitBy = ",";
                        tx1 = session1.beginTransaction();
                        //Get Log details
                        log = session1.get(Log.class,log_id);
                        if(log!=null) {
                            // Update status & description
                            log.setStatus("started");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log_add.put("value","Task Started");

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session1.update(log);
                            tx1.commit();
                            

                            Storage storage = StorageOptions.newBuilder()
                                    .setProjectId(env.getProperty("spring.gcp.project-id"))
                                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                    .build()
                                    .getService();

                            // Create Random directory to store the file
                            Files.createDirectories(Paths.get(local_folder));
                            // generate file name
                            String csv_file_report=local_folder+"/"+ local_file;
                            outputfile = new FileWriter(csv_file_report);
                            // create CSVWriter object filewriter object as parameter
                            writer = new CSVWriter(outputfile);
                            // adding header to csv
                            String[] header = { "System Order Ref", "Marketplace Identifier", "Order Date","Marketplace Status","Status"};
                            writer.writeNext(header);
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
                                throw new Exception("Amazon auth token is not found");
                            }
                            Setting refresh_token = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.refresh_token_key"));
                            if(refresh_token==null || refresh_token.getValue().equals("0")) {
                                throw new Exception("Amazon auth refresh token is not found");
                            }
                            
                            if(this.IsMaxTokenCreateFailWithDate("amazon", session1, true)){
                                this.StopSyncJob(session1, "AMAZON_ORDER_SYNC", false, client.getId(), true);
                                this.StopSyncJob(session1, "AMAZON_FLIPKART_INVENTORY_SYNC", false, client.getId(), true);
                                throw new Exception("Token creation failed max time. Sync Job Stopped");
                            }
                            tx2 = session1.beginTransaction();

                            Setting expire_in = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.token_expire_in"));
                            //System.out.println(expire_in);//rm
                            //System.out.println(expire_in.getValue());//rm
                            if(expire_in==null || expire_in.getValue().equals("0")) {
                                
                                throw new Exception("Amazon expire in not found");
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

                                }
                                catch (Exception ex) {
                                    Transaction tx_exception = exception_session.beginTransaction();
                                    expired.setValue("1");
                                    expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    exception_session.update(expired);
                                    tx_exception.commit();
                                    this.SetMaxTokenCreateFail("amazon", exception_session, true);
                                    // Will put timer to set integration disabled after 2-3 attempt
                                    throw new Exception("Issue generating amazon token :" + ex.getMessage());
                                }
                            }

                            // Get last order sync date / time in UTC
                            List<String> list_order_ref = new ArrayList<String>();
                            Setting order_last_sync = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.order_last_sync_time"));
                            LocalDateTime inventory_update_datetime = LocalDateTime.now();
                            String latest_order_update_datetime = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString()+"Z";
                            String last_order_sync_datetime_str = order_last_sync.getValue();
                            LocalDateTime last_order_sync_datetime = LocalDateTime.parse(last_order_sync_datetime_str.split("T")[0]+"T"+last_order_sync_datetime_str.split("T")[1].split("\\.")[0]);
                            last_order_sync_datetime = last_order_sync_datetime.minus(5,ChronoUnit.HOURS).minus(30,ChronoUnit.MINUTES);// time convert Asia/Kolkata

                            if(order_last_sync==null) {
                                // set for development or it will take time when sync operation updated
                            }
                            String next_token="";
                            while(true) { // loop for multiple order page

                                for (int i = 1; i <= 10; i++) {
                                    try {
                                        sleep(1000*Integer.valueOf(env.getProperty("spring.integration.amazon.general_wait_time"))*i);
                                        //System.out.println("In the room");

                                        // call orders api to get orders LastUpdatedAfter > order_last_sync
                                        OkHttpClient amazon_client = new OkHttpClient();
                                        com.squareup.okhttp.Request request_orders = new Request.Builder()
                                                .url(env.getProperty("spring.integration.amazon.orders_url")+"?MarketplaceIds="+env.getProperty("spring.integration.amazon.marketplace_id")+"&LastUpdatedAfter="+last_order_sync_datetime.toString()+"Z" + (!next_token.isEmpty() ? "&NextToken="+ URLEncoder.encode(next_token, StandardCharsets.UTF_8) : "")) // Date time in UTC
                                                .get()
                                                .addHeader("x-amz-access-token", token.getValue())
                                                .build();
                                        //System.out.println("In the room 1 ");
                                        AWSAuthenticationCredentials awsAuthenticationCredentials = AWSAuthenticationCredentials.builder()
                                                .accessKeyId(env.getProperty("spring.integration.amazon.iam_access_keyID"))
                                                .secretKey(env.getProperty("spring.integration.amazon.iam_access_secretKey"))
                                                .region(env.getProperty("spring.integration.amazon.iam_region"))
                                                .build();
                                        //System.out.println("In the room 2");
                                        com.squareup.okhttp.Request request_orders_signed = new AWSSigV4Signer(awsAuthenticationCredentials)
                                                .sign(request_orders);
                                        Response orders_response = amazon_client.newCall(request_orders_signed).execute();
                                        //System.out.println("Order Reponse: "+ orders_response.code());
                                        if(orders_response.code()==403) { // Token expired
                                            // update settings to set expired true
                                            tx3 = exception_session_token.beginTransaction();
                                            expired.setValue("1");
                                            expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                            exception_session_token.update(expired);
                                            tx3.commit();
                                            throw new Exception("403");
                                        }
                                        if(orders_response.code()==400) { // bad request
                                            System.out.println(orders_response.body().string());
                                            throw new Exception("400");
                                        }
                                        if(orders_response.code()!=200) { // wait to call again, service unavailable sleep
                                            throw new Exception("404"); // wait to call again
                                        }
                                        else {
                                            JSONObject orders_json = new JSONObject(orders_response.body().string());
                                            // Process orders that received
                                            //latest_order_update_datetime= orders_json.getJSONObject("payload").getString("LastUpdatedBefore");

                                            JSONArray orders_json_array= orders_json.getJSONObject("payload").getJSONArray("Orders");
                                            // parse orders & update in db
                                            for(int cnt = 0; cnt<orders_json_array.length(); cnt++) {
                                                System.out.println(orders_json_array.getJSONObject(cnt).toString());
                                                String amazon_order_id =orders_json_array.getJSONObject(cnt).getString("AmazonOrderId");
                                                String order_status =orders_json_array.getJSONObject(cnt).getString("OrderStatus").toLowerCase();
                                                Integer order_system_id=0;
                                                Integer order_system_amazon_id=0;
                                                Boolean inventory_sub=false;
                                                Boolean inventory_add=false;
                                                Boolean cancel_order=false;
                                                Order   order=null;
                                                String  orderSystemStatusName=null;
                                                String  ship_date = orders_json_array.getJSONObject(cnt).has("LatestShipDate")?
                                                                   orders_json_array.getJSONObject(cnt).getString("LatestShipDate"): 
                                                                   null;
                                                if(!ship_date.equals(null)){
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
                                                Query query = session1.createQuery("From Order O WHERE O.marketplace_id = :marketplace_id AND O.marketplace_identifier = :marketplace_identifier");
                                                query.setParameter("marketplace_id", Integer.valueOf(env.getProperty("spring.integration.amazon.system_id")));
                                                query.setParameter("marketplace_identifier", amazon_order_id);
                                                List<Order> order_list = query.list();
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
                                                       }
                                                       else {
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
                                                else { 
                                                    // order need to newly created in db
                                                    // set wait time for other call
                                                    sleep(1000*Integer.valueOf(env.getProperty("spring.integration.amazon.ex_wait_time"))*i);
                                                    String purchase_date =orders_json_array.getJSONObject(cnt).getString("PurchaseDate");
                                                    LocalDateTime purchase_localdatetime = LocalDateTime.parse(purchase_date.split("T")[0]+"T"+purchase_date.split("T")[1].split("Z")[0]);
                                                    if(purchase_localdatetime.isBefore(last_order_sync_datetime)){
                                                        continue;
                                                    }
                                                    String last_order_update_date =orders_json_array.getJSONObject(cnt).getString("LastUpdateDate");
                                                    System.out.println(orders_json_array.getJSONObject(cnt).toString());
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
                                                    String order_type =orders_json_array.getJSONObject(cnt).getString("OrderType");
                                                    Boolean is_business_order = orders_json_array.getJSONObject(cnt).getBoolean("IsBusinessOrder");
                                                    Boolean is_prime_customer = orders_json_array.getJSONObject(cnt).getBoolean("IsPrime");
                                                    Boolean is_premium_order = orders_json_array.getJSONObject(cnt).getBoolean("IsPremiumOrder");


                                                    // call getorderaddress to get specific customer shipping address
                                                    com.squareup.okhttp.Request request_order_address = new Request.Builder()
                                                            .url(env.getProperty("spring.integration.amazon.orders_url")+"/"+ amazon_order_id+"/address") // getorderaddress
                                                            .get()
                                                            .addHeader("x-amz-access-token", token.getValue())
                                                            .build();

                                                    com.squareup.okhttp.Request request_order_address_signed = new AWSSigV4Signer(awsAuthenticationCredentials)
                                                            .sign(request_order_address);
                                                    Response order_address_response = amazon_client.newCall(request_order_address_signed).execute();
                                                    if(order_address_response.code()==403) { // Token expired
                                                        // update settings to set expired true
                                                        tx3 = exception_session_token.beginTransaction();
                                                        expired.setValue("1");
                                                        expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                                        exception_session_token.update(expired);
                                                        tx3.commit();
                                                        throw new Exception("403");
                                                    }
                                                    if(order_address_response.code()==400) { // bad request
                                                        throw new Exception("400");
                                                    }
                                                    if(order_address_response.code()!=200) { // wait to call again, service unavailable sleep
                                                        throw new Exception("404"); // wait to call again
                                                    }
                                                    else {
                                                        // extract order address
                                                        JSONObject order_address_json = new JSONObject(order_address_response.body().string()).getJSONObject("payload");
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



                                                        // call getOrderItems to get order items
                                                        com.squareup.okhttp.Request request_order_items = new Request.Builder()
                                                                .url(env.getProperty("spring.integration.amazon.orders_url")+"/"+ amazon_order_id+"/orderItems") // getOrderItems
                                                                .get()
                                                                .addHeader("x-amz-access-token", token.getValue())
                                                                .build();

                                                        com.squareup.okhttp.Request request_order_items_signed = new AWSSigV4Signer(awsAuthenticationCredentials)
                                                                .sign(request_order_items);
                                                        Response order_items_response = amazon_client.newCall(request_order_items_signed).execute();

                                                        if(order_items_response.code()==403) { // Token expired
                                                            // update settings to set expired true
                                                            tx3 = exception_session_token.beginTransaction();
                                                            expired.setValue("1");
                                                            expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                                            exception_session_token.update(expired);
                                                            tx3.commit();
                                                            throw new Exception("403");
                                                        }
                                                        if(order_items_response.code()==400) { // bad request
                                                            throw new Exception("400");
                                                        }
                                                        if(order_items_response.code()!=200) { // wait to call again, service unavailable sleep
                                                            throw new Exception("404"); // wait to call again
                                                        }
                                                        else {
                                                            //Create order with available info
                                                            //create customer for each order
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
                                                            if(last_order.size()==0){
                                                                order_ref_num=GenerateOrderRef.getInitialRef("ORD");
                                                            }else{
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
                                                            JSONArray order_items_json = new JSONObject(order_items_response.body().string()).getJSONObject("payload").getJSONArray("OrderItems");
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
                                                                }
                                                                else {
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

                                                        }

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
                                                              itm.setInventory_updated_at(Timestamp.valueOf(inventory_update_datetime));
                                                              session1.update(itm);
                                                            }
                                                        }
                                                }


                                                // call

                                                // write to csv what has been updated
                                                String[] row = new String[]{order.getOrder_ref(), order.getMarketplace_identifier(),order.getOrder_date().toString(), order_status,order.getIntegration_status()};
                                                writer.writeNext(row);
                                                order_import_count = order_import_count.add(BigInteger.ONE);
                                            }
                                            // loop if nextToken available with some sleep

                                            if(orders_json.getJSONObject("payload").has("NextToken"))
                                            {
                                                next_token= orders_json.getJSONObject("payload").getString("NextToken");
                                            }
                                            else {
                                                next_token="";
                                            }
                                            break; // continue while loop

                                        }

                                    } catch (Exception ex) {
                                        ex.printStackTrace();
                                        //System.out.println("new roor :" + ex.getMessage());
                                        if(ex.getMessage().equals("403")) {
                                            throw new Exception("101"); // Token expired eject with requeue message
                                        }
                                        else if(ex.getMessage().equals("400")) { // Eject as bad api call request
                                            throw new Exception("102");
                                        }
                                        else if (ex.getMessage().equals("404")) {
                                            if(i<10) {
                                                sleep(1000*i*Integer.valueOf(env.getProperty("spring.integration.amazon.max_wait_time")));
                                                continue;
                                            }
                                            else {
                                                throw new Exception("105"); // API unavailable for long time
                                            }
                                        }
                                        else {
                                            throw new Exception(ex.getMessage()); // unhandled error
                                        }

                                    }
                                }
                                if(next_token.isEmpty()) {  // order next page is not available
                                    break;
                                }

                            }

                            // Amazon Sync logic end

                            // Update last success sync timestamp
                            if(latest_order_update_datetime!=null && !latest_order_update_datetime.isEmpty()) {
                                order_last_sync.setValue(latest_order_update_datetime);
                                session1.update(order_last_sync);
                            }

                            // close report file
                            if(writer!=null) {
                                //closing writer connection
                                writer.close();
                                outputfile.close();
                                writer=null;
                            }

                            // Generate log & upload to GCS

                            // upload report to gcs
                            BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+env.getProperty("spring.gcp.log-folder-name")+"/"+local_folder+"/"+local_file);
                            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                            storage.create(blobInfo, Files.readAllBytes(Paths.get(csv_file_report)));

                            // Update output file in db
                            JSONArray output_files_array = new JSONArray();
                            JSONObject output_file_object= new JSONObject();
                            output_file_object.put("gcp_file_name",local_file);
                            output_file_object.put("gcp_log_folder_name", "logs");
                            output_file_object.put("gcp_task_folder_name",local_folder);

                            output_files_array.put(output_file_object);
                            log.setOutput_files(output_files_array.toString());
                            session1.update(log);


                            // Update status & description
                            // Update task as completed
                            log.setStatus("completed");
                            //JSONArray log_description1 = new JSONArray(log.getDescription());
                            JSONObject log_add1= new JSONObject();
                            log_add1.put("key","event");
                            log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log_add1.put("value","Task Completed");

                            log_description.put(log_add1);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session1.update(log);
                        }
                        tx2.commit();
                        this.OrderImportCount(client_id, "amazon", order_import_count);
                        //System.out.println("Done");
                        // success
                        this.LogMessageInfo("processed : "+ message);
                    } catch (Exception e) {
                        if (tx2 != null) tx2.rollback();
                        e.printStackTrace();
                        // Log client DB issue
                        if(log!=null && log.getId()!=null) {
                            tx3 = session1.beginTransaction();
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            System.out.println("Error :" + e.getMessage());
                            if(e.getMessage().equals("101")) {
                                log.setStatus("re-queue");
                                log_add.put("value","Info : Re-queue job due to amazon token expired");
                            }
                            else if(e.getMessage().equals("102")) {
                                log.setStatus("failed");
                                log_add.put("value","Error : Amazon bad API request");
                            }
                            else if(e.getMessage().equals("105")) {
                                log.setStatus("failed");
                                log_add.put("value","Error : Amazon API unavailable for long time");
                            }
                            else {
                                log.setStatus("failed");
                                log_add.put("value","Error : "+ e.getMessage());
                            }

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session1.update(log);
                            tx3.commit();
                            // if requeue required
                            if(e.getMessage().equals("101")) { // token expired or requeue instruction
                                // send same message to kafka current topic with the same message to redo
                                kafkaproducer.sendMessage(message,current_topic);
                                System.out.println("Sent new message to kafka "+ message);
                            }
                        }
                        else {
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

                        // Delete downloaded file after successfully processed - recursively
                        FileUtils.deleteQuietly(new File(local_folder));
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
            // Store Exception for future ref & update message as failed
            this.LogMessageError("Error in message : "+ message+" :Error: "+ e.getMessage());
        }
    }
    */
    
    /*
    // for flipkart order pull
    @KafkaListener(topics="gcp.store.order.flipkart.sync.0", groupId="nano_group_id")
    public void getFlipkartOrderSync(String message) throws InterruptedException {
        try {
            String current_topic="gcp.store.order.flipkart.sync.0";
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
            String local_folder=new GenerateRandomString(10).toString();
            String local_file=new GenerateRandomString(10).toString()+".csv";
            Boolean client_valid=false;
            Log log=null;
            BigInteger order_import_count = new BigInteger("0");
            
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
                    FileWriter outputfile =null;
                    CSVWriter writer =null;
                    try {
                        String line = "";
                        String cvsSplitBy = ",";
                        tx1 = session1.beginTransaction();
                        //Get Log details
                        log = session1.get(Log.class,log_id);
                        if(log!=null) {
                            // Update status & description
                            log.setStatus("started");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log_add.put("value","Task Started");

                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session1.update(log);
                            tx1.commit();

                            Storage storage = StorageOptions.newBuilder()
                                    .setProjectId(env.getProperty("spring.gcp.project-id"))
                                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                    .build()
                                    .getService();

                            // Create Random directory to store the file
                            Files.createDirectories(Paths.get(local_folder));
                            // generate file name
                            String csv_file_report=local_folder+"/"+ local_file;
                            outputfile = new FileWriter(csv_file_report);
                            // create CSVWriter object filewriter object as parameter
                            writer = new CSVWriter(outputfile);
                            // adding header to csv
                            String[] header = { "System Order Ref", "Marketplace Identifier", "Order Date","Marketplace Status","Status"};
                            writer.writeNext(header);
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
                                    HttpResponse<JsonNode> token_response = Unirest.get(env.getProperty("spring.integration.flipkart.token_url"))
                                            .basicAuth(env.getProperty("spring.integration.flipkart.app_id"), env.getProperty("spring.integration.flipkart.app_secret"))
                                            .header("accept", "application/json")
                                            .queryString("redirect_uri", env.getProperty("spring.integration.flipkart.redirect_url"))
                                            .queryString("grant_type", "refresh_token")
                                            .queryString("state", env.getProperty("spring.integration.flipkart.state"))
                                            .queryString("refresh_token", refresh_token.getValue())
                                            .asJson();
                                    // retrieve the parsed JSONObject from the response
                                    if(!token_response.isSuccess()){
                                        throw new Exception("Issue generating flipkart access token using existing refresh token.May be re auth needed");
                                    }
                                    System.out.println("token refreshing");
                                    kong.unirest.json.JSONObject token_response_object = token_response.getBody().getObject();
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
                                    Transaction tx_exception = exception_session.beginTransaction();
                                    expired.setValue("1");
                                    expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    exception_session.update(expired);
                                    tx_exception.commit();
                                    SetMaxTokenCreateFail("flipkart", exception_session, true);
                                    throw new Exception("Issue generating flipkart token :" + ex.getMessage());
                                }
                            }

                            // Get last order sync date / time in UTC
                            Setting order_last_sync = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.order_last_sync_time"));
                            LocalDateTime inventory_update_datetime = LocalDateTime.now();
                            String latest_order_update_datetime = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString()+"Z";
                            String latest_order_update_datetime_utc = LocalDateTime.now().toString()+"Z";
                            LocalDateTime datetime_before_order_last_sync = LocalDateTime.parse(order_last_sync.getValue().split("T")[0]+"T"+order_last_sync.getValue().split("T")[1].split("\\.")[0])
                                                                            .minus(5,ChronoUnit.HOURS).minus((18+30),ChronoUnit.MINUTES);
                            List<String> list_order_ref = new ArrayList<String>();
                            //String inventory_updated_at=latest_order_update_datetime;
                            if(order_last_sync==null) {
                                // set for development or it will take time when sync operation updated
                            }

                            // loop multiple status
                            JSONArray flipkart_status_to_check= new JSONArray(env.getProperty("spring.integration.flipkart.status"));

                            for(Integer process_count=0;process_count<flipkart_status_to_check.length();process_count++) {
                                String flipkart_type= flipkart_status_to_check.getJSONObject(process_count).getString("type");
                                JSONArray flipkart_typed_status= flipkart_status_to_check.getJSONObject(process_count).getJSONArray("status");
                                String status_array="";
                                for(Integer status_count=0;status_count<flipkart_typed_status.length();status_count++) {
                                    if(status_count>0) {
                                        status_array+=","+ flipkart_typed_status.get(status_count).toString();
                                    }
                                    else {
                                        status_array+=flipkart_typed_status.get(status_count).toString();
                                    }
                                }
                                String next_token="";
                                while(true) { // loop for multiple order page
                                    
                                    for (Integer i = 1; i <= 10; i++) {
                                        try {
                                            sleep(1000*Integer.valueOf(env.getProperty("spring.integration.flipkart.general_wait_time"))*i);
                                            //System.out.println("In the room");

                                            // call orders api to get orders LastUpdatedAfter > order_last_sync
                                            JsonNode request_body = null;
                                            if(!flipkart_type.equals("cancelled")){
                                                  request_body = new JsonNode("{\n" +
                                                          "  \"filter\": {\n" +
                                                          "    \"states\": [\n" + status_array +
                                                          "    ],\n" +
                                                          "    \"type\": \""+ flipkart_type + "\",\n" +
                                                          "    \"modifiedDate\": {\n" +
                                                          "      \"to\": \"" +  latest_order_update_datetime_utc+ "\",\n" +
                                                          "      \"from\": \""+ datetime_before_order_last_sync.toString() + "\"\n" +
                                                          "    }\n" +
                                                          "  }\n" +
                                                          "}");
                                            }else{
                                                  request_body = new JsonNode("{\n" +
                                                  "  \"filter\": {\n" +
                                                  "    \"states\": [\n" + status_array +
                                                  "    ],\n" +
                                                  "    \"type\": \""+ flipkart_type + "\",\n" +
                                                  "    \"cancellationDate\": {\n" +
                                                  "      \"to\": \"" +  latest_order_update_datetime_utc+ "\",\n" +
                                                  "      \"from\": \""+ datetime_before_order_last_sync.toString() + "\"\n" +
                                                  "    }\n" +
                                                  "  }\n" +
                                                  "}");
                                            }
                                            // System.out.println(next_token);
                                            System.out.println("flipkart order get request");
                                            System.out.println(request_body);
                                            String flipkart_shipments_url=env.getProperty("spring.integration.flipkart.api_url")+ (next_token.isEmpty() ? "/v3/shipments/filter" : next_token);
                                            HttpResponse<JsonNode> order_http_response;
                                            if(next_token.isEmpty()){
                                                        order_http_response = Unirest.post(flipkart_shipments_url)
                                                       .header("Content-Type", "application/json")
                                                       .header("Authorization", "Bearer "+ token.getValue())
                                                       .header("accept", "application/json")
                                                       .body(request_body.toString())
                                                       .asJson();
                                            }else{
                                                 order_http_response = Unirest.get(flipkart_shipments_url)
                                                .header("Content-Type", "application/json")
                                                .header("Authorization", "Bearer "+ token.getValue())
                                                .header("accept", "application/json")
                                                .asJson();

                                            }
                                            // retrieve the parsed JSONObject from the response
                                            System.out.println("response flipkart order sync");
                                            System.out.println(order_http_response.getBody());
                                            System.out.println("status: "+order_http_response.getStatus());
                                            //kong.unirest.json.JSONObject orders_response  = order_http_response.getBody().getObject();
                                            if(order_http_response.getStatus()==401) { // Token expired
                                                // update settings to set expired true
                                                tx3 = exception_session_token.beginTransaction();
                                                expired.setValue("1");
                                                expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                                exception_session_token.update(expired);
                                                tx3.commit();
                                                throw new Exception("403");
                                            }
                                            if(order_http_response.getStatus()==400 || order_http_response.getStatus()==500) { // bad request
                                                throw new Exception("400");
                                            }
                                            if(order_http_response.getStatus()!=200) { // wait to call again, service unavailable sleep
                                                throw new Exception("404"); // wait to call again
                                            }
                                            else {
                                                kong.unirest.json.JSONObject orders_response  = order_http_response.getBody().getObject();
                                                JSONObject orders_json = new JSONObject(orders_response.toString());
                                                // Process orders that received
                                                //latest_order_update_datetime= orders_json.getJSONObject("payload").getString("LastUpdatedBefore");
                                                
                                                JSONArray orders_json_array= orders_json.getJSONArray("shipments");
                                                // associate orders with line items segment line item for orders
                                                List<Order> orders= new ArrayList<>();
                                                for(int cnt = 0; cnt<orders_json_array.length(); cnt++) {
                                                    // loop through order items
                                                    LocalDateTime dispatchByDate=null;
                                                    JSONArray order_items_json_array = orders_json_array.getJSONObject(cnt).getJSONArray("orderItems");
                                                    for (int cnt_lt = 0; cnt_lt < order_items_json_array.length(); cnt_lt++) {
                                                        Order order = null;
                                                        String flipkart_order_id=order_items_json_array.getJSONObject(cnt_lt).getString("orderId");

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
                                                            order.setMarketplace_identifier(flipkart_order_id);
                                                            order.setMarketplace_id(Integer.valueOf(env.getProperty("spring.integration.flipkart.system_id")));
                                                            String order_date[]=order_items_json_array.getJSONObject(cnt_lt).getString("orderDate").split("T");
                                                            order.setOrder_date(Timestamp.valueOf(order_date[0]+" "+ order_date[1].split("\\+")[0]));
                                                            order.setStatus("ordered");
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
                                                }
                                               //test
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
                                                        if(purchase_localdatetime.isBefore(datetime_before_order_last_sync)){
                                                            continue;
                                                        }
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
                                                        //order.setStatus("ordered");
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
                                                            itm.setInventory_updated_at(Timestamp.valueOf(inventory_update_datetime));

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

                                                    // call

                                                    // write to csv what has been updated
                                                    String[] row = new String[]{order.getOrder_ref(), order.getMarketplace_identifier(),order.getOrder_date().toString(), order_status,order.getIntegration_status()};
                                                    writer.writeNext(row);
                                                    order_import_count = order_import_count.add(BigInteger.ONE);
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

                                        } catch (Exception ex) {
                                            ex.printStackTrace();
                                            //System.out.println("new roor :" + ex.getMessage());
                                            if(ex.getMessage().equals("403")) {
                                                throw new Exception("101"); // Token expired eject with requeue message
                                            }
                                            else if(ex.getMessage().equals("400")) { // Eject as bad api call request
                                                throw new Exception("102");
                                            }
                                            else if (ex.getMessage().equals("404")) {
                                                if(i<10) {
                                                    sleep(1000*i*Integer.valueOf(env.getProperty("spring.integration.flipkart.max_wait_time")));
                                                    continue;
                                                }
                                                else {
                                                    throw new Exception("105"); // API unavailable for long time
                                                }
                                            }
                                            else {
                                                throw new Exception(ex.getMessage()); // unhandled error
                                            }

                                        }
                                    }
                                    if(next_token.isEmpty()) {  // order next page is not available
                                        break;
                                    }

                                }

                                // Amazon Sync logic end

                            }



                            // Update last success sync timestamp
                            if(latest_order_update_datetime!=null && !latest_order_update_datetime.isEmpty()) {
                                order_last_sync.setValue(latest_order_update_datetime);
                                session1.update(order_last_sync);
                            }

                            // close report file
                            if(writer!=null) {
                                //closing writer connection
                                writer.close();
                                outputfile.close();
                                writer=null;
                            }

                            // Generate log & upload to GCS

                            // upload report to gcs
                            BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+env.getProperty("spring.gcp.log-folder-name")+"/"+local_folder+"/"+local_file);
                            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                            storage.create(blobInfo, Files.readAllBytes(Paths.get(csv_file_report)));

                            // Update output file in db
                            JSONArray output_files_array = new JSONArray();
                            JSONObject output_file_object= new JSONObject();
                            output_file_object.put("gcp_file_name",local_file);
                            output_file_object.put("gcp_log_folder_name", "logs");
                            output_file_object.put("gcp_task_folder_name",local_folder);

                            output_files_array.put(output_file_object);
                            log.setOutput_files(output_files_array.toString());
                            session1.update(log);


                            // Update status & description
                            // Update task as completed
                            log.setStatus("completed");
                            //JSONArray log_description1 = new JSONArray(log.getDescription());
                            JSONObject log_add1= new JSONObject();
                            log_add1.put("key","event");
                            log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log_add1.put("value","Task Completed");

                            log_description.put(log_add1);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session1.update(log);
                        }
                        tx2.commit();
                        this.OrderImportCount(client_id, "flipkart", order_import_count);
                        System.out.println("Done");
                        // success
                        this.LogMessageInfo("processed : "+ message);
                    } catch (Exception e) {
                        if (tx2 != null) tx2.rollback();
                        e.printStackTrace();
                        // Log client DB issue
                        if(log!=null && log.getId()!=null) {
                            tx3 = session1.beginTransaction();
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            System.out.println("Error :" + e.getMessage());
                            // System.out.println(e);
                            // System.out.println(e.getStackTrace()[0]);
                            if(e.getMessage().equals("101")) {
                                log.setStatus("re-queue");
                                log_add.put("value","Info : Re-queue job due to Flipkart token expired");
                            }
                            else if(e.getMessage().equals("102")) {
                                log.setStatus("failed");
                                log_add.put("value","Error : Flipkart bad API request");
                            }
                            else if(e.getMessage().equals("105")) {
                                log.setStatus("failed");
                                log_add.put("value","Error : Flipkart API unavailable for long time");
                            }
                            else {
                                log.setStatus("failed");
                                log_add.put("value","Error : "+ e.getMessage());
                                log_add.put("line",e.getStackTrace()[0]);
                            }
                            
                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session1.update(log);
                            tx3.commit();
                            // if requeue required
                            if(e.getMessage().equals("101")) { // token expired or requeue instruction
                                // send same message to kafka current topic with the same message to redo
                                kafkaproducer.sendMessage(message,current_topic);
                                System.out.println("Sent new message to kafka "+ message);
                            }
                        }
                        else {
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

                        // Delete downloaded file after successfully processed - recursively
                        FileUtils.deleteQuietly(new File(local_folder));
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
            // Store Exception for future ref & update message as failed
            this.LogMessageError("Error in message : "+ message+" :Error: "+ e.getMessage());
        }

    }
    */
    
    /*
    @KafkaListener(topics="gcp.store.inventory.amazon_flipkart.sync.0",groupId="nano_group_id")
    public void getAmazonFlipKartInventorySync(String message){
        //System.out.println("listioning ok");
        try{
             String xml_file = new GenerateRandomString(10).toString()+".xml";
             String xml_folder = new GenerateRandomString(10).toString();
             String xml_filepath = xml_folder+"/"+xml_file;
             String current_topic="gcp.store.inventory.amazon_flipkart.sync.0";
             String body=new JSONObject(message).getString("body");
             JSONObject json=new JSONObject(body);
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
             SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
             Boolean client_valid = false;
             Log log = null;
             String local_folder = new GenerateRandomString(10).toString();
             String local_file = new GenerateRandomString(10).toString()+".csv";

             try{
                tx = session.beginTransaction();
                Client client= (Client) session.get(Client.class,client_id);
                if(client!=null) {
                    client_valid=true;
                }
                else {
                    this.LogMessageWarning("Client not found : "+ message);
                }
                tx.commit();
                if(client_valid) {
                    Properties client_db = this.SetClientDB(client.getDb_host(),client.getDb_port(),client.getDb_name(),client.getDb_user(),client.getDb_pass());
                    configuration.setProperties(client_db);
                    SessionFactory sessionFactory1 = configuration.buildSessionFactory();
                    Session session1 = sessionFactory1.openSession();
                    Session exception_session = sessionFactory1.openSession();
                    Session exception_session_token = sessionFactory1.openSession();
                    Transaction tx1 = null;
                    Transaction tx2 = null;
                    Transaction tx3 = null;
                    Transaction tx4 = null;
                    BufferedReader br = null;
                    FileWriter outputfile =null;
                    CSVWriter writer =null;
                    // System.out.println("client is valid");
                    try{
                        tx1 = session1.beginTransaction();
                        log = session1.get(Log.class,log_id);
                        if(log!=null) {
                            log.setStatus("started");
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time",java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log_add.put("value","Task Started");
                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session1.update(log);
                            tx1.commit();
                     
                            // Create Random directory to store the file
                            Files.createDirectories(Paths.get(local_folder));
                            Files.createDirectories(Paths.get(xml_folder));
                            // generate file name
                            String csv_file_report=local_folder+"/"+ local_file;
                            outputfile = new FileWriter(csv_file_report);
                            // create CSVWriter object filewriter object as parameter
                            writer = new CSVWriter(outputfile);
                            String[] header = {"SKU","amazon inventory(new)", "flipkart inventory(old)","flipkart inventory(new)"};
                            writer.writeNext(header);
                            System.out.println("file: "+csv_file_report);
                            Storage storage = StorageOptions.newBuilder()
                                    .setProjectId(env.getProperty("spring.gcp.project-id"))
                                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                    .build()
                                    .getService();
                            
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
                                throw new Exception("Flipkart expire in not found");
                            }

                            if(this.IsMaxTokenCreateFailWithDate("amazon", session1, true)){
                                 this.StopSyncJob(session1, "AMAZON_ORDER_SYNC", false, client.getId(), true);
                                 this.StopSyncJob(session1, "AMAZON_FLIPKART_INVENTORY_SYNC", false, client_id, true);
                                 throw new Exception("Token creation failed max time. Sync Job Stopped");
                            }

                            if(this.IsMaxTokenCreateFailWithDate("flipkart", session1, true)){
                                 this.StopSyncJob(session1, "FLIPKART_ORDER_SYNC", false, client.getId(), true);
                                 this.StopSyncJob(session1, "AMAZON_FLIPKART_INVENTORY_SYNC", false, client_id, true);
                                 throw new Exception("Token creation failed max time. Sync Job Stopped");
                            }

                            tx2 = session1.beginTransaction();

                            Setting expired = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.expired"));
                            // calculate token validity
                            Timestamp token_generated= expire_in.getUpdated_at();
                            Timestamp current_timestamp = Timestamp.valueOf(java.time.LocalDateTime.now());
                            long time_differnce_sec = (current_timestamp.getTime() - token_generated.getTime())/1000;
                            Boolean valid_token =false;
                            if((Integer.valueOf(expire_in.getValue()) - 86400) > time_differnce_sec) {
                                valid_token = true;
                            }
                            
                            if(expired==null || expired.getValue().equals("1") || !valid_token) {
                                // generate new token and continue
                                try {
                                    HttpResponse<JsonNode> token_response = Unirest.get(env.getProperty("spring.integration.flipkart.token_url"))
                                            .basicAuth(env.getProperty("spring.integration.flipkart.app_id"), env.getProperty("spring.integration.flipkart.app_secret"))
                                            .header("accept", "application/json")
                                            .queryString("redirect_uri", env.getProperty("spring.integration.flipkart.redirect_url"))
                                            .queryString("grant_type", "refresh_token")
                                            .queryString("state", env.getProperty("spring.integration.flipkart.state"))
                                            .queryString("refresh_token", refresh_token.getValue())
                                            .asJson();
                                    // retrieve the parsed JSONObject from the response
                                    
                                    if(!token_response.isSuccess()){
                                        throw new Exception("Issue generating flipkart access token using existing refresh token.May be re auth needed");
                                    }

                                    kong.unirest.json.JSONObject token_response_object = token_response.getBody().getObject();
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
                                    System.out.println("new token generated");

                                }
                                catch (Exception ex) {
                                    Transaction tx_exception = exception_session.beginTransaction();
                                    expired.setValue("1");
                                    expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                    exception_session.update(expired);
                                    this.SetMaxTokenCreateFail("flipkart", exception_session, false);
                                    tx_exception.commit();
                                    throw new Exception("Issue generating flipkart token :" + ex.getMessage());
                                }
                            }
                            
                            
                            Setting default_location = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.flipkart.default_location_key"));
                            Setting amz_flipkart_last_sync=session1.bySimpleNaturalId(Setting.class).load("amazon_flipkart_last_sync_time");
                            String amz_flipkart_latest_sync_time = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString().replaceAll("\\.\\d*","").replaceAll("T"," ");
                            String amz_flipkart_latest_sync_time_utc = LocalDateTime.now().toString().replaceAll("\\.\\d*","").replaceAll("T"," ");
                            String amz_flipkart_last_sync_utc = this.IstToUtc(amz_flipkart_last_sync.getValue());
                            int first_row=0;
                            int row_count=10;
                            int item_count=0;
                            List <Item> totalItemList=new ArrayList<>();
                            System.out.println(dateFormatter.parse(amz_flipkart_last_sync.getValue()));
                            System.out.println(dateFormatter.parse(amz_flipkart_latest_sync_time));
                            
                            while(true){
                                 Query query=session1.createQuery("From Item as I WHERE I.sku!=:sku and I.inventory_updated_at between :last_amz_flipkart_sync AND :current_date");
                                 query.setParameter("sku","undefined");
                                 query.setParameter("last_amz_flipkart_sync", dateFormatter.parse(amz_flipkart_last_sync_utc));
                                 query.setParameter("current_date",dateFormatter.parse(amz_flipkart_latest_sync_time_utc));
                                 query.setFirstResult(first_row);
                                 query.setMaxResults(row_count);
                                 List<Item> item_list = query.list();
                                 if(item_list.size()==0){
                                     break;
                                 }
                                 //JSONArray jsonItemArray = new JSONArray();
                                 int row=0;
                                 JSONObject jsonItems = new JSONObject();
                                 for(Item singleItem:item_list){
                                     if(singleItem.getFlipkart_product_id()!=null){
                                          JSONArray  itemLocationArray = new JSONArray();
                                          JSONObject itemLocation = new JSONObject();
                                          JSONObject jsonItemCont = new JSONObject();
                                          jsonItemCont.put("product_id",singleItem.getFlipkart_product_id());
                                          itemLocation.put("id",default_location.getValue());
                                          itemLocation.put("inventory",singleItem.getQuantity());
                                          itemLocationArray.put(itemLocation);
                                          jsonItemCont.put("locations",itemLocationArray);
                                          jsonItems.put(singleItem.getSku(),jsonItemCont);
                                          totalItemList.add(singleItem);
                                          item_count++;
                                     }
                                     row=row+1;
                                 }
                                 System.out.println(jsonItems);//comment
                                 if(jsonItems.toString().equals("{}")){
                                       System.out.println("empty json request ignored\n");
                                 }else{
                                     HttpResponse<JsonNode> flipkart_http_response = Unirest.post(env.getProperty("spring.integration.flipkart.api_url")+"/listings/v3/update/inventory")
                                     .header("Content-Type", "application/json")
                                     .header("Authorization", "Bearer "+ token.getValue())
                                     .header("accept", "application/json")
                                     .body(jsonItems.toString())
                                     .asJson();
                                     
                                        
                                      if(flipkart_http_response.getStatus()==401) { // Token expired
                                          // update settings to set expired true
                                          tx3 = exception_session_token.beginTransaction();
                                          expired.setValue("1");
                                          expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                          exception_session_token.update(expired);
                                          tx3.commit();
                                          throw new Exception("403");
                                      }
                                      if(flipkart_http_response.getStatus()==400 || flipkart_http_response.getStatus()==500) { // bad request
                                          System.out.println(flipkart_http_response.getBody().getObject());
                                          throw new Exception("400");
                                      }
                                      if(flipkart_http_response.getStatus()!=200) { // wait to call again, service unavailable sleep
                                          throw new Exception("404"); // wait to call again
                                      }if(flipkart_http_response.getStatus()==200){
                                          System.out.println("flipkart inventory updated");
                                          sleep(1000*(Integer.valueOf(env.getProperty("spring.integration.flipkart.general_wait_time"))+5));
                                      }
                                }
                                 first_row=first_row+row;

                            }
                            Setting amz_token = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.token_key"));
                            if(amz_token==null || amz_token.getValue().equals("0")) {
                                throw new Exception("Amazn auth token is not found");
                            }
                            Setting amz_refresh_token = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.refresh_token_key"));
                            if(amz_refresh_token==null || amz_refresh_token.getValue().equals("0")) {
                                throw new Exception("Amazon auth refresh token is not found");
                            }
                            Setting amz_expire_in = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.token_expire_in"));
                            if(amz_expire_in==null || amz_expire_in.getValue().equals("0")) {
                                throw new Exception("Amazon auth refresh token is not found");
                            }
                            Setting amz_expired = session1.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.expired"));
                            AmzFeedApiService amzFeedApiService=new AmzFeedApiService();
                            //Session amz_session=sessionFactory1.openSession();
                            try {
                                amzFeedApiService.setAmz_token(amz_token.getValue());
                                amzFeedApiService.setRefresh_token(amz_refresh_token.getValue());
                                amzFeedApiService.setClient_id(env.getProperty("spring.integration.amazon.client_id"));
                                amzFeedApiService.setClient_secret(env.getProperty("spring.integration.amazon.client_secret"));
                                amzFeedApiService.setIam_access_keyId(env.getProperty("spring.integration.amazon.iam_access_keyID"));
                                amzFeedApiService.setIam_access_secret(env.getProperty("spring.integration.amazon.iam_access_secretKey"));
                                amzFeedApiService.setNew_token_url(env.getProperty("spring.integration.amazon.token_url"));
                                amzFeedApiService.setIam_region(env.getProperty("spring.integration.amazon.iam_region"));
                                amzFeedApiService.setSp_url(env.getProperty("spring.integration.amazon.feed_api_url"));
                                amzFeedApiService.createXMLFile(env.getProperty("marketplace_id"),totalItemList,xml_filepath);
                                JSONObject amz_res1=amzFeedApiService.createFeedDocument();
                                amzFeedApiService.xmlUpload(amz_res1.getString("url"),xml_filepath);
                                amzFeedApiService.createFeed(env.getProperty("spring.integration.amazon.marketplace_id"),String.valueOf(amz_res1.getString("feedDocumentId")));
                                String amz_new_token = amzFeedApiService.getAmz_token();
                                amz_token.setValue(amz_new_token);
                                
                                amz_token.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                session1.update(amz_token);
                                amz_expire_in.setValue("3600");
                                amz_expire_in.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                session1.update(amz_expire_in);
                                amz_expired.setValue("0");
                                amz_expired.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                                session1.update(amz_expired);
    
                                amz_flipkart_last_sync.setValue(amz_flipkart_latest_sync_time);
                                session1.update(amz_flipkart_last_sync);
                                
                            } catch (InvalidRefreshTokenException irex) {
                                this.SetMaxTokenCreateFail("amazon", session1, false);
                                throw irex;
                            }
                            
                            for(int i=0; i<totalItemList.size(); i++){
                                Item current_item=totalItemList.get(i);
                                writer.writeNext(new String[]{
                                current_item.getSku(),
                                String.valueOf(current_item.getQuantity()),
                                String.valueOf(current_item.getFlipkart_quantity()),
                                String.valueOf(current_item.getQuantity()),
                                });
                                current_item.setFlipkart_quantity(current_item.getQuantity());
                                session1.update(current_item);
                                sleep(100 * 1);
                            }
                            if(writer!=null) {
                                //closing writer connection
                                writer.close();
                                outputfile.close();
                                writer=null;
                            }
                            BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+env.getProperty("spring.gcp.log-folder-name")+"/"+local_folder+"/"+local_file);
                            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                            storage.create(blobInfo, Files.readAllBytes(Paths.get(csv_file_report)));

                            // Update output file in db
                            JSONArray output_files_array = new JSONArray();
                            JSONObject output_file_object= new JSONObject();
                            output_file_object.put("gcp_file_name",local_file);
                            output_file_object.put("gcp_log_folder_name", "logs");
                            output_file_object.put("gcp_task_folder_name",local_folder);

                            output_files_array.put(output_file_object);
                            log.setOutput_files(output_files_array.toString());
                            log.setStatus("completed");
                            //JSONArray log_description1 = new JSONArray(log.getDescription());
                            JSONObject log_add1= new JSONObject();
                            log_add1.put("key","event");
                            log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                            log_add1.put("value","Task Completed");

                            log_description.put(log_add1);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session1.update(log);
                            
                        }

                        
                         tx2.commit();
                         System.out.println("Task completed");

                    }catch(Exception e){
                        if (tx2 != null) tx2.rollback();
                        e.printStackTrace();
                        // Log client DB issue
                        if(log!=null && log.getId()!=null) {
                            tx3 = session1.beginTransaction();
                            JSONArray log_description = new JSONArray(log.getDescription());
                            JSONObject log_add= new JSONObject();
                            log_add.put("key","event");
                            log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                           
                            if(e.getMessage().equals("403")) {
                                log.setStatus("re-queue");
                                log_add.put("value","Info : Re-queue job due to Flipkart token expired");
                            }else{
                                 log.setStatus("failed");
                                 log_add.put("value","Error : "+ e.getMessage());
                            }
                            
                            log_description.put(log_add);
                            log.setDescription(log_description.toString());
                            log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                            session1.update(log);
                            tx3.commit();
                            if(e.getMessage().equals("403")) {
                                kafkaproducer.sendMessage(message,current_topic);
                                System.out.println("Sent new message to kafka "+ message);
                            }
                        }
                        else {
                            // Log error in system log file
                            this.LogMessageError(e.getMessage());
                        }
                    }finally{
                        session.close();
                        sessionFactory.close();
                        session1.close();
                        exception_session.close();
                        exception_session_token.close();
                        sessionFactory1.close();
                        if(writer!=null) {
                                //closing writer connection
                                writer.close();
                                outputfile.close();
                                writer=null;
                        }
                       FileUtils.deleteQuietly(new File(local_folder));
                       FileUtils.deleteQuietly(new File(xml_folder));
                    }
                }
             }
             catch (Exception e){
                this.LogMessageError("Primary DB issue : "+e.getMessage());
            } finally {
                session.close();
                sessionFactory.close();
            }

        }catch(Exception e){
             this.LogMessageError("Error in message : "+ message+" :Error: "+ e.getMessage());
        }
    }

    */
    public String IstToUtc(String date){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime lDateTimeIst = LocalDateTime.parse(date, formatter);

        // String a = date.split("")
    //     LocalDateTime lDateTimeIst= LocalDateTime.of(
    //        Integer.parseInt(date.substring(0,4)),
    //        Integer.parseInt(date.substring(5,7)),
    //        Integer.parseInt(date.substring(8,10)),
    //        Integer.parseInt(date.substring(11,13)),
    //        Integer.parseInt(date.substring(14,16)),
    //        Integer.parseInt(date.substring(17,19))
    //    );
       LocalDateTime lDateTimeUtc = lDateTimeIst.minus(5,ChronoUnit.HOURS).minus(30,ChronoUnit.MINUTES);
     
       return lDateTimeUtc.format(formatter);
    }

}