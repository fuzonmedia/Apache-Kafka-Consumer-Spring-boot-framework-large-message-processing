package com.consumer.store;
import com.consumer.store.model.*;
import com.consumer.store.service.KafkaProducerService;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.*;
import com.opencsv.CSVWriter;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.apache.commons.io.FileUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;
import org.hibernate.resource.transaction.spi.TransactionStatus;
import org.json.JSONArray;
import org.json.JSONObject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
//log
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import static java.lang.Thread.sleep;

@Service
public class ProductDownloadController {

    @Autowired
    private Environment env;

    @Autowired
    private final KafkaProducerService kafkaproducer;
    public ProductDownloadController(KafkaProducerService kafkaproducer){
        this.kafkaproducer = kafkaproducer;
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


    @KafkaListener(topics="gcp.store.inventory.new.download.0", groupId="nano_group_id")
    public void NewInventoryDownload(String message){
        try{
            String body =  new JSONObject(message).getString("body");
            JSONObject json = new JSONObject(body);
            BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id = BigInteger.valueOf(json.getInt("log_id"));
            Configuration configuration = new Configuration();
            SessionFactory  sessionFactory1 = null;
            Transaction tx1 =  null;
            Session session1 = null;
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String local_file = "NewInventory " + LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1) +".csv";
            String local_folder = this.getRandStr(15);
            configuration.configure();
            configuration.setProperties(this.SetMasterDB());

            try{
                sessionFactory1 = configuration.buildSessionFactory();
                session1 = sessionFactory1.openSession();
                tx1 = session1.beginTransaction();
                Client client = session1.load(Client.class, client_id);
                tx1.commit();
                if(client==null){
                    throw new Exception("Client not found");
                }
                configuration.setProperties(this.SetClientDB(client.getDb_host(), null, client.getDb_name(), client.getDb_user(), client.getDb_pass()));
                SessionFactory sessionFactory2 = null;
                Session session2 = null;
                Transaction tx2 = null;
                Transaction tx3 = null;
                sessionFactory2 = configuration.buildSessionFactory();
                session2 = sessionFactory2.openSession();
                tx2 = session2.beginTransaction();
                Log log = session2.load(Log.class,log_id);
                if(log==null){
                    throw new Exception("Log is not valid");
                }
                try{
                    JSONArray log_description = new JSONArray(log.getDescription());
                    JSONObject log_add = new JSONObject();
                    log.setStatus("started");
                    log_add.put("key", "event");
                    log_add.put("time",LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add.put("value", "Task Started");
                    log_description.put(log_add);
                    log.setDescription(log_description.toString());
                    log.setUpdated_at( Timestamp.valueOf(LocalDateTime.now()));
                    session2.update(log);
                    tx2.commit();
                    tx3 = session2.beginTransaction();
                    Storage storage = StorageOptions.newBuilder()
                                    .setProjectId(env.getProperty("spring.gcp.project-id"))
                                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                    .build()
                                    .getService();
                    String file_path = local_folder + "/" +local_file;
                    Files.createDirectory(Paths.get(local_folder));
                    FileWriter csv_file = new FileWriter(file_path);
                    CSVWriter csv_writer = new CSVWriter(csv_file);
                    String[] headers = {"sku", "qty", "price", "location", "active"};
                    csv_writer.writeNext(headers);
                    Query query = session2.createQuery("From Item IT where IT.newly_added=true and inventory_type=:inventory_type");
                    query.setParameter("inventory_type", "0");
                    List<Item> item_list = query.list();
                    List<String[]> data = new ArrayList<String[]>();
                    for(int i=0; i <item_list.size(); i++){
                        data.add(new String[]{ 
                            item_list.get(i).getSku(),
                            item_list.get(i).getQuantity().toString(), 
                            item_list.get(i).getPrice().toString(),
                            item_list.get(i).getInventory_location(),
                            String.valueOf(item_list.get(i).getActive())
                            });
                    }
                    csv_writer.writeAll(data);
                    csv_writer.close();
                    csv_file.close();

                    BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+env.getProperty("spring.gcp.log-folder-name")+"/"+local_folder+"/"+local_file);
                    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                    storage.create(blobInfo, Files.readAllBytes(Paths.get(file_path)));
                    FileUtils.deleteQuietly(new File(file_path));
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
                    log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add1.put("value","Task Completed");

                    log_description.put(log_add1);
                    log.setDescription(log_description.toString());
                    log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                    session2.update(log);
                    tx3.commit();
                }catch(Exception ex){
                    this.LogMessageError(ex.getMessage());
                    if(tx3!=null) tx3.rollback();
                    Transaction tx4 = session2.beginTransaction();
                    JSONArray log_description = new JSONArray(log.getDescription());
                    JSONObject log_add= new JSONObject();
                    log_add.put("key","event");
                    log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add.put("value","Error : "+ ex.getMessage());
                    log_description.put(log_add);
                    log.setStatus("failed");
                    log.setDescription(log_description.toString());
                    log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                    session2.update(log);
                    tx4.commit();

                   
                }finally{
                   
                    if( session1 != null ){
                        session1.close();
                    }
                    if( sessionFactory1 != null ){
                        sessionFactory1.close();
                    }
                    if( session2 != null ){
                        session2.close();
                    }
                    if( sessionFactory2 != null ){
                        sessionFactory2.close();
                    }
                }
                 
            }catch(Exception err){
                  if( session1 != null ) session1.close();
                  if( sessionFactory1 != null ) sessionFactory1.close();
                  this.LogMessageError("e2"+err.getMessage());
            }

        }catch(Exception e){
            this.LogMessageError(e.getMessage());
        }

    }
    @KafkaListener(topics="gcp.store.inventory.stat.download.0", groupId="nano_group_id")
    public void InventoryStatDownload(String message){
        try{
            String body =  new JSONObject(message).getString("body");
            JSONObject json = new JSONObject(body);
            BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id = BigInteger.valueOf(json.getInt("log_id"));
            Configuration configuration = new Configuration();
            SessionFactory  sessionFactory1 = null;
            Transaction tx1 =  null;
            Session session1 = null;
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String local_file = "ProductStat " + LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1) +".csv";
            String local_folder = this.getRandStr(15);
            configuration.configure();
            configuration.setProperties(this.SetMasterDB());

            try{
                sessionFactory1 = configuration.buildSessionFactory();
                session1 = sessionFactory1.openSession();
                tx1 = session1.beginTransaction();
                Client client = session1.load(Client.class, client_id);
                tx1.commit();
                if(client==null){
                    throw new Exception("Client not found");
                }
                configuration.setProperties(this.SetClientDB(client.getDb_host(), null, client.getDb_name(), client.getDb_user(), client.getDb_pass()));
                SessionFactory sessionFactory2 = null;
                Session session2 = null;
                Transaction tx2 = null;
                Transaction tx3 = null;
                sessionFactory2 = configuration.buildSessionFactory();
                session2 = sessionFactory2.openSession();
                tx2 = session2.beginTransaction();
                Log log = session2.load(Log.class,log_id);
                if(log==null){
                    throw new Exception("Log is not valid");
                }
                try{
                    JSONArray log_description = new JSONArray(log.getDescription());
                    JSONObject log_add = new JSONObject();
                    log.setStatus("started");
                    log_add.put("key", "event");
                    log_add.put("time",LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add.put("value", "Task Started");
                    log_description.put(log_add);
                    log.setDescription(log_description.toString());
                    log.setUpdated_at( Timestamp.valueOf(LocalDateTime.now()));
                    session2.update(log);
                    tx2.commit();
                    tx3 = session2.beginTransaction();
                    Storage storage = StorageOptions.newBuilder()
                                    .setProjectId(env.getProperty("spring.gcp.project-id"))
                                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                    .build()
                                    .getService();
                    String file_path = local_folder + "/" +local_file;
                    Files.createDirectory(Paths.get(local_folder));
                    FileWriter csv_file = new FileWriter(file_path);
                    CSVWriter csv_writer = new CSVWriter(csv_file);
                    String[] headers = {"Parent SKU","SKU","Qty", "rank","Market Places", "Amzon Alias","Flipkart Alias","ASIN","Flipkart Identifier"};
                    csv_writer.writeNext(headers);
                    Query query = session2.createQuery("From Item IT where IT.inventory_type=:inventory_type");
                    query.setParameter("inventory_type", "0");
                    List<Item> item_list = query.list();
                    List<String[]> data = new ArrayList<String[]>();
                    for(int i=0; i <item_list.size(); i++){
                        Product parent_product = item_list.get(i).getProduct();
                        String market_places = "";
                        Boolean amazon_sync,flipkart_sync;
                        JSONArray rank_json_array;
                        amazon_sync = item_list.get(i).getSync_amazon().toString().equals("1");
                        flipkart_sync = item_list.get(i).getSync_flipcart().toString().equals("1");
                        if(amazon_sync && flipkart_sync){
                            market_places = "amazon, flipkart";
                        }else if(flipkart_sync){
                            market_places = "flipkart";
                        }else if(amazon_sync){
                            market_places = "amazon";
                        }
                        if(item_list.get(i).getRank()!=null && !item_list.get(i).getRank().isEmpty())
                        {
                            rank_json_array = new JSONArray( item_list.get(i).getRank());
                        }else{
                            rank_json_array = new JSONArray();
                        }
                        for(int j=0; j< rank_json_array.length();j++){
                             if(rank_json_array.getJSONObject(j).has("link")){
                                 rank_json_array.getJSONObject(j).remove("link");
                             }
                             if(rank_json_array.getJSONObject(j).has("rank")){
                                rank_json_array.getJSONObject(j).remove("rank");
                             }
                        }
                        data.add(new String[]{ 
                            parent_product.getSku(),
                            item_list.get(i).getSku(),
                            item_list.get(i).getQuantity().toString(),
                            rank_json_array.toString(), 
                            market_places,
                            item_list.get(i).getAmazon_sku_alias(),
                            item_list.get(i).getFlipkart_sku_alias(),
                            item_list.get(i).getAsin(),
                            item_list.get(i).getFlipkart_product_id(),
                        });
                    }
                    csv_writer.writeAll(data);
                    csv_writer.close();
                    csv_file.close();

                    BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+env.getProperty("spring.gcp.log-folder-name")+"/"+local_folder+"/"+local_file);
                    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                    storage.create(blobInfo, Files.readAllBytes(Paths.get(file_path)));
                    FileUtils.deleteQuietly(new File(file_path));

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
                    log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add1.put("value","Task Completed");

                    log_description.put(log_add1);
                    log.setDescription(log_description.toString());
                    log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                    session2.update(log);
                    tx3.commit();
                }catch(Exception ex){
                    this.LogMessageError(ex.getMessage());
                    if(tx3!=null) tx3.rollback();
                    Transaction tx4 = session2.beginTransaction();
                    JSONArray log_description = new JSONArray(log.getDescription());
                    JSONObject log_add= new JSONObject();
                    log_add.put("key","event");
                    log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add.put("value","Error : "+ ex.getMessage());
                    log_description.put(log_add);
                    log.setStatus("failed");
                    log.setDescription(log_description.toString());
                    log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                    session2.update(log);
                    tx4.commit();

                   
                }finally{
                   
                    if( session1 != null ){
                        session1.close();
                    }
                    if( sessionFactory1 != null ){
                        sessionFactory1.close();
                    }
                    if( session2 != null ){
                        session2.close();
                    }
                    if( sessionFactory2 != null ){
                        sessionFactory2.close();
                    }
                }
                 
            }catch(Exception err){
                  if( session1 != null ) session1.close();
                  if( sessionFactory1 != null ) sessionFactory1.close();
                  this.LogMessageError("e2"+err.getMessage());
            }

        }catch(Exception e){
            this.LogMessageError(e.getMessage());
        }



    }

    @KafkaListener(topics="gcp.store.product.stat.download.0", groupId="nano_group_id")
    public void ProductStatDownload(String message){
        try{
            String body =  new JSONObject(message).getString("body");
            JSONObject json = new JSONObject(body);
            BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id = BigInteger.valueOf(json.getInt("log_id"));
            Configuration configuration = new Configuration();
            SessionFactory  sessionFactory1 = null;
            Transaction tx1 =  null;
            Session session1 = null;
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String local_file = this.getRandStr(15)+".csv";
            String local_folder = this.getRandStr(15);
            configuration.configure();
            configuration.setProperties(this.SetMasterDB());

            try{
                sessionFactory1 = configuration.buildSessionFactory();
                session1 = sessionFactory1.openSession();
                tx1 = session1.beginTransaction();
                Client client = session1.load(Client.class, client_id);
                tx1.commit();
                if(client==null){
                    throw new Exception("Client not found");
                }
                configuration.setProperties(this.SetClientDB(client.getDb_host(), null, client.getDb_name(), client.getDb_user(), client.getDb_pass()));
                SessionFactory sessionFactory2 = null;
                Session session2 = null;
                Transaction tx2 = null;
                Transaction tx3 = null;
                sessionFactory2 = configuration.buildSessionFactory();
                session2 = sessionFactory2.openSession();
                tx2 = session2.beginTransaction();
                Log log = session2.load(Log.class,log_id);
                if(log==null){
                    throw new Exception("Log is not valid");
                }
                try{
                    JSONArray log_description = new JSONArray(log.getDescription());
                    JSONObject log_add = new JSONObject();
                    log.setStatus("started");
                    log_add.put("key", "event");
                    log_add.put("time",LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add.put("value", "Task Started");
                    log_description.put(log_add);
                    log.setDescription(log_description.toString());
                    log.setUpdated_at( Timestamp.valueOf(LocalDateTime.now()));
                    session2.update(log);
                    tx2.commit();
                    tx3 = session2.beginTransaction();
                    Storage storage = StorageOptions.newBuilder()
                                    .setProjectId(env.getProperty("spring.gcp.project-id"))
                                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                    .build()
                                    .getService();
                    String file_path = local_folder + "/" +local_file;
                    Files.createDirectory(Paths.get(local_folder));
                    FileWriter csv_file = new FileWriter(file_path);
                    CSVWriter csv_writer = new CSVWriter(csv_file);
                    String[] headers = {"SKU", "Rank"};
                    csv_writer.writeNext(headers);
                    Query query = session2.createQuery("From Product");
                    List<Product> product_list = query.list();
                    List<String[]> data = new ArrayList<String[]>();
                    for(int i=0; i <product_list.size(); i++){
                        data.add(new String[]{ 
                            product_list.get(i).getSku(),
                            product_list.get(i).getRank(),
                        });
                    }
                    csv_writer.writeAll(data);
                    csv_writer.close();
                    csv_file.close();

                    BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+env.getProperty("spring.gcp.log-folder-name")+"/"+local_folder+"/"+local_file);
                    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                    storage.create(blobInfo, Files.readAllBytes(Paths.get(file_path)));
                    FileUtils.deleteQuietly(new File(file_path));
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
                    log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add1.put("value","Task Completed");

                    log_description.put(log_add1);
                    log.setDescription(log_description.toString());
                    log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                    session2.update(log);
                    tx3.commit();
                }catch(Exception ex){
                    this.LogMessageError(ex.getMessage());
                    if(tx3!=null) tx3.rollback();
                    Transaction tx4 = session2.beginTransaction();
                    JSONArray log_description = new JSONArray(log.getDescription());
                    JSONObject log_add= new JSONObject();
                    log_add.put("key","event");
                    log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add.put("value","Error : "+ ex.getMessage());
                    log_description.put(log_add);
                    log.setStatus("failed");
                    log.setDescription(log_description.toString());
                    log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                    session2.update(log);
                    tx4.commit();

                   
                }finally{
                   
                    if( session1 != null ){
                        session1.close();
                    }
                    if( sessionFactory1 != null ){
                        sessionFactory1.close();
                    }
                    if( session2 != null ){
                        session2.close();
                    }
                    if( sessionFactory2 != null ){
                        sessionFactory2.close();
                    }
                }
                 
            }catch(Exception err){
                  if( session1 != null ) session1.close();
                  if( sessionFactory1 != null ) sessionFactory1.close();
                  this.LogMessageError(err.getMessage());
            }

        }catch(Exception e){
            this.LogMessageError(e.getMessage());
        }
    }

    @KafkaListener(topics="gcp.store.product.all.download.0", groupId="nano_group_id")
    public void AllProductDownload(String message){
        try{
            String body =  new JSONObject(message).getString("body");
            JSONObject json = new JSONObject(body);
            BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id = BigInteger.valueOf(json.getInt("log_id"));
            Configuration configuration = new Configuration();
            SessionFactory  sessionFactory1 = null;
            Transaction tx1 =  null;
            Session session1 = null;
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String local_file = this.getRandStr(15)+".csv";
            String local_folder = this.getRandStr(15);
            configuration.configure();
            configuration.setProperties(this.SetMasterDB());

            try{
                sessionFactory1 = configuration.buildSessionFactory();
                session1 = sessionFactory1.openSession();
                tx1 = session1.beginTransaction();
                Client client = session1.load(Client.class, client_id);
                tx1.commit();
                if(client==null){
                    throw new Exception("Client not found");
                }
                configuration.setProperties(this.SetClientDB(client.getDb_host(), null, client.getDb_name(), client.getDb_user(), client.getDb_pass()));
                SessionFactory sessionFactory2 = null;
                Session session2 = null;
                Transaction tx2 = null;
                Transaction tx3 = null;
                sessionFactory2 = configuration.buildSessionFactory();
                session2 = sessionFactory2.openSession();
                tx2 = session2.beginTransaction();
                Log log = session2.load(Log.class,log_id);
                if(log==null){
                    throw new Exception("Log is not valid");
                }
                try{
                    JSONArray log_description = new JSONArray(log.getDescription());
                    JSONObject log_add = new JSONObject();
                    log.setStatus("started");
                    log_add.put("key", "event");
                    log_add.put("time",LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add.put("value", "Task Started");
                    log_description.put(log_add);
                    log.setDescription(log_description.toString());
                    log.setUpdated_at( Timestamp.valueOf(LocalDateTime.now()));
                    session2.update(log);
                    tx2.commit();
                    tx3 = session2.beginTransaction();
                    Storage storage = StorageOptions.newBuilder()
                                    .setProjectId(env.getProperty("spring.gcp.project-id"))
                                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                    .build()
                                    .getService();
                    String file_path = local_folder + "/" +local_file;
                    Files.createDirectory(Paths.get(local_folder));
                    FileWriter csv_file = new FileWriter(file_path);
                    CSVWriter csv_writer = new CSVWriter(csv_file);
                    String[] headers = {"SKU", "Name","Descrition","asin","images"};
                    csv_writer.writeNext(headers);
                    Query query = session2.createQuery("From Product");
                    List<Product> product_list = query.list();
                    List<String[]> data = new ArrayList<String[]>();
                    for(int i=0; i <product_list.size(); i++){
                        String pictures = product_list.get(i).getPictures(),picture_data="";
                        if(pictures != null && pictures.isEmpty()!=true){
                           JSONObject pictures_json = new JSONObject(pictures);
                           if(pictures_json.has("images")){
                             JSONArray  images = pictures_json.getJSONArray("images");
                             String image_host = "https://storage.googleapis.com";
                             String image_path =  image_host + "/" + pictures_json.getString("bucket") + "/" + pictures_json.getString("client") + "/" + pictures_json.getString("folder");
                             for(int j=0; j< images.length(); j++){
                                 Iterator<String> keys = images.getJSONObject(j).keys();
                                 while(keys.hasNext()) {
                                    String key = keys.next();
                                    if (images.getJSONObject(j).get(key) instanceof String) {
                                          picture_data += picture_data.length()==0? image_path + "/" + images.getJSONObject(j).getString(key): "," + image_path + "/" + images.getJSONObject(j).getString(key);   
                                    }
                                }
                             }
                           }
                        }

                        data.add(new String[]{ 
                            product_list.get(i).getSku(),
                            product_list.get(i).getName(),
                            product_list.get(i).getDescription(),
                            product_list.get(i).getAsin(),
                            picture_data,
                        });
                    }
                    csv_writer.writeAll(data);
                    csv_writer.close();
                    csv_file.close();

                    BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+env.getProperty("spring.gcp.log-folder-name")+"/"+local_folder+"/"+local_file);
                    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                    storage.create(blobInfo, Files.readAllBytes(Paths.get(file_path)));
                    FileUtils.deleteQuietly(new File(file_path));
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
                    log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add1.put("value","Task Completed");

                    log_description.put(log_add1);
                    log.setDescription(log_description.toString());
                    log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                    session2.update(log);
                    tx3.commit();
                }catch(Exception ex){
                    this.LogMessageError(ex.getMessage());
                    if(tx3!=null) tx3.rollback();
                    Transaction tx4 = session2.beginTransaction();
                    JSONArray log_description = new JSONArray(log.getDescription());
                    JSONObject log_add= new JSONObject();
                    log_add.put("key","event");
                    log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add.put("value","Error : "+ ex.getMessage());
                    log_description.put(log_add);
                    log.setStatus("failed");
                    log.setDescription(log_description.toString());
                    log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                    session2.update(log);
                    tx4.commit();

                   
                }finally{
                   
                    if( session1 != null ){
                        session1.close();
                    }
                    if( sessionFactory1 != null ){
                        sessionFactory1.close();
                    }
                    if( session2 != null ){
                        session2.close();
                    }
                    if( sessionFactory2 != null ){
                        sessionFactory2.close();
                    }
                }
                 
            }catch(Exception err){
                  if( session1 != null ) session1.close();
                  if( sessionFactory1 != null ) sessionFactory1.close();
                  this.LogMessageError(err.getMessage());
            }

        }catch(Exception e){
            this.LogMessageError(e.getMessage());
        }
    }

    @KafkaListener(topics="gcp.store.inventory.all.download.0", groupId="nano_group_id")
    public void AllInventoryDownload(String message){
        try{
            String body =  new JSONObject(message).getString("body");
            JSONObject json = new JSONObject(body);
            BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id = BigInteger.valueOf(json.getInt("log_id"));
            Configuration configuration = new Configuration();
            SessionFactory  sessionFactory1 = null;
            Transaction tx1 =  null;
            Session session1 = null;
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String local_file = "ListingData " + LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1) +".csv";
            String local_folder = this.getRandStr(15);
            configuration.configure();
            configuration.setProperties(this.SetMasterDB());

            try{
                sessionFactory1 = configuration.buildSessionFactory();
                session1 = sessionFactory1.openSession();
                tx1 = session1.beginTransaction();
                Client client = session1.load(Client.class, client_id);
                tx1.commit();
                if(client==null){
                    throw new Exception("Client not found");
                }
                configuration.setProperties(this.SetClientDB(client.getDb_host(), null, client.getDb_name(), client.getDb_user(), client.getDb_pass()));
                SessionFactory sessionFactory2 = null;
                Session session2 = null;
                Transaction tx2 = null;
                Transaction tx3 = null;
                sessionFactory2 = configuration.buildSessionFactory();
                session2 = sessionFactory2.openSession();
                tx2 = session2.beginTransaction();
                Log log = session2.load(Log.class,log_id);
                if(log==null){
                    throw new Exception("Log is not valid");
                }
                try{
                    JSONArray log_description = new JSONArray(log.getDescription());
                    JSONObject log_add = new JSONObject();
                    log.setStatus("started");
                    log_add.put("key", "event");
                    log_add.put("time",LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add.put("value", "Task Started");
                    log_description.put(log_add);
                    log.setDescription(log_description.toString());
                    log.setUpdated_at( Timestamp.valueOf(LocalDateTime.now()));
                    session2.update(log);
                    tx2.commit();
                    tx3 = session2.beginTransaction();
                    Storage storage = StorageOptions.newBuilder()
                                    .setProjectId(env.getProperty("spring.gcp.project-id"))
                                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                    .build()
                                    .getService();
                    String file_path = local_folder + "/" +local_file;
                    Files.createDirectory(Paths.get(local_folder));
                    FileWriter csv_file = new FileWriter(file_path);
                    CSVWriter csv_writer = new CSVWriter(csv_file);
                    String[] headers =new String[]{"Parent SKU","SKU","Qty", "Name","Description","MRP","Price","HSN","Image","Status"};
                   // csv_writer.writeNext(headers);
                    Query query = session2.createQuery("From Item where inventory_type=:inventory_type");
                    query.setParameter("inventory_type","0");
                    List<Item> item_list = query.list();
                    List<ArrayList<String>> data = new ArrayList<>();
                    // int total_image_field =0;
                    for(int i=0; i <item_list.size(); i++){
                        try {
                            Product parent_product = item_list.get(i).getProduct();
                            String pictures = parent_product.getPictures();
                            List<String> picture_data = new ArrayList<String>();
                            String mrp = item_list.get(i).getMrp()!=null? item_list.get(i).getMrp().toString(): "";
                            String price = item_list.get(i).getPrice()!=null? item_list.get(i).getPrice().toString(): "";
                            int m = 0;
                            if(pictures != null && pictures.isEmpty()!=true){
                                JSONObject pictures_json = new JSONObject(pictures);
                                if(pictures_json.has("bucket") && pictures_json.has("client") && pictures_json.has("folder")){
                                    JSONArray  images;
                                    String image_host = "https://storage.googleapis.com";
                                    String image_path =  image_host + "/" + pictures_json.getString("bucket") + "/" + pictures_json.getString("client") + "/" + pictures_json.getString("folder");
                                    if(pictures_json.has("images") && (images = pictures_json.getJSONArray("images"))!=null){
                                      for(int j=0; j< images.length(); j++){
                                          Iterator<String> keys = images.getJSONObject(j).keys();
                                          while(keys.hasNext()) {
                                             String key = keys.next();
                                             if (images.getJSONObject(j).get(key) instanceof String) {
                                                   picture_data.add( image_path + "/" + images.getJSONObject(j).getString(key) );   
                                                   break;
                                             }
                                         }
                                      }
                                    }
                                }
                             }
                             ArrayList<String>  data_element = new ArrayList<String>();
                             data_element.add(parent_product.getSku()); 
                             data_element.add(item_list.get(i).getSku());
                             data_element.add(item_list.get(i).getQuantity().toString());
                             data_element.add(item_list.get(i).getName());
                             data_element.add(parent_product.getDescription());
                             data_element.add(mrp);
                             data_element.add(price);
                             data_element.add(parent_product.getHsn_code());
                            //  int image_field = 0;
                            //  for(int k=0; k <picture_data.size(); k++){
                            //       data_element.add(picture_data.get(k));
                            //       image_field ++;
                            //  }
                            //  if(image_field >= total_image_field){
                            //      total_image_field = image_field;
                            //  }
                             data_element.add( String.join(",", picture_data));
                             data_element.add("Successful");
                             data.add(data_element);
                        } catch (Exception e) {
                             ArrayList<String>  data_element = new ArrayList<String>();
                             data_element.add(""); 
                             data_element.add(item_list.get(i).getSku());
                             data_element.add("");
                             data_element.add("");
                             data_element.add("");
                             data_element.add("");
                             data_element.add("");
                             data_element.add("");
                             data_element.add("");
                             data_element.add("failed");
                             data.add( data_element );
                        }
                    }
                    ArrayList<String> header_list = new ArrayList<String>(Arrays.asList(headers));
                    //add all image header
                    // for(int i=0;i<total_image_field;i++){
                    //        header_list.add("image"+(i+1));
                    // }
                    data.add(0,header_list);
                    List<String[]> new_data = new ArrayList<String[]>(); 
                    for(int i=0; i<data.size(); i++){
                         new_data.add( data.get(i).toArray( new String[data.get(i).size()] ) );
                    }
                    csv_writer.writeAll(new_data);
                    csv_writer.close();
                    csv_file.close();

                    BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName()+"/"+env.getProperty("spring.gcp.log-folder-name")+"/"+local_folder+"/"+local_file);
                    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                    storage.create(blobInfo, Files.readAllBytes(Paths.get(file_path)));
                    FileUtils.deleteQuietly(new File(file_path));
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
                    log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add1.put("value","Task Completed");

                    log_description.put(log_add1);
                    log.setDescription(log_description.toString());
                    log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                    session2.update(log);
                    tx3.commit();
                }catch(Exception ex){
                    this.LogMessageError(ex.getMessage());
                    if(tx3!=null) tx3.rollback();
                    Transaction tx4 = session2.beginTransaction();
                    JSONArray log_description = new JSONArray(log.getDescription());
                    JSONObject log_add= new JSONObject();
                    log_add.put("key","event");
                    log_add.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                    log_add.put("value","Error : "+ ex.getMessage());
                    log_description.put(log_add);
                    log.setStatus("failed");
                    log.setDescription(log_description.toString());
                    log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                    session2.update(log);
                    tx4.commit();

                   
                }finally{
                   
                    if( session1 != null ){
                        session1.close();
                    }
                    if( sessionFactory1 != null ){
                        sessionFactory1.close();
                    }
                    if( session2 != null ){
                        session2.close();
                    }
                    if( sessionFactory2 != null ){
                        sessionFactory2.close();
                    }
                }
                 
            }catch(Exception err){
                  if( session1 != null ) session1.close();
                  if( sessionFactory1 != null ) sessionFactory1.close();
                  this.LogMessageError(err.getMessage());
            }

        }catch(Exception e){
            this.LogMessageError(e.getMessage());
        }
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
    
}
