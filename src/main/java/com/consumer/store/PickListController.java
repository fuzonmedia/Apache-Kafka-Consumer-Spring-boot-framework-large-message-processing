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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
//log
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Service
public class PickListController {

    @Autowired
    private Environment env;

    @Autowired
    private final KafkaProducerService kafkaproducer;

    public PickListController(KafkaProducerService kafkaproducer){
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

    @KafkaListener(topics="gcp.store.wms.picklist.download.0", groupId="nano_group_id")
    public void PickListDownload(String message){
        try{
            String body =  new JSONObject(message).getString("body");
            JSONObject json = new JSONObject(body);
            BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id = BigInteger.valueOf(json.getInt("log_id"));
            BigInteger pick_list_id = BigInteger.valueOf(json.getInt("pick_list_id"));
            Configuration configuration = new Configuration();
            SessionFactory  sessionFactory1 = null;
            Transaction tx1 =  null;
            Session session1 = null;
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String local_file = "pick_list_" + LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1) +".csv";
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
                    FileWriter csv_file   = new FileWriter(file_path);
                    CSVWriter  csv_writer = new CSVWriter(csv_file);

                    PickList picklist = session2.load(PickList.class, pick_list_id);
                    if(picklist != null){
                        Query query = session2.createQuery("From PickListItem PLITM where PLITM.picklist=:picklist");
                        query.setParameter("picklist", picklist);
                        List<PickListItem> item_list = query.list();
                        List<PickListItem> filtered_item_list = new ArrayList<PickListItem>();
                        String[] headers = {"Pick List ID", "sku", "qty", "Bin", "status"};
                        csv_writer.writeNext(headers);
                        List<String[]> data = new ArrayList<String[]>();
                        List<HashMap<String, String>> filter_buffer = new ArrayList<HashMap<String,String>>();

                        for(int i=0; i <item_list.size(); i++){
                            BigInteger total_qty = new BigInteger("0");
                            HashMap<String,String> buffer_item = new HashMap<>();
                            Boolean exist = false;
                            for (int j = 0; j < filter_buffer.size(); j++) {
                                if( filter_buffer.get(j).get("sku").equals( item_list.get(i).getSku() )){
                                     exist = true;
                                     break;
                                }
                            }
                            if(Boolean.FALSE.equals(exist)){
                                total_qty = item_list.get(i).getQuantity();
                                for (int j = i + 1; j < item_list.size(); j++) {
                                    if(item_list.get(j).getSku().equals( item_list.get(i).getSku() )){
                                        total_qty = total_qty.add( item_list.get(j).getQuantity() );
                                    }
                                }
                                buffer_item.put("sku",      item_list.get(i).getSku());
                                buffer_item.put("quantity", total_qty.toString());
                                buffer_item.put("bin",      item_list.get(i).getBin());
                                buffer_item.put("status",   item_list.get(i).getStatus());
                                filter_buffer.add( buffer_item );
                            }
                        }
                        System.out.println("<<<<Below is the filter_buffer Array of JSON>>>>");
                        System.out.println(filter_buffer);
                        
                        for (int i = 0; i < filter_buffer.size(); i++) {
                            if (filter_buffer.get(i).get("bin") == null) {
                                break;
                            }
                            if (i==filter_buffer.size()-1)
                            {
                                filter_buffer = filter_buffer.stream().sorted((a,b) -> a.get("bin").equals(null) || b.get("bin").equals(null)? 0 : a.get("bin").compareTo(b.get("bin"))).collect(Collectors.toList());
                            }
                        }
                        
                        for (int i = 0; i < filter_buffer.size(); i++) {
                            data.add(new String[]{
                                picklist.getName(),
                                filter_buffer.get(i).get("sku"),
                                filter_buffer.get(i).get("quantity"),
                                filter_buffer.get(i).get("bin"),
                                filter_buffer.get(i).get("status"),
                            });
                        }

                        csv_writer.writeAll(data);
                    }else{
                        csv_writer.writeNext(new String[]{"picklist not found"});
                    }

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
                    ex.printStackTrace();
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
                  err.printStackTrace();
                  if( session1 != null ) session1.close();
                  if( sessionFactory1 != null ) sessionFactory1.close();
                  this.LogMessageError("e2"+err.getMessage());
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
