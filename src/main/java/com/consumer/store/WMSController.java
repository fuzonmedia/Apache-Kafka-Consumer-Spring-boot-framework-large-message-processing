package com.consumer.store;
import com.consumer.store.helper.GenerateRandomString;
import com.consumer.store.model.*;
import com.consumer.store.service.KafkaProducerService;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.threeten.bp.LocalDate;
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
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
import static java.lang.Thread.sleep;
import com.consumer.store.model.WMSTransaction;
import com.consumer.store.model.Item;  

@Service
public class  WMSController{

    @Autowired
    private Environment env;

    @Autowired
    private final KafkaProducerService kafkaproducer;
    public  WMSController(KafkaProducerService kafkaproducer){
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


    @KafkaListener(topics="gcp.store.wms.inbound.inventory.add.0", groupId="nano_group_id")
    public void inboundAddInventory(String message){
          // Extract JSON body of the messaget
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
            
            CSVReader  cr = null;
            FileWriter fw = null;
            CSVWriter  cw = null;
            FileReader fr = null;
            // CSVWriter writer = null;

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
                        String[] requiredHeaders = {"sku", "qty", "reason"};
                        HashMap<String,Integer> headerIndex = new HashMap<>();
    
                        for (int i=0; i<csvInputHeaders.length; i++) {
                            csvInputHeadersLowercase[i] = csvInputHeaders[i].toLowerCase();
                        }
                        Arrays.sort(requiredHeaders);
                        Integer count = 0;
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
                            String  sku    = csvInputRow[ headerIndex.get("sku") ];
                            Integer quantity    = csvInputRow[ headerIndex.get("qty") ]!=null && !csvInputRow[ headerIndex.get("qty")].isEmpty()? Integer.valueOf( csvInputRow[ headerIndex.get("qty") ] ): 1;
                            String  reason = csvInputRow[ headerIndex.get("reason") ];

                            if(quantity <= 0){
                                 quantity = 0;
                            }

                            // if(quantity<=0){
                            //     /** Check for Duplicate SKUs */
                            //     HashMap<String,Object> outputRow = new HashMap<>();
                                
                            //     Integer itemPosition = -1;
                            //     if(outputRow  !=  null){
                                     
                            //          if((itemPosition  = csvOutputData.indexOf(outputRow))>-1){
                            //              csvOutputData.remove(itemPosition.intValue());
                            //          }
                                  
                            //          outputRow.put("grn_quantity", newWmsTransaction.getQuantity() + (Integer)outputRow.get("grn_quantity"));
                            //          outputRow.put("total_quantity", item.getQuantity());
                            //          outputRow.put("total_quantity", item.getQuantity());
                            //          outputRow.put("status", "success");
                            //     }
                            //     if(outputRow  == null){
                            //          outputRow = new HashMap<>();
                            //          outputRow.put("sku", item.getSku());
                            //          outputRow.put("system_quantity",previousQuantity);
                            //          outputRow.put("grn_quantity", newWmsTransaction.getQuantity());
                            //          outputRow.put("total_quantity", item.getQuantity());
                            //          outputRow.put("reason", newWmsTransaction.getReason());
                            //          outputRow.put("status", "success");
                            //     }
                            //     if(itemPosition != -1){
                            //         csvOutputData.add(itemPosition.intValue(),outputRow);
                            //     }else{
                            //         csvOutputData.add(outputRow);
                            //     }

                            //     /**  */
                            //     // outputRow.put("sku", sku);
                            //     // outputRow.put("system_quantity","");
                            //     // outputRow.put("grn_quantity", "");
                            //     // outputRow.put("total_quantity", "");
                            //     // outputRow.put("reason", "");
                            //     // outputRow.put("status", "ignored: quantity to be added is 0");
                            //     // csvOutputData.add(outputRow);

                            //     continue;
                            // }

                            if(sku.isEmpty()){
                                HashMap<String,Object> outputRow = new HashMap<>();
                                outputRow.put("sku", sku);
                                outputRow.put("system_quantity","");
                                outputRow.put("grn_quantity", "");
                                outputRow.put("total_quantity", "");
                                outputRow.put("reason", "");
                                outputRow.put("status", "fail: sku is empty");
                                csvOutputData.add(outputRow);
                                continue;
                            }

                            Item item = session2.bySimpleNaturalId(Item.class).load(sku);
                            if(item == null){
                                HashMap<String,Object> outputRow =  csvOutputData.stream()
                                                .filter(itemCheck -> itemCheck.get("sku").equals(sku))
                                                .findAny()
                                                .orElse(null);
                                if(outputRow == null){
                                    outputRow = new HashMap<>(); 
                                    outputRow.put("sku", sku);
                                    outputRow.put("system_quantity","");
                                    outputRow.put("grn_quantity", quantity);
                                    outputRow.put("total_quantity", "");
                                    outputRow.put("reason", "");
                                    outputRow.put("status", "fail: sku not found");
                                    csvOutputData.add(outputRow);
                                }else{
                                    Integer itemPosition;
                                    if((itemPosition  = csvOutputData.indexOf(outputRow))>-1){
                                        csvOutputData.remove(itemPosition.intValue());
                                        outputRow.put("grn_quantity", quantity + (Integer)outputRow.get("grn_quantity"));
                                        csvOutputData.add(itemPosition.intValue(),outputRow);
                                    }
                                }                             
                                
                            }else{
                                Integer previousQuantity = item.getQuantity();
                                item.setQuantity(item.getQuantity() + quantity);
                                item.setInventory_updated_at( Timestamp.valueOf( LocalDateTime.now() ) );
                                session2.update(item);
                                WMSTransaction newWmsTransaction = new WMSTransaction();
                                newWmsTransaction.setQuantity(quantity);
                                newWmsTransaction.setItem(item);
                                newWmsTransaction.setCreated_at(Timestamp.valueOf(LocalDateTime.now()));
                                newWmsTransaction.setReason(reason);
                                newWmsTransaction.setIs_added(true);
                                session2.save(newWmsTransaction);

                                HashMap<String,Object> outputRow =  csvOutputData.stream()
                                                .filter(itemCheck -> itemCheck.get("sku").equals(item.getSku()))
                                                .findAny()
                                                .orElse(null);
                                Integer itemPosition = -1;
                                if(outputRow  !=  null){
                                     
                                     if((itemPosition  = csvOutputData.indexOf(outputRow))>-1){
                                         csvOutputData.remove(itemPosition.intValue());
                                     }
                                  
                                     outputRow.put("grn_quantity", newWmsTransaction.getQuantity() + (Integer)outputRow.get("grn_quantity"));
                                     outputRow.put("total_quantity", item.getQuantity());
                                     outputRow.put("total_quantity", item.getQuantity());
                                     outputRow.put("status", "success");
                                }
                                if(outputRow  == null){
                                     outputRow = new HashMap<>();
                                     outputRow.put("sku", item.getSku());
                                     outputRow.put("system_quantity",previousQuantity);
                                     outputRow.put("grn_quantity", newWmsTransaction.getQuantity());
                                     outputRow.put("total_quantity", item.getQuantity());
                                     outputRow.put("reason", newWmsTransaction.getReason());
                                     outputRow.put("status", "success");
                                }
                                if(itemPosition != -1){
                                    csvOutputData.add(itemPosition.intValue(),outputRow);
                                }else{
                                    csvOutputData.add(outputRow);
                                }
                            }
                        }
                        cw.writeNext(new String[]{"SKU",    "System Qty",	"GRN Qty",	"Total",    "Reason",	"Status"});
                        
                        for (int i = 0; i < csvOutputData.size(); i++) {
                            HashMap<String,Object> csvOutputRow = csvOutputData.get(i);
                            cw.writeNext(new String[]{ 
                                 csvOutputRow.get("sku").toString(),
                                 csvOutputRow.get("system_quantity").toString(),
                                 csvOutputRow.get("grn_quantity").toString(),
                                 csvOutputRow.get("total_quantity").toString(),
                                 csvOutputRow.get("reason").toString(),
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
                log_add1.put("value", "Task Started");
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
    @KafkaListener(topics="gcp.store.wms.inbound.inventory.subtract.0", groupId="nano_group_id")
    public void inboundSubstractInventory(String message){
          // Extract JSON body of the messaget
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
                        String[] requiredHeaders = {"sku", "qty", "reason"};
                        HashMap<String,Integer> headerIndex = new HashMap<>();
    
                        for (int i=0; i<csvInputHeaders.length; i++) {
                            csvInputHeadersLowercase[i] = csvInputHeaders[i].toLowerCase();
                        }
                        Arrays.sort(requiredHeaders);
                        Integer count = 0;
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
                            String  sku    = csvInputRow[ headerIndex.get("sku") ];
                            Integer quantity    = csvInputRow[ headerIndex.get("qty") ]!=null && !csvInputRow[ headerIndex.get("qty")].isEmpty()? Integer.valueOf( csvInputRow[ headerIndex.get("qty") ] ): 1;
                            String  reason = csvInputRow[ headerIndex.get("reason") ];
                            if(quantity < 0){
                                 quantity = 0;
                            }
                            if(sku.isEmpty()){
                                HashMap<String,Object> outputRow = new HashMap<>();
                                outputRow.put("sku", sku);
                                outputRow.put("system_quantity","");
                                outputRow.put("grn_quantity", "");
                                outputRow.put("total_quantity", "");
                                outputRow.put("reason", "");
                                outputRow.put("status", "fail: sku is empty");
                                csvOutputData.add(outputRow);
                                continue;
                            }

                            Item item = session2.bySimpleNaturalId(Item.class).load(sku);
                            if(item == null){
                                HashMap<String,Object> outputRow =  csvOutputData.stream()
                                                .filter(itemCheck -> itemCheck.get("sku").equals(sku))
                                                .findAny()
                                                .orElse(null);
                                if(outputRow == null){
                                    outputRow = new HashMap<>(); 
                                    outputRow.put("sku", sku);
                                    outputRow.put("system_quantity","");
                                    outputRow.put("grn_quantity", quantity);
                                    outputRow.put("total_quantity", "");
                                    outputRow.put("reason", "");
                                    outputRow.put("status", "fail: sku not found");
                                    csvOutputData.add(outputRow);
                                }else{
                                    Integer itemPosition;
                                    if((itemPosition  = csvOutputData.indexOf(outputRow))>-1){
                                        csvOutputData.remove(itemPosition.intValue());
                                        outputRow.put("grn_quantity", quantity + (Integer)outputRow.get("grn_quantity"));
                                        csvOutputData.add(itemPosition.intValue(),outputRow);
                                    }
                                }                             
                                
                            }else{
                                Integer oldQty = item.getQuantity();
                                Integer calQty = item.getQuantity() - quantity;
                                Integer newQty = calQty < 0? 0: calQty;
                                if( oldQty != newQty){
                                  item.setQuantity( newQty );
                                  item.setInventory_updated_at( Timestamp.valueOf( LocalDateTime.now() ) );
                                  session2.update(item);
                                }
                                WMSTransaction newWmsTransaction = new WMSTransaction();
                                newWmsTransaction.setQuantity(quantity);
                                newWmsTransaction.setItem(item);
                                newWmsTransaction.setCreated_at(Timestamp.valueOf(LocalDateTime.now()));
                                newWmsTransaction.setReason(reason);
                                newWmsTransaction.setIs_added(false);
                                session2.save(newWmsTransaction);

                                HashMap<String,Object> outputRow =  csvOutputData.stream()
                                                .filter(itemCheck -> itemCheck.get("sku").equals(item.getSku()))
                                                .findAny()
                                                .orElse(null);
                                Integer itemPosition = -1;

                                String row_status = "success";

                                if(calQty < 0){
                                    row_status = "successful-warning negative inventory updated as 0";
                                }

                                if(outputRow  !=  null){
                                     
                                     if((itemPosition  = csvOutputData.indexOf(outputRow))>-1){
                                         csvOutputData.remove(itemPosition.intValue());
                                     }
                                     outputRow.put("grn_quantity", newWmsTransaction.getQuantity() + (Integer)outputRow.get("grn_quantity"));
                                     outputRow.put("total_quantity", item.getQuantity());
                                     outputRow.put("status", row_status);
                                }
                                if(outputRow  == null){
                                     outputRow = new HashMap<>();
                                     outputRow.put("sku", item.getSku());
                                     outputRow.put("system_quantity",oldQty);
                                     outputRow.put("grn_quantity", newWmsTransaction.getQuantity());
                                     outputRow.put("total_quantity", item.getQuantity());
                                     outputRow.put("reason", newWmsTransaction.getReason());
                                     outputRow.put("status", row_status);
                                }
                                if(itemPosition != -1){
                                    csvOutputData.add(itemPosition.intValue(),outputRow);
                                }else{
                                    csvOutputData.add(outputRow);
                                }
                            }
                        }
                        cw.writeNext(new String[]{"SKU",    "System Qty",	"Removal Qty",	"New Total",    "Reason",	"Status"});
                        
                        for (int i = 0; i < csvOutputData.size(); i++) {
                            HashMap<String,Object> csvOutputRow = csvOutputData.get(i);
                            cw.writeNext(new String[]{ 
                                 csvOutputRow.get("sku").toString(),
                                 csvOutputRow.get("system_quantity").toString(),
                                 csvOutputRow.get("grn_quantity").toString(),
                                 csvOutputRow.get("total_quantity").toString(),
                                 csvOutputRow.get("reason").toString(),
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

    @KafkaListener(topics="gcp.store.wms.inbound.inventory.query.0", groupId="nano_group_id")
    public void inboundQuerytInventory(String message){
          // Extract JSON body of the messaget
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
                        String[] requiredHeaders = {"sku"};
                        HashMap<String,Integer> headerIndex = new HashMap<>();
    
                        for (int i=0; i<csvInputHeaders.length; i++) {
                            csvInputHeadersLowercase[i] = csvInputHeaders[i].toLowerCase();
                        }
                        Arrays.sort(requiredHeaders);
                        Integer count = 0;
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
                            String  sku    = csvInputRow[ headerIndex.get("sku") ];
                            if(sku.isEmpty()){
                                HashMap<String,Object> outputRow = new HashMap<>();
                                outputRow.put("sku", sku);
                                outputRow.put("quantity","");
                                outputRow.put("bin", "");
                                outputRow.put("status", "fail: sku is empty");
                                csvOutputData.add(outputRow);
                                continue;
                            }

                            Item item = session2.bySimpleNaturalId(Item.class).load(sku);
                            if(item == null){
                                    HashMap<String,Object> outputRow =  new HashMap<>();
                                    outputRow.put("sku", sku);
                                    outputRow.put("quantity","");
                                    outputRow.put("bin", "");
                                    outputRow.put("status", "fail: sku not found");
                                    csvOutputData.add(outputRow);
                                                            
                                
                            }else{
                                    String location   =  item.getInventory_location() == null? "": item.getInventory_location() ;
                                    Integer quantity  =  item.getQuantity() == null? 0: item.getQuantity();
                                    HashMap<String,Object> outputRow = new HashMap<>();
                                    outputRow.put("sku", item.getSku());
                                    outputRow.put("quantity",quantity);;
                                    outputRow.put("bin", location);
                                    outputRow.put("status", "success");
                                    csvOutputData.add(outputRow);
                            }
                        }
                        cw.writeNext(new String[]{"SKU",    "Qty",	"Bin",	"Status"});
                        
                        for (int i = 0; i < csvOutputData.size(); i++) {
                            HashMap<String,Object> csvOutputRow = csvOutputData.get(i);
                            cw.writeNext(new String[]{ 
                                 csvOutputRow.get("sku").toString(),
                                 csvOutputRow.get("quantity").toString(),
                                 csvOutputRow.get("bin").toString(),
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

    @KafkaListener(topics="gcp.store.wms.inbound.inventory.transactions.0", groupId="nano_group_id")
    public void geInboundInventoryTransactions(String message){
          // Extract JSON body of the messaget
          try{

            String body = new JSONObject(message).getString("body");
            JSONObject json = new JSONObject(body);
            BigInteger client_id= BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id= BigInteger.valueOf(json.getInt("log_id"));
            String startDate = json.getString("start_date");
            String endDate = json.getString("end_date");
            Configuration configuration = new Configuration();
            configuration.configure();
            configuration.setProperties(this.SetMasterDB());
            SessionFactory sessionFactory1 = null;
            Session session1 = null;
            sessionFactory1 = configuration.buildSessionFactory();
            session1 = sessionFactory1.openSession();
            Transaction tx1 = session1.beginTransaction();
            String local_folder = new GenerateRandomString(10).toString();
            String local_file =  "report-"+ new GenerateRandomString(12).toString() + ".csv";
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat queryDateFormat = new SimpleDateFormat("yyyy-MM-dd");

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
            FileWriter fw = null;
            CSVWriter  cw = null;

            try {
                JSONArray log_description = new JSONArray(log.getDescription());
                JSONObject log_add = new JSONObject();
                log.setStatus("started");
                log_add.put("key", "Event");
                log_add.put("time", LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                log_add.put("value", "Task Started");
                log_description.put(log_add);
                log.setDescription(log_description.toString());
                log.setUpdated_at(Timestamp.valueOf(LocalDateTime.now()));
                session2.update(log);
                tx2.commit();

                tx3 = session2.beginTransaction();
                
                Storage storage = StorageOptions.newBuilder()
                                         .setProjectId(env.getProperty("spring.gcp.project-id"))
                                         .setCredentials(GoogleCredentials.fromStream(new FileInputStream(env.getProperty("spring.gcp.credentials"))))
                                         .build()
                                         .getService();
                // Create Random directory to store the file
                Files.createDirectories(Paths.get(local_folder));
                String csv_file_report=local_folder + "/" + local_file;
                
                fw = new FileWriter(csv_file_report);
                cw = new CSVWriter(fw);
                cw.writeNext(new String[]{"SKU",	"Added Qty",    "Removed Qty",  "Reason",  "Created At",	"Status"});
               
                Query query = session2.createQuery("From WMSTransaction T where T.created_at BETWEEN :startDate AND :endDate");
                query.setParameter("startDate", queryDateFormat.parse(startDate));
                query.setParameter("endDate", queryDateFormat.parse(endDate));
               
                List<WMSTransaction> transactions = query.list();
                System.out.println(queryDateFormat.parse(startDate));
                System.out.println(queryDateFormat.parse(endDate));
                System.out.println(transactions.size());
                for(int i=0; i<transactions.size(); i++){
                        WMSTransaction singleTransaction = transactions.get(i);
                        Item item = singleTransaction.getItem();
                        String add_qty = singleTransaction.getIs_added() == true ? singleTransaction.getQuantity().toString(): "";
                        String rem_qty = singleTransaction.getIs_added() == false? singleTransaction.getQuantity().toString(): "";
                        if( item != null ){
                            cw.writeNext(new String[]{
                                item.getSku(),
                                add_qty ,
                                rem_qty ,
                                singleTransaction.getReason() ,
                                singleTransaction.getCreated_at().toString(),
                                "success"
                            });
                        }else{
                            cw.writeNext(new String[]{
                                "",
                                "",
                                "",
                                "",
                                "",
                                "Item Missing"
                            });
                        }
                }
                
                cw.close();
                cw = null;
                fw.close();
                fw = null;
                BlobId blobId = BlobId.of(env.getProperty("spring.gcp.bucket-name"), client.getName() + "/" + env.getProperty("spring.gcp.log-folder-name") + "/" + local_folder + "/" + local_file);
                BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                storage.create(blobInfo, Files.readAllBytes(Paths.get(csv_file_report)));
                JSONArray output_files_array = new JSONArray();
                JSONObject output_file_object= new JSONObject();
                output_file_object.put("gcp_file_name",local_file);
                output_file_object.put("gcp_log_folder_name", env.getProperty("spring.gcp.log-folder-name"));
                output_file_object.put("gcp_task_folder_name",local_folder);
                output_files_array.put(output_file_object);
                log.setOutput_files(output_files_array.toString());
                session2.update(log);
                
                JSONObject log_add1 = new JSONObject();
                log_add1.put("key", "Event");
                log_add1.put("time", LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
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
                log_add.put("time", LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1));
                log_add.put("value", "Error: " + e.getMessage());
                log_description.put(log_add);
                log.setDescription(log_description.toString());
                log.setUpdated_at(Timestamp.valueOf(LocalDateTime.now()));
                session2.update(log);
                tx4.commit();

            }finally{
                if(cw != null) cw.close();
                if(fw != null) fw.close();
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
    
    @KafkaListener(topics="gcp.store.wms.inbound.inventory.bin.update.0", groupId="nano_group_id")
    public void updateInventoryBin(String message){
          // Extract JSON body of the messaget
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
                        String[] requiredHeaders = {"sku" , "bin"};
                        HashMap<String,Integer> headerIndex = new HashMap<>();
    
                        for (int i=0; i<csvInputHeaders.length; i++) {
                            csvInputHeadersLowercase[i] = csvInputHeaders[i].toLowerCase();
                        }
                        Arrays.sort(requiredHeaders);
                        Integer count = 0;
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
                            String  sku    =    csvInputRow[ headerIndex.get("sku") ];
                            String  bin    =    csvInputRow[ headerIndex.get("bin") ];
                            if(bin == null){
                                bin = "";
                            }
                            //System.out.println(sku + ":" + bin);

                            if(sku.isEmpty()){
                                HashMap<String,Object> outputRow = new HashMap<>();
                                outputRow.put("sku", sku);
                                outputRow.put("quantity", "");
                                outputRow.put("bin", "");
                                outputRow.put("status", "fail: sku is empty");
                                csvOutputData.add(outputRow);
                                continue;
                            }

                            Item item = session2.bySimpleNaturalId(Item.class).load(sku.trim());
                            // System.out.println(sku);
                            // System.out.println((item == null? item: item.toString()));
                            if(item == null){
                                    HashMap<String,Object> outputRow = new HashMap<>(); 
                                    outputRow.put("sku", sku);
                                    outputRow.put("quantity","");
                                    outputRow.put("bin", "");
                                    outputRow.put("status", "fail: sku not found");
                                    csvOutputData.add(outputRow);                                               
                                
                            }else{
                                item.setInventory_location(bin);
                                session2.update(item);
                                HashMap<String,Object> outputRow = new HashMap<>();
                                outputRow.put("sku", item.getSku());
                                outputRow.put("quantity",item.getQuantity());
                                outputRow.put("bin",item.getInventory_location());
                                outputRow.put("status", "success");
                                csvOutputData.add(outputRow);
                            }
                        }
                        cw.writeNext(new String[]{"SKU",    "Qty",	"Bin",	"Status"});
                        
                        for (int i = 0; i < csvOutputData.size(); i++) {
                            HashMap<String,Object> csvOutputRow = csvOutputData.get(i);
                            cw.writeNext(new String[]{ 
                                 csvOutputRow.get("sku").toString(),
                                 csvOutputRow.get("quantity").toString(),
                                 csvOutputRow.get("bin").toString(),
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
    
}
