package com.consumer.store;

import com.consumer.store.model.*;
import com.consumer.store.service.KafkaProducerService;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.opencsv.CSVWriter;

import ch.qos.logback.core.status.Status;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.commons.io.FileUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.*;
import java.time.LocalDateTime;
import com.google.cloud.storage.StorageOptions;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;

@Service
public class OrderController extends StoreConsumer{

    public OrderController(KafkaProducerService kafkaproducer) {
        super(kafkaproducer);
    }

    @KafkaListener( topics = "gcp.store.order.fct.download.0" )
    public void getOrderDownload(String message){
        try{
            String body =  new JSONObject(message).getString("body");
            JSONObject json       = new JSONObject(body);
            BigInteger client_id  = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id     = BigInteger.valueOf(json.getInt("log_id"));
            String start_date     = json.getString("start_date");
            String end_date       = json.getString("end_date");
            String order_status   = json.has("order_status")? json.getString("order_status"): null;
            String marketplace    = json.has("marketplace")? json.getString("marketplace"): null;
            String[] available_status = new String[]{"ordered", "shipped", "completed", "returned", "cancelled", "processed", "unshipped", "unassigned"};
            String[] available_marketplaces = new String[]{"amazon", "flipkart", "store"};

            
            Configuration configuration = new Configuration();
            SessionFactory  sessionFactory1 = null;
            Transaction tx1 =  null;
            Session session1 = null;
            Random random = new Random();

            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat  simpleDateFormatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            String local_file = "OrderData-" + LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter1) + "-" + random.nextLong() + ".csv";
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
                                    .setProjectId(this.getEnv().getProperty("spring.gcp.project-id"))
                                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(this.getEnv().getProperty("spring.gcp.credentials"))))
                                    .build()
                                    .getService();
                    String file_path = local_folder + "/" +local_file;
                    Files.createDirectory(Paths.get(local_folder));
                    FileWriter csv_file   = new FileWriter(file_path);
                    CSVWriter  csv_writer = new CSVWriter(csv_file);

                    // PickList picklist = session2.load(PickList.class, pick_list_id);
                    // if(picklist != null){
                        String hql = "From Order ORD where ORD.updated_at BETWEEN :start_date and :end_date";

                        if((marketplace != null) && Arrays.asList( available_marketplaces ).contains( marketplace.trim() )){
                            hql += " and ORD.sales_channel =  '" + marketplace.toLowerCase() + "'";
                        }
                        if(order_status != null &&  Arrays.asList(available_status).contains( order_status.trim() )){
                            hql += " and ORD.status =  '" + order_status.toLowerCase() + "'";
                        }
                                                
                        Query query = session2.createQuery( hql );
                        // query.setParameter("status", order_status );
                        query.setParameter("start_date", simpleDateFormatter1.parse(start_date + " 00:00:00"));
                        query.setParameter("end_date", simpleDateFormatter1.parse( end_date + " 00:00:00"));

                        List<Order> order_list = query.list();
                        List<Order> filtered_item_list = new ArrayList<Order>();
                        String[] headers = {
                            "order ref", 
                            "invoice number",	
                            "invoice date",	
                            "order date", 
                            "customer phone",	
                            "customer email",	
                            "customer name",	
                            "customer pincode",
                        	"customer address",	
                            "customer state",	
                            "customer gst",	
                            "ship to name",	
                            "ship to address", 
                            "ship to pincode"	,
                            "ship to state",	
                            "ship to phone no",	
                            "item sku",
                            "asin",
                            "hsn_code",
                            "unit price",	
                            "qty",	
                            "lineitem total discount amount",	
                            "sales channel", 
                            "total shipping cost" , 
                            "tracking ref", 
                            "order type",
                            "priority",
                            "dispatch by",
                            "status",
                        };
                        csv_writer.writeNext(headers);
                        List<String[]> data = new ArrayList<String[]>();
                        for(int i=0; i < order_list.size(); i++){
                            Customer customer = order_list.get(i).getCustomer();
                            String[] row = new String[ headers.length ];
                            List<LineItem> lineitems = order_list.get(i).getLineitems();

                            if(lineitems.size() > 0){
                               for (int j = 0; j < lineitems.size(); j++) {
                                   LineItem lineitem = lineitems.get(j);
                                   Item item = lineitem.getItem();
                                   Product product = item.getProduct();
                                   Order order = order_list.get(i);
                                   row[0] = order.getOrder_ref();
                                   row[1] = order.getInvoice_number();
                                   row[2] = "";
                                   row[3] = (order.getOrder_date() != null)? order.getOrder_date().toString(): "";
                                   
                                   if(customer != null){
                                       row[4]   = customer.getPhone();
                                       row[5]   = customer.getEmail();
                                       row[6]   = customer.getName();
                                       row[7]   = customer.getPin_code();
                                       row[8]   = customer.getAddress();
                                       row[9]   = customer.getState();
                                       row[10]  = customer.getGstin();
                                   }else{
                                       row[4]   = "";
                                       row[5]   = "";
                                       row[6]   = "";
                                       row[7]   = "";
                                       row[8]   = "";
                                       row[9]   = "";
                                       row[10]  = "";
                                   }
       
                                   row[11] =  order.getBuyer_name();
                                   row[12] =  order.getBuyer_address();
                                   row[13] =  order.getBuyer_pincode();
                                   row[14] =  order.getBuyer_state();
                                   row[15] =  order.getBuyer_phone();
                                   
                                   row[16] =   item.getSku();
                                   row[17] =   item.getAsin();
                                   row[18] =   product.getHsn_code();
                                   row[19] =  (lineitem.getUnit_price() != null)? Double.toString(lineitem.getUnit_price()): "";
                                   row[20] =  (lineitem.getQuantity()   != null)? lineitem.getQuantity().toString(): null;
                                   row[21] = "";
                                   row[22] = order.getSales_channel();
   
                                   row[23] = (order.getDelivery_amount() != null)? Double.toString(order.getDelivery_amount()): "";
                                   row[24] = order.getShipping();
                                   row[25] = order.getOrder_type();
                                   row[26] = (order.getDispatch_by()!=null)? order.getDispatch_by().toString(): "";
                                   row[27] = (order.getStatus() != null) ? order.getStatus(): "";
                                   row[28] = (order.getPriority()?"true":"false");
                                   data.add( row );
                               }
                            }else{
                                   Order order = order_list.get(i);
                                   row[0]      = order.getOrder_ref();
                                   row[1]      = order.getInvoice_number();
                                   row[2]      = "";
                                   row[3]      = (order.getOrder_date() != null)? order.getOrder_date().toString(): "";

                                   row[4]   = "";
                                   row[5]   = "";
                                   row[6]   = "";
                                   row[7]   = "";
                                   row[8]   = "";
                                   row[9]   = "";
                                   row[10]  = "";
                                   
                                   row[11] =  order.getBuyer_name();
                                   row[12] =  order.getBuyer_address();
                                   row[13] =  order.getBuyer_pincode();
                                   row[14] =  order.getBuyer_state();
                                   row[15] =  order.getBuyer_phone();
                                   row[22] =  order.getShipping();
                                   row[23] =  order.getOrder_type();
                                   row[20] =  order.getSales_channel();
   
                                   row[21] = (order.getDelivery_amount() != null)? Double.toString(order.getDelivery_amount()): "";
                                   row[24] = (order.getDispatch_by()!=null)? order.getDispatch_by().toString(): "";
                                   row[25] = (order.getStatus() != null) ? order.getStatus(): "";
                                   row[26] = (order.getPriority()?"true":"false");
                                   data.add(row);
                            }
                            
                        }
                    csv_writer.writeAll(data);
                    csv_writer.close();
                    csv_file.close();

                    BlobId blobId = BlobId.of(this.getEnv().getProperty("spring.gcp.bucket-name"), client.getName()+"/"+this.getEnv().getProperty("spring.gcp.log-folder-name")+"/"+local_folder+"/"+local_file);
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
