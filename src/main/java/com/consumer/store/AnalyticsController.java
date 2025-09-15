package com.consumer.store;

import com.consumer.store.helper.GenerateRandomString;
import com.consumer.store.Exceptions.*;
import com.consumer.store.helper.GenerateOrderRef;
import com.consumer.store.model.*;
import com.consumer.store.service.KafkaProducerService;
import com.google.gson.JsonObject;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.protocol.types.Field.Bool;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.io.*;
import java.math.BigInteger;
import java.lang.Math;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.format.DateTimeFormatter;
import java.util.*;
import static java.lang.Thread.sleep;


@Service
public class AnalyticsController extends StoreConsumer{

    private final KafkaProducerService kafkaproducer;

    public AnalyticsController( KafkaProducerService kafkaproducer ) throws Exception {
        super( kafkaproducer );
        this.kafkaproducer = this.getKafkaProducer();
    }

    @KafkaListener(topics="gcp.store.analytics.process.0", groupId="nano_group_id")
    public void processClientAnalytics(String message) {

        Transaction tx1 = null, tx2 = null, tx3 = null, tx4 = null, tx5 = null;
        Session session1 = null, session2 = null, session3 = null;
        SessionFactory sessionFactory1 = null, sessionFactory2 = null, sessionFactory3 = null;

        try {
            String body = new JSONObject(message).getString("body");
            JSONObject json = new JSONObject(body);
            BigInteger client_id = BigInteger.valueOf(json.getInt("client_id"));
            BigInteger log_id = BigInteger.valueOf(json.getInt("log_id"));

            Configuration configuration = new Configuration();
            configuration.configure();
            configuration.setProperties(this.SetMasterDB());
            sessionFactory1 = configuration.buildSessionFactory();
            session1 = sessionFactory1.openSession();
            Boolean isClientOk = false, isLogOk = false;
            tx1 = session1.beginTransaction();
            Client client = session1.load(Client.class, client_id);
            if( client != null ) {
                isClientOk = true;
            }
            tx1.commit();
            tx1 = null;

            if( !isClientOk) {
                throw new Exception("Invalid Client");
            }

            try {
                configuration.setProperties(this.SetClientDB(client.getDb_host(), client.getDb_port(), client.getDb_name(), client.getDb_user(), client.getDb_pass()));
                sessionFactory2 = configuration.buildSessionFactory();
                session2 = sessionFactory2.openSession();

                tx2 = session2.beginTransaction();

                Log log = session2.load(Log.class, log_id);
                if( log != null ) {
                    isLogOk = true;
                }
                log.setStatus("started");
                JSONArray log_description1 = new JSONArray(log.getDescription());
                JSONObject log_add1 = new JSONObject();
                log_add1.put("key","event");
                log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                log_add1.put("value","Task Started");    
                log_description1.put( log_add1 );
                log.setDescription(log_description1.toString());
                log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                session2.persist(log);
                tx2.commit();
                tx2 = null;

                if( !isLogOk ) {
                    throw new Exception("Exception");
                }
                this.LogMessageInfo(this.getClass().getName() + ": analytics process job running.");
                this.LogMessageInfo("log: " + log.getId() + " client: " + client.getId());

                tx3 = session2.beginTransaction();

                Query item_query = session2.createQuery("From Item");
                List<Item> item_list = item_query.list();
                int total_product = item_list.size();
                List<HashMap<String,Object>> params = new ArrayList<HashMap<String,Object>>();

                List<Object[]> average_sales_period = new ArrayList<Object[]>();
                    LocalDateTime $07_avg_sales_days = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusDays(7).withHour(00).withMinute(00).withSecond(00);
                    LocalDateTime $15_avg_sales_days = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusDays(15).withHour(00).withMinute(00).withSecond(00);
                    LocalDateTime $30_avg_sales_days = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusDays(30).withHour(00).withMinute(00).withSecond(00);
                    LocalDateTime $60_avg_sales_days = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusDays(60).withHour(00).withMinute(00).withSecond(00);
                    LocalDateTime $90_avg_sales_days = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusDays(90).withHour(00).withMinute(00).withSecond(00);

                    average_sales_period.add(new Object[]{"07_days", $07_avg_sales_days});
                    average_sales_period.add(new Object[]{"15_days", $15_avg_sales_days});
                    average_sales_period.add(new Object[]{"30_days", $30_avg_sales_days});
                    average_sales_period.add(new Object[]{"60_days", $60_avg_sales_days});
                    average_sales_period.add(new Object[]{"90_days", $90_avg_sales_days});



                List<Object[]> forecast_date_range = new ArrayList<Object[]>();
                    // LocalDateTime $00_days = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).plusDays(1);
                    // LocalDateTime $01_days = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).plusDays(2);
                    LocalDateTime $07_forecast_days = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).plusDays(7);
                    LocalDateTime $15_forecast_days = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).plusDays(15);

                    // forecast_date_range.add(new Object[]{"00_days", $00_days});
                    // forecast_date_range.add(new Object[]{"01_days", $01_days});
                    forecast_date_range.add(new Object[]{"07_days", $07_forecast_days});
                    forecast_date_range.add(new Object[]{"15_days", $15_forecast_days});

                List<Object[]> date_range = new ArrayList<Object[]>();
                    LocalDateTime $00_days_ago = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).withHour(00).withMinute(00).withSecond(00);
                    LocalDateTime $01_days_ago = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusDays(1).withHour(00).withMinute(00).withSecond(00);
                    LocalDateTime $15_days_ago = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusDays(15).withHour(00).withMinute(00).withSecond(00);
                    LocalDateTime $30_days_ago = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusDays(30).withHour(00).withMinute(00).withSecond(00);
                    LocalDateTime $45_days_ago = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusDays(45).withHour(00).withMinute(00).withSecond(00);
                    LocalDateTime $60_days_ago = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusDays(60).withHour(00).withMinute(00).withSecond(00);
                    LocalDateTime $90_days_ago = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).minusDays(90).withHour(00).withMinute(00).withSecond(00);

                    date_range.add(new Object[]{"00_days_ago", $00_days_ago});
                    date_range.add(new Object[]{"01_days_ago", $01_days_ago});
                    date_range.add(new Object[]{"15_days_ago", $15_days_ago});
                    date_range.add(new Object[]{"30_days_ago", $30_days_ago});
                    date_range.add(new Object[]{"45_days_ago", $45_days_ago});
                    date_range.add(new Object[]{"60_days_ago", $60_days_ago});
                    date_range.add(new Object[]{"90_days_ago", $90_days_ago});

                List<String> marketplaces = new ArrayList<>();
                    marketplaces.add("all");
                    marketplaces.add("flipkart");
                    marketplaces.add("amazon");
                    marketplaces.add("store");

                List<String> states = new ArrayList<>();
                    states.add("all");
                    states.add("andhra pradesh");
                    states.add("arunachal pradesh");
                    states.add("assam");
                    states.add("bihar");
                    states.add("chattisgarh");
                    states.add("goa");
                    states.add("gujarat");
                    states.add("haryana");
                    states.add("himachal pradesh");
                    states.add("jharkhand");
                    states.add("karnataka");
                    states.add("kerala");
                    states.add("madhya pradesh");
                    states.add("maharashtra");
                    states.add("manipur");
                    states.add("meghalaya");
                    states.add("mizoram");
                    states.add("nagaland");
                    states.add("odisha");
                    states.add("punjab");
                    states.add("rajasthan");
                    states.add("sikkim");
                    states.add("tamil nadu");
                    states.add("telengana");
                    states.add("tripura");
                    states.add("uttarakhand");
                    states.add("uttar pradesh");
                    states.add("west bengal");
                    states.add("andaman and nicobar islands");
                    states.add("chandigarh");
                    states.add("dadra and nagar haveli and daman & diu");
                    states.add("new delhi");
                    states.add("jammu & kashmir");
                    states.add("ladakh");
                    states.add("lakshadweep");
                    states.add("puducherry");

                // Save Inventory ReOrder forecast data
                System.out.println("Started Inventory reorder");
                for (String market : marketplaces) {
                    for (Object[] date : forecast_date_range) {
                        for (Object[] sales_date : average_sales_period) {
                            LocalDateTime query_sales_date = (LocalDateTime)sales_date[1];
                            String query_sales_date_name  = sales_date[0].toString();
                            Integer query_sales_days = 0;
                            LocalDateTime query_date = (LocalDateTime)date[1];
                            String query_date_name  = date[0].toString();
                            switch (query_sales_date_name) {
                                case ("07_days"):
                                    query_sales_days  = 7;
                                    break;

                                case ("15_days"):
                                    query_sales_days  = 15;
                                    break;

                                case ("30_days"):
                                    query_sales_days  = 30;
                                    break;

                                case ("60_days"):
                                    query_sales_days  = 60;
                                    break;

                                case ("90_days"):
                                    query_sales_days  = 90;
                                    break;

                                default:
                                    break;
                            }
                            String hql = this.getInventoryReorderSQLString( query_sales_date, query_sales_days, market, null);
                            Query query = session2.createSQLQuery( hql );
                            List<Object[]> results = query.list();

                            Analytic analytic = null;

                            String data_key1 = market + "_reorder_within_" + query_date_name + "_avg_sales_period_" + query_sales_date_name;
                            
                            analytic = session2.bySimpleNaturalId(Analytic.class)
                            .load(data_key1);
                            if(analytic == null) {
                                analytic = new Analytic();
                                analytic.setData_name( data_key1 );
                            }
                            if(query_date_name=="07_days") {
                                analytic.setData_value(this.getWeeklyInventoryReorder(results, query_date).toString());
                            } else {
                                analytic.setData_value(this.getFortnightlyInventoryReorder(results, query_date).toString());
                            }
                            session2.persist( analytic );
                        }
                    }
                }

                // Save Average sales & order, Total Inventory Quantity & Total Inventory Value figure
                System.out.println("<<<<< Started Basic Analytics calculation >>>>>");
                for (String market : marketplaces) {
                    String hql1 = this.getAvgSQLString( market );
                    String hql2 = this.getTotalSQLString( market );
                    String hql3 = this.getSellThroughSQLString( market );
                    Query query1 = session2.createSQLQuery( hql1 );
                    Query query2 = session2.createSQLQuery( hql2);
                    Query query3 = session2.createSQLQuery( hql3);
                    List<Object[]> results1 = query1.list();
                    List<Object[]> results2 = query2.list();
                    List<Object[]> results3 = query3.list();

                    Analytic analytic = null;

                    String data_key1 = market + "_" + "basic_analytics";

                    analytic = session2.bySimpleNaturalId(Analytic.class)
                    .load(data_key1);
                    if(analytic == null) {
                        analytic = new Analytic();
                        analytic.setData_name( data_key1 );
                    }
                    analytic.setData_value(this.getAvgSales_Order( results1,results2,results3 ).toString());
                    session2.persist( analytic );
                }

                // Save Daily Inventory & Transaction data
                System.out.println("<<<<< Started Daily Inventory & Transaction Book-keeping >>>>>");
                for (String market : marketplaces) {
                    String hql = this.getDailyDataSQLString( market );
                    System.out.println("<<<<< Query: "+hql.toString()+">>>>>");
                    Query query = session2.createSQLQuery( hql);
                    List<Object[]> results = query.list();
                    System.out.println("<<<<< Results: "+results.toString()+">>>>>");
                    LocalDateTime now = LocalDateTime.now(ZoneId.of("Asia/Kolkata"));
                    LocalDate currentDate = now.toLocalDate();
                    Bookkeeping bookkeeping = null;

                    String data_key1 = market + "_" + currentDate;
                    System.out.println("<<<<< Data Key: "+data_key1+">>>>>");
                    try{
                        bookkeeping = session2.bySimpleNaturalId(Bookkeeping.class)
                        .load(data_key1);
                        if(bookkeeping == null) {
                            bookkeeping = new Bookkeeping();
                            bookkeeping.setData_name( data_key1 );
                        }
                        bookkeeping.setData_value(this.getDailyData(results).toString());
                        session2.persist( bookkeeping );
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("Error: " + e.getMessage());
                    }
                }

                // Save State wise sales figure
                System.out.println("<<<<<< Started State wise sales >>>>>>");
                for (String market : marketplaces) {
                    for (Object[] date : date_range) {
                        for (String state_name : states) {
                            LocalDateTime query_date = (LocalDateTime)date[1];
                            String query_date_name  = date[0].toString();
                            String hql = this.getState_wiseSalesSQLString( query_date, state_name, market, null);
                            Query query = session2.createSQLQuery( hql );
                            List<Object[]> results = query.list();
                            Analytic analytic = null;

                            String data_key1 = market + "_" + query_date_name + "_" + state_name +"_wise_sales";

                            analytic = session2.bySimpleNaturalId(Analytic.class)
                            .load(data_key1);
                            if(analytic == null) {
                                analytic = new Analytic();
                                analytic.setData_name( data_key1 );
                            }
                            analytic.setData_value(this.getState_wiseSales(results).toString());
                            session2.persist( analytic );
                        }
                    }
                }

                // Save Top/Medium/Low sales figure
                System.out.println("Started Performance wise sales");
                for (String market : marketplaces) {
                    for (Object[] date : date_range) {
                        LocalDateTime query_date = (LocalDateTime)date[1];
                        String query_date_name  = date[0].toString();
                        String hql = this.getSalesSQLString( query_date, market, null);
                        Query query = session2.createSQLQuery( hql );
                        List<Object[]> results = query.list();
                        Analytic analytic1 = null;
                        Analytic analytic2 = null;
                        Analytic analytic3 = null;

                        String data_key1 = market + "_" + query_date_name + "_" + "top_sales";
                        String data_key2 = market + "_" + query_date_name + "_" + "medium_sales";
                        String data_key3 = market + "_" + query_date_name + "_" + "low_sales";

                        analytic1 = session2.bySimpleNaturalId(Analytic.class)
                        .load(data_key1);
                        if(analytic1 == null) {
                            analytic1 = new Analytic();
                            analytic1.setData_name( data_key1 );
                        }
                        analytic1.setData_value(this.getTopSales(results).toString());
                        session2.persist( analytic1 );

                        analytic2 = session2.bySimpleNaturalId(Analytic.class)
                        .load( data_key2 );
                        if(analytic2 == null) {
                            analytic2 = new Analytic();
                            analytic2.setData_name( data_key2 );
                        }
                        analytic2.setData_value(this.getMediumSales(results).toString());
                        session2.persist( analytic2 );

                        analytic3 = session2.bySimpleNaturalId(Analytic.class)
                        .load( data_key3 );
                        if(analytic3 == null){
                            analytic3 = new Analytic();
                            analytic3.setData_name( data_key3 );
                        }
                        analytic3.setData_value(this.getLowSales(results).toString());
                        session2.persist(analytic3);
                    }
                }

                // Save Worst Product sales figure
                System.out.println("Started Worst sales");
                for (String market : marketplaces) {
                    for (Object[] date : date_range) {
                        LocalDateTime query_date = (LocalDateTime)date[1];
                        String query_date_name  = date[0].toString();
                        String hql  = this.getWorstPerformingSqlString( query_date, market);
                        Query query = session2.createSQLQuery( hql );
                        List<Object[]> results = query.list();
                        Analytic analytic1 = null, analytic2 = null;
                        String data_key1 = market + "_" + query_date_name + "_worst_products";
                        analytic1 = session2.bySimpleNaturalId(Analytic.class)
                        .load( data_key1 );
                        if(analytic1 == null) {
                            analytic1 = new Analytic();
                            analytic1.setData_name( data_key1 );
                        }
                        analytic1.setData_value(this.addCutOffPercentage(results).toString());
                        session2.persist( analytic1 );
                    }
                }

                // Save Cancelled sales figure
                System.out.println("Started Cancelled sales");
                for (String market : marketplaces) {
                    for (Object[] date : date_range) {
                        LocalDateTime query_date = (LocalDateTime)date[1];
                        String query_date_name  = date[0].toString();
                        String hql  = this.getHighestCancelProductsSql(query_date, market);
                        Query query = session2.createSQLQuery( hql );
                        List<Object[]> results = query.list();
                        JSONArray data_json = new JSONArray();
                        for (int k = 0; k < results.size(); k++) {     
                            Object[] object = results.get(k);
                            data_json.put (
                                new JSONObject()
                                .put("id", new BigInteger(object[0].toString()))
                                .put("sku", object[1].toString())
                                .put("cancel_count", object[2])
                                .put("sales", object[3])
                            );
                        }
                        Analytic analytic = null;
                        String data_key = market + "_" + query_date_name + "_cancelled_products";
                        analytic = session2.bySimpleNaturalId(Analytic.class)
                        .load( data_key );
                        if(analytic == null) {
                            analytic = new Analytic();
                            analytic.setData_name( data_key );
                        }
                        analytic.setData_value(data_json.toString());
                        session2.persist( analytic );
                    }
                }

                // Save Sales Return figure
                System.out.println("Started Sales return");
                for (String market : marketplaces) {
                    for (Object[] date : date_range) {
                        LocalDateTime query_date = (LocalDateTime)date[1];
                        String query_date_name  = date[0].toString();
                        String hql  = this.getHighestReturnProductsSql(query_date, market);
                        Query query = session2.createSQLQuery( hql );
                        List<Object[]> results = query.list();
                        JSONArray data_json = new JSONArray();
                        for (int k = 0; k < results.size(); k++) {     
                            Object[] object = results.get(k);
                            data_json.put( 
                                new JSONObject()
                                .put("id", new BigInteger(object[0].toString()))
                                .put("sku", object[1].toString())
                                .put("return_count", object[2])
                                .put("sales", object[3])
                            );
                        }
                        Analytic analytic = null;
                        String data_key = market + "_" + query_date_name + "_returned_products";
                        analytic = session2.bySimpleNaturalId(Analytic.class)
                        .load( data_key );
                        if(analytic == null) {
                            analytic = new Analytic();
                            analytic.setData_name( data_key );
                        }
                        analytic.setData_value(data_json.toString());
                        session2.persist( analytic );
                    }
                }
                System.out.println("<<< Completed Analytics >>>");
                log.setStatus("completed");
                JSONArray log_description2 = new JSONArray(log.getDescription());
                JSONObject log_add2 = new JSONObject();
                log_add2.put("key","event");
                log_add2.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                log_add2.put("value","completed");    
                log_description1.put( log_add1 );
                log.setDescription(log_description2.toString());
                log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                session2.persist(log);
                tx3.commit();
                tx3= null;
                this.LogMessageInfo(this.getClass().getName() + " : " + "analytics job finished");
                this.LogMessageInfo("client: " + client.getId() + " log: " + log.getId());
            } catch (Exception e) {
                e.printStackTrace();
                if(tx1 != null) {
                    tx1.rollback();
                }
                if(tx2 != null) {
                    tx2.rollback();
                }
                if(session2 != null) {
                    tx4 = session2.beginTransaction();
                    Log log = session2.load(Log.class, log_id);
                    log.setStatus("failed");
                    JSONArray log_description1 = new JSONArray(log.getDescription());
                    JSONObject log_add1 = new JSONObject();
                    log_add1.put("key","event");
                    log_add1.put("time", java.time.LocalDateTime.now(ZoneId.of("Asia/Kolkata")).toString());
                    log_add1.put("value","Task Started");    
                    log_description1.put( log_add1 );
                    log.setDescription(log_description1.toString());
                    log.setUpdated_at(Timestamp.valueOf(java.time.LocalDateTime.now()));
                    session2.update(log);
                    tx4.commit();
                }
            } finally {
                if(session1 != null) {
                    session1.close();
                }
                if(session2 != null) {
                    session2.close();
                }
                if(sessionFactory1 != null ) {
                    sessionFactory1.close();
                }
                if(sessionFactory2 != null) {
                    sessionFactory2.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error: " + e.getMessage());
        }
    }


    /////////////////////////User Defined Functions///////////////////////////////////

    ///////////////////// DATABASE QUERIES ///////////////
    
    // Query to  get Items to be Reordered within 7/15 days
    public String getInventoryReorderSQLString( LocalDateTime salesperiod, Integer avgdays, String marketplace, BigInteger limit) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now( ZoneId.of("Asia/Kolkata")).withHour(23).withMinute(59).withSecond(59);
        String sql = "SELECT  I.id, I.sku, I.quantity/(SUM(LI.unit_price*LI.quantity)/"+avgdays+") as order_within " +
            "FROM lineitems as LI " +
            "INNER JOIN orders as OD ON LI.order_id = OD.id " +
            "INNER JOIN items as I ON I.id = LI.item_id " +
            "WHERE OD.dispatch_by BETWEEN \"" + salesperiod.format(formatter) +
            "\" AND \"" + now.format(formatter) +"\" " +
            "AND OD.status IN ('shipped', 'completed')";

        if( !marketplace.equalsIgnoreCase("all")) {
            sql += "AND OD.sales_channel = \"" + marketplace +"\" ";
        }

        sql += "GROUP BY I.sku ORDER BY I.quantity DESC";

        if(limit != null) {
            sql += " LIMIT " + limit.toString();
        }
        return sql;
    }

    // Query to  get Total Inventory Value & Total Inventory Quantity for a Seller
    public String getTotalSQLString(String marketplace) {
        String sql = "SELECT SUM(ITM.quantity) As total_inv_qty, SUM(ITM.price*ITM.quantity)" +
            "As total_inv_value FROM items AS ITM";
        return sql;
    }

    // Query to  get Units sold in last 30 days for a Seller
    public String getSellThroughSQLString(String marketplace) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime start = LocalDateTime.now( ZoneId.of("Asia/Kolkata")).minusDays(30).withHour(00).withMinute(00).withSecond(00);
        LocalDateTime end = LocalDateTime.now( ZoneId.of("Asia/Kolkata")).withHour(23).withMinute(59).withSecond(59);

        String sql = "SELECT SUM(LI.quantity) AS total_sales, SUM(LI.unit_price*LI.quantity) AS total_sales_value " +
        "FROM orders AS OD " +
        "INNER JOIN lineitems AS LI ON OD.id = LI.order_id " +
        "WHERE OD.dispatch_by BETWEEN \"" + start.format(formatter) +
        "\" AND \"" + end.format(formatter) +"\" ";
        return sql;
    }    

    // Query to  get daily pending order for last 6 weeks
    public String getDailyDataSQLString(String marketplace) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime start = LocalDateTime.now( ZoneId.of("Asia/Kolkata")).minusDays(1).withHour(00).withMinute(00).withSecond(00);
        LocalDateTime end = LocalDateTime.now( ZoneId.of("Asia/Kolkata")).minusDays(1).withHour(23).withMinute(59).withSecond(59);

        String sql = "SELECT DATE(OD.dispatch_by) AS record_date, " +
            "SUM(CASE WHEN (OD.status <> 'cancelled') THEN (LI.unit_price * LI.quantity) ELSE 0 END) AS total_sales, " +
	        "SUM(CASE WHEN (OD.status = 'shipped') THEN LI.quantity ELSE 0 END) AS shipped_orders, " +
            "SUM(CASE WHEN (OD.status <> 'shipped' OR OD.status <> 'cancelled') THEN LI.quantity ELSE 0 END) AS pending_orders " +
            "FROM orders AS OD " +
            "INNER JOIN lineitems AS LI ON OD.id = LI.order_id " +
            "WHERE OD.dispatch_by BETWEEN \"" + start.format(formatter) +
            "\" AND \"" + end.format(formatter) +"\" ";

            if( !marketplace.equalsIgnoreCase("all")) {
                sql += "AND OD.sales_channel = \"" + marketplace +"\" ";
            }

            sql += "GROUP BY record_date";

        return sql;
    }


    // Query to  get Average Order & Sales for last 6 weeks
    public String getAvgSQLString(String marketplace) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now( ZoneId.of("Asia/Kolkata"));
        LocalDateTime _6_weeks_ago = LocalDateTime.now( ZoneId.of("Asia/Kolkata")).minusWeeks(6L);
        String sql = "SELECT  SUM(LI.unit_price*LI.quantity)/45 as avg_daily_sales, " +
        "(SELECT  SUM(LI.unit_price*LI.quantity)/6 " +
        "FROM orders as OD " +
        "INNER JOIN lineitems as LI ON LI.order_id = OD.id " +
        "WHERE OD.order_date BETWEEN \"" + _6_weeks_ago.format(formatter) +
        "\" AND \"" + now.format(formatter) +"\" ";

        if( !marketplace.equalsIgnoreCase("all")) {
            sql += "AND OD.sales_channel = \"" + marketplace +"\" ";
        }        

        sql += "AND OD.status NOT IN ('cancelled')) as avg_ord_val " +
        "FROM orders as OD " + 
        "INNER JOIN lineitems as LI ON LI.order_id = OD.id " +
        "WHERE OD.dispatch_by BETWEEN \"" + _6_weeks_ago.format(formatter) +
        "\" AND \"" + now.format(formatter) +"\" " +
        "AND OD.status IN ('shipped', 'completed')";

        if( !marketplace.equalsIgnoreCase("all")) {
            sql += "AND OD.sales_channel = \"" + marketplace +"\" ";
        }

        return sql;
    }
    
    public String getState_wiseSalesSQLString(LocalDateTime date, String state_name, String marketplace, BigInteger limit) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now( ZoneId.of("Asia/Kolkata")).withHour(23).withMinute(59).withSecond(59);
        String sql = "SELECT  I.id, I.sku as sku, SUM(LI.unit_price*LI.quantity) as sale_amount,SUM(LI.quantity) as qty_sold, " +
            "(SELECT SUM(LI.unit_price*LI.quantity) as sale_amount " +
                "FROM lineitems as LI " +
                "INNER JOIN orders as OD ON LI.order_id = OD.id " +
                "INNER JOIN items as I ON I.id = LI.item_id " +
                "WHERE OD.dispatch_by BETWEEN \"" + date.format(formatter) +
                "\" AND \"" + now.format(formatter) +"\"" +
                "AND OD.status NOT IN ('cancelled')) as total_sales " +
            "FROM lineitems as LI " +
            "INNER JOIN orders as OD ON LI.order_id = OD.id " +
            "INNER JOIN items as I ON I.id = LI.item_id " +
            "WHERE OD.dispatch_by BETWEEN \"" + date.format(formatter) +
            "\" AND \"" + now.format(formatter) +"\" " +
            "AND OD.status NOT IN ('cancelled') ";

        if( !marketplace.equalsIgnoreCase("all")) {
            sql += "AND OD.sales_channel = \"" + marketplace +"\" ";
        }
        if ( !state_name.equalsIgnoreCase("all")) {
            sql += "AND LOWER(OD.buyer_state) = \"" + state_name +"\" ";
        }

        sql += "GROUP BY I.sku ORDER BY sale_amount DESC";
        if(limit != null) {
            sql += " LIMIT " + limit.toString();
        }
        return sql;
    }

    public String getSalesSQLString(LocalDateTime date, String marketplace, BigInteger limit) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now( ZoneId.of("Asia/Kolkata")).withHour(23).withMinute(59).withSecond(59);
        String sql = "SELECT  I.id, I.sku as sku, SUM(LI.unit_price*LI.quantity) as sale_amount " +
            "FROM lineitems as LI " +
            "INNER JOIN orders as OD ON LI.order_id = OD.id " +
            "INNER JOIN items as I ON I.id = LI.item_id " +
            "WHERE OD.dispatch_by BETWEEN \"" + date.format(formatter) +
            "\" AND \"" + now.format(formatter) +"\" " + 
            "AND OD.status NOT IN ('cancelled') ";

        if( !marketplace.equalsIgnoreCase("all")) {
            sql += "AND OD.sales_channel = \"" + marketplace +"\" ";
        }

        sql += "GROUP BY I.sku ORDER BY sale_amount DESC";
        if(limit != null) {
            sql += " LIMIT " + limit.toString();
        }
        return sql;
    }

    public String getHighestCancelProductsSql( LocalDateTime date, String marketplace ) {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now( ZoneId.of("Asia/Kolkata")).withHour(23).withMinute(59).withSecond(59);

        String sql  = "SELECT I.id, I.sku as sku, COUNT(*) as cancel_count, SUM(LI.unit_price) as sales_amount " +
            "FROM lineitems as LI " +
            "INNER JOIN items AS I ON LI.item_id = I.id " +
            "INNER JOIN orders AS OD ON OD.id = LI.order_id " +
            "WHERE OD.`status` = \"cancelled\" " +
            "AND OD.dispatch_by BETWEEN \"" + date.format(formatter) +
            "\" AND \"" + now.format(formatter) +"\" ";

        if( !marketplace.equalsIgnoreCase("all") ) {
            sql += "AND OD.sales_channel = \"" + marketplace +"\" ";
        }
        sql += "GROUP BY I.sku, OD.`status` " + "ORDER BY cancel_count DESC ";
        return sql ;
    }

    public String getHighestReturnProductsSql( LocalDateTime date, String marketplace ) {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now( ZoneId.of("Asia/Kolkata")).withHour(23).withMinute(59).withSecond(59);

        String sql  = "SELECT I.id, I.sku as sku, COUNT(*) as return_count, SUM(LI.unit_price) as sales_amount " +
            "FROM lineitems as LI " +
            "INNER JOIN items AS I ON LI.item_id = I.id " +
            "INNER JOIN orders AS OD ON OD.id = LI.order_id " +
            "WHERE OD.`status` = \"returned\" " +
            "AND OD.dispatch_by BETWEEN \"" + date.format(formatter) +
            "\" AND \"" + now.format(formatter) +"\" ";

        if( !marketplace.equalsIgnoreCase("all") ) {
            sql += "AND OD.sales_channel = \"" + marketplace +"\" ";
        }
        sql += "GROUP BY I.sku, OD.`status` " + "ORDER BY return_count DESC ";
        return sql ;
    }   

    public String getWorstPerformingSqlString(LocalDateTime date, String marketplace) {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now( ZoneId.of("Asia/Kolkata")).withHour(23).withMinute(59).withSecond(59);

        String sql = "SELECT id, sku FROM items where sku not in " +
            "(SELECT I.sku " +
            "FROM lineitems as LI " +
                "INNER JOIN orders as OD ON OD.id = LI.order_id " +
                "INNER JOIN items as I ON LI.item_id = I.id " +
                "WHERE OD.dispatch_by BETWEEN \"" + date.format(formatter) +
                "\" AND \""+ now.format(formatter) + "\" " +
                "GROUP BY I.sku )";
        return sql;
    }

    ///////////////////// DATA PARSING ///////////////

    public JSONArray addCutOffPercentage(List<Object[]> products) throws JSONException {

        JSONArray results = new JSONArray();
        int len = products.size();

        for (int i = 0; i < len; i++) {              
            Object[] object = products.get(i) ;
            results.put( new JSONObject()
                .put("id", object[0].toString())
                .put("sku",  object[1].toString())
                .put("sales",0 )
            );
        }
        return results;
    }

    public JSONArray getWeeklyInventoryReorder( List<Object[]> products, LocalDateTime date) throws JSONException {
        JSONArray results = new JSONArray();
        int len =  products.size();
        int start = 0;
        int end = len;

        for (int i = start; i < end; i++) {              
            Object[] object = products.get(i) ;
            Double mOrderWitihin = object[2]!=null?Double.valueOf(object[2].toString()):9999.99;
            if (mOrderWitihin <= 7.00) {
                results.put( new JSONObject()
                    .put("id", object[0]!= null?object[0].toString():"")
                    .put("sku",  object[1]!= null?object[1].toString():"")
                    .put("reord_date", date.toString())
                );
            }
        }
        return results;
    }

    public JSONArray getFortnightlyInventoryReorder( List<Object[]> products, LocalDateTime date) throws JSONException {
        JSONArray results = new JSONArray();
        int len =  products.size();
        int start = 0;
        int end = len;

        for (int i = start; i < end; i++) {              
            Object[] object = products.get(i) ;
            Double mOrderWithin = object[2]!=null?Double.valueOf(object[2].toString()):9999.99;
            if (mOrderWithin > 7.00 && mOrderWithin <= 15.00) {
                results.put( new JSONObject()
                    .put("id", object[0]!= null?object[0].toString():"")
                    .put("sku",  object[1]!= null?object[1].toString():"")
                    .put("reord_date", date.toString())
                );
            }
        }
        return results;
    }

    public JSONArray getAvgSales_Order( List<Object[]> products1, List<Object[]> products2, List<Object[]> products3 ) throws JSONException {
        JSONArray results = new JSONArray();
        Object[] object1 = products1.get(0) ;
        Object[] object2 = products2.get(0) ;
        Object[] object3 = products3.get(0) ;
        results.put( new JSONObject()
            .put("avg_daily_sales", object1[0]!= null?object1[0].toString():"0")
            .put("avg_ord_val",  object1[1]!= null?object1[1].toString():"0")
            .put("total_inv_qty",  object2[0]!= null?object2[0].toString():"0")
            .put("total_inv_val",  object2[1]!= null?object2[1].toString():"0")
            .put("sold_30", object3[0]!= null ? object3[0].toString()!= "0" ? Double.parseDouble(object3[0].toString()) : "0" : "0")
        );        
        return results;
    }

    public JSONArray getDailyData( List<Object[]> values) throws JSONException {
        JSONArray results = new JSONArray();
        Object[] object = values.get(0);
        results.put( new JSONObject()
            .put("total_sales", object[1]!= null?object[1].toString():"0")
            .put("shipped_orders",  object[2]!= null?object[2].toString():"0")
            .put("pending_orders",  object[3]!= null?object[3].toString():"0")
        );
        return results;
    }

    public JSONArray getState_wiseSales( List<Object[]> products) throws JSONException {

        JSONArray results = new JSONArray();
        int len =  products.size();
        int start = 0;
        int end = len;

        for (int i = start; i < end; i++) {              
            Object[] object = products.get(i) ;
            results.put( new JSONObject()
                .put("id", object[0].toString())
                .put("sku",  object[1].toString())
                .put("sales", Double.parseDouble(object[2].toString()))
                .put("qty_sold",  object[3].toString())
                .put("percent_sales", Math.round(((Double.parseDouble(object[2].toString())/Double.parseDouble(object[4].toString()))*100.0)*100.0)/100.0)
            );
        }
        return results;
    }

    public JSONArray getTopSales( List<Object[]> products) throws JSONException {

        JSONArray results = new JSONArray();
        int len =  ( products.size() * 20 ) / 100;

        int a =  ( products.size() * 20 ) / 100;
        int b =  ( products.size() * 30 ) / 100;
        int c =  ( products.size() * 50 ) / 100;
        int start = 0;
        int end = a;

        for (int i = start; i < end; i++) {              
            Object[] object = products.get(i) ;
            results.put( new JSONObject()
                .put("id", object[0].toString())
                .put("sku",  object[1].toString())
                .put("sales", Double.parseDouble(object[2].toString()))
            );
        }
        return results;
    }

    public JSONArray getMediumSales( List<Object[]> products) throws JSONException {

        JSONArray results = new JSONArray();
        int len =  ( products.size() * 30 ) / 100;;

        int a =  ( products.size() * 20 ) / 100;
        int b =  ( products.size() * 30 ) / 100;
        int c =  ( products.size() * 50 ) / 100;
        int start = a;
        int end = b;

        for (int i = start; i < end; i++) {              
            Object[] object = products.get(i) ;
            results.put( new JSONObject()
                .put("id", object[0].toString())
                .put("sku",  object[1].toString())
                .put("sales", Double.parseDouble(object[2].toString()))
            );
        }
        return results;
    }

    public JSONArray getLowSales( List<Object[]> products) throws JSONException {

        JSONArray results = new JSONArray();
        int a =  ( products.size() * 20 ) / 100;
        int b =  ( products.size() * 30 ) / 100;
        int c =  ( products.size() * 50 ) / 100;
        int start = a + b;
        int end = a + b + c;

        for (int i = start; i < end; i++) {              
            Object[] object = products.get(i) ;
                results.put( new JSONObject()
                .put("id", object[0].toString())
                .put("sku",  object[1].toString())
                .put("sales", Double.parseDouble(object[2].toString()))
            );
        }
        return results;
    }
}
