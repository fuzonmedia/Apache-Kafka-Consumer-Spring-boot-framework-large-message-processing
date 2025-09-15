package com.consumer.store.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.NaturalId;

import javax.persistence.*;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.List;

@Entity // This tells Hibernate to make a table out of this class
@Table(name = "orders")
@Getter
@Setter
public class Order {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    @NaturalId
    private String order_ref;
    private Timestamp order_date;
    private Timestamp created_at= Timestamp.valueOf(java.time.LocalDateTime.now());
    private Timestamp updated_at= Timestamp.valueOf(java.time.LocalDateTime.now());
    private Byte archive=Byte.valueOf("0");
    private String sales_channel;
    private String status;
    private Double total_igst=0.0;
    private Double total_cgst=0.0;
    private Double total_sgst=0.0;
    private Double sum_total=0.0;
    private Double delivery_amount=0.0;
    private String shipping; // tracking information
    private String buyer_phone;
    private String buyer_name;
    private String buyer_address;
    private String buyer_state;
    private String buyer_pincode;
    private String invoice_number;
    private String order_type;
    private Boolean priority=false;
    private Timestamp dispatch_by;
    private Timestamp parent_reconcile_date;
    private Integer marketplace_id=1; // Set as default store as one , amazon = 2 , flipkart =2 == system_id
    private String marketplace_identifier; // amazon id or flipkart id
    private Integer status_position_id=1;
    private Timestamp status_updated_at= Timestamp.valueOf(java.time.LocalDateTime.now());


    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "customer_id" , referencedColumnName = "id")
    private Customer customer;

    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "parent_order_id" , referencedColumnName = "id")
    private Order parent_order;

    @OneToMany(cascade = CascadeType.ALL ,fetch = FetchType.EAGER)
    @JoinColumn(name = "order_id")
    private List<LineItem> lineitems;

    @OneToMany(cascade = CascadeType.ALL ,fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id")
    private List<OrderTrack> order_tracks;

    @Transient
    private Integer csv_row_number;
    @Transient
    private String message="";
    @Transient
    private String actual_csv_row_data;
    @Transient
    private Boolean is_valid=false;
    @Transient
    private Boolean is_success=false;
    @Transient
    private String integration_status=null;
    @Transient
    private String marketplace_status=null;


}
