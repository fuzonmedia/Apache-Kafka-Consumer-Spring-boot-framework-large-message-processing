package com.consumer.store.model;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import javax.sound.sampled.Line;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

@Entity // This tells Hibernate to make a table out of this class
@Table(name = "transactions")
@Getter
@Setter
public class OrderTransaction {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    private Timestamp initiate_date;
    private Timestamp created_at= Timestamp.valueOf(java.time.LocalDateTime.now());
    private Timestamp updated_at= Timestamp.valueOf(java.time.LocalDateTime.now());
    private Byte is_debit=Byte.valueOf("0");
    private String settlement_id;
    private Double sum_total=0.0;
    private Double total_igst=0.0;
    private Double total_cgst=0.0;
    private Double total_sgst=0.0;
    private String service_type;
    private String notes;



    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "order_id" , referencedColumnName = "id")
    private Order order;

    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "lineitem_id" , referencedColumnName = "id")
    private LineItem lineitem;

    @Transient
    private String order_ref;
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
    private Boolean initiate_return=false;
    @Transient
    private Boolean parent_order_present=false;
    @Transient
    private BigInteger parent_order_id;
    @Transient
    private Integer quantity=1;

    @Transient
    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "order_id" , referencedColumnName = "id")
    private Order parent_order;

}
