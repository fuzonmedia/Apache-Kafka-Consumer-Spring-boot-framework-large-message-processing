package com.consumer.store.model;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.math.BigInteger;
import java.sql.Timestamp;

@Entity // This tells Hibernate to make a table out of this class
@Table(name = "order_returns")
@Getter
@Setter
public class Return {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    private Integer quantity;
    private Integer quantity_received;
    private Timestamp initiate_date;
    private Timestamp received_date;
    private Timestamp inventory_adjusted_date;
    private Timestamp created_at= Timestamp.valueOf(java.time.LocalDateTime.now());
    private Timestamp updated_at= Timestamp.valueOf(java.time.LocalDateTime.now());
    private String reason;
    private String notes;
    private String type="return";
    private Byte is_inventory_adjusted=Byte.valueOf("0");
    private Byte is_good_inventory=Byte.valueOf("0");
    private Byte is_received=Byte.valueOf("0");

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
    private Boolean is_adjustment=false;

}
