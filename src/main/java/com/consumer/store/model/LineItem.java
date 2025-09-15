package com.consumer.store.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.math.BigInteger;
import java.sql.Timestamp;

@Entity // This tells Hibernate to make a table out of this class
@Table(name = "lineitems")
@Getter
@Setter
public class LineItem {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private BigInteger id;
    private Integer quantity;
    private Double unit_price;
    private String order_item_id;
    private Timestamp created_at= Timestamp.valueOf(java.time.LocalDateTime.now());
    private Timestamp updated_at= Timestamp.valueOf(java.time.LocalDateTime.now());
    @OneToOne(cascade = CascadeType.REFRESH, fetch = FetchType.EAGER)
    @JoinColumn(name = "item_id" , referencedColumnName = "id")
    private Item item;
    @Transient
    private Integer csv_row_number;
    @Transient
    private String message="";
    @Transient
    private String actual_csv_row_data;
    @Transient
    private Double total_price;
    @Transient
    private Boolean is_valid=false;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id")
    private Order order;

}
