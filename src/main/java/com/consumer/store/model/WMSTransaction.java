package com.consumer.store.model;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import javax.persistence.*;

import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "wms_transactions")
@Setter
@Getter
public class WMSTransaction {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    @OneToOne(cascade = CascadeType.REFRESH, fetch = FetchType.EAGER)
    @JoinColumn(name = "item_id" , referencedColumnName = "id")
    private Item item;
    private Integer quantity;
    private String reason;
    private Timestamp updated_at = Timestamp.valueOf(LocalDateTime.now());
    private Timestamp created_at;
    private Boolean is_added;
    
}
