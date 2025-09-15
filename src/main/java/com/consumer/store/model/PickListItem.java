package com.consumer.store.model;

import lombok.Getter;
import lombok.Setter;
import java.math.BigInteger;
import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name = "picklist_items")
@Getter
@Setter
public class PickListItem{

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    private String sku;
    private BigInteger quantity;
    private String bin;
    private String status;
    private Timestamp updated_at;
    private Timestamp created_at;
    private BigInteger lineitem_id;
    // private BigInteger picklist_id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "picklist_id")
    private PickList picklist;

    
}
