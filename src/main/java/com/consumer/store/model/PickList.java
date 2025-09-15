package com.consumer.store.model;

import lombok.Getter;
import lombok.Setter;
import java.math.BigInteger;
import java.util.List;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name = "picklists")
@Getter
@Setter
public class PickList{

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    private String name;
    private Timestamp updated_at;
    private Timestamp created_at;
    private String status;

    @OneToMany(fetch = FetchType.LAZY)
    @JoinColumn(name = "picklist_id")
    private List<PickListItem> items;
    
}
