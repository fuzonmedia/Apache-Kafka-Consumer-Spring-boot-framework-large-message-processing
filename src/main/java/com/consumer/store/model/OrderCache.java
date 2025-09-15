package com.consumer.store.model;
import java.math.BigInteger;
import java.sql.Timestamp;

import javax.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.util.List;

@Entity // This tells Hibernate to make a table out of this class
@Table(name = "orders_cache")
@Getter
@Setter
public class OrderCache {

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;

    private String     channel;
    private String     api_response;
    private String     api_request;
    private BigInteger log_id;
    private Boolean    processed;
    private Integer    status_code;
    private String     next_token;
    private String     context;

    private Timestamp  created_at;
    private Timestamp  updated_at;

    @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "parent_id" , referencedColumnName = "id")
    private OrderCache parent_cache;


    @OneToMany(mappedBy="parent_cache", cascade = CascadeType.ALL ,fetch = FetchType.EAGER)
    private List<OrderCache> child_caches;


    
}
