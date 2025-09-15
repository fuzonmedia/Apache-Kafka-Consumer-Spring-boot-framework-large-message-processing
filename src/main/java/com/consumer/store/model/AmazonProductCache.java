package com.consumer.store.model;

import java.math.BigInteger;
import java.sql.Timestamp;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Getter;
import lombok.Setter;

@Table(name = "amazon_product_cache")
@Getter
@Setter
@Entity
public class AmazonProductCache {


    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    private String sku;
    private String asin;
    private String status;
    private String name;
    private String description;
    private Integer quantity;
    private Double mrp;
    private Double price;
    private Timestamp launch_date;
    private Boolean processed;
    private String api_response;
    private String amazon_listing_id;
    private BigInteger log_id;
    private Boolean is_outside;
    private String product_type;
    private String relation;

}
