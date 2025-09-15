package com.consumer.store.model;

import lombok.*;

import javax.persistence.*;
import java.math.BigInteger;

@Entity // This tells Hibernate to make a table out of this class
@Table(name = "clients")
@Getter
@Setter
public class Client {
    
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    private String name;
    private String db_host;
    private String db_name;
    private String db_port;
    private String db_user;
    private String db_pass;
    private Boolean db_ssl;
    private String db_ssl_ca;
    private String state;
    private BigInteger flipkart_order_count;
    private BigInteger amazon_order_count;

}
