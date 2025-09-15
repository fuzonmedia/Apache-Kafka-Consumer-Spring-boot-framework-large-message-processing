package com.consumer.store.model;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;

import javax.persistence.*;
import java.math.BigInteger;
import java.sql.Timestamp;

@Entity // This tells Hibernate to make a table out of this class
@Table(name = "customers")
@Getter
@Setter
public class Customer {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    private String name;
    private String email;
    private Timestamp created_at= Timestamp.valueOf(java.time.LocalDateTime.now());
    private Timestamp updated_at= Timestamp.valueOf(java.time.LocalDateTime.now());
    @NaturalId
    private String phone;
    private String address;
    private String state;
    private String gstin;
    @Transient
    private String pin_code;
    @Transient
    private Boolean is_prime;
    private String customer_type;

}
