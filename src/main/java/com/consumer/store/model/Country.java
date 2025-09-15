package com.consumer.store.model;
import lombok.*;
import javax.persistence.*;
import java.math.BigInteger;


@Entity
@Table(name = "countries")
@Getter
@Setter
public class Country {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    BigInteger id;
    String name;

    
}
