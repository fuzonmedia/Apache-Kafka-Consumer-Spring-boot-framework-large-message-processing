package com.consumer.store.model;
import lombok.*;
import javax.persistence.*;
import java.math.BigInteger;


@Entity
@Table(name = "manufacturers")
@Getter
@Setter
public class Manufacturer {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    BigInteger id;
    String name;

    
}
