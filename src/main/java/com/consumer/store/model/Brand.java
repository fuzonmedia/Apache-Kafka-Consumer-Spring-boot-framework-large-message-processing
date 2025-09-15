package com.consumer.store.model;
import lombok.*;
import javax.persistence.*;
import java.math.BigInteger;


@Entity
@Table(name = "brands")
@Getter
@Setter
public class Brand {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    BigInteger id;
    String name;

    
}
