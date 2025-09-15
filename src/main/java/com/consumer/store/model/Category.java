package com.consumer.store.model;
import lombok.*;
import javax.persistence.*;
import java.math.BigInteger;


@Entity
@Table(name = "categories")
@Getter
@Setter
public class Category {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    BigInteger id;
    String name;
    BigInteger parent_category_id;

    
}
