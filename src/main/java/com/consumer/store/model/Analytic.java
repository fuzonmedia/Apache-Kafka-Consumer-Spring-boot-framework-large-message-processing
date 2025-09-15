package com.consumer.store.model;
import lombok.*;
import javax.persistence.*;

import org.hibernate.annotations.NaturalId;

import java.math.BigInteger;


@Entity
@Table(name = "analytics")
@Getter
@Setter

public class Analytic{
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    BigInteger id;
    @NaturalId
    @Column(name = "[key]")
    private String data_name;
    @Column(name = "[data]")
    private String data_value;
}
