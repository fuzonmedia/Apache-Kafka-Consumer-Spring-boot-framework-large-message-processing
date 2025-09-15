package com.consumer.store.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;
import java.math.BigInteger;
import java.sql.Timestamp;

@Entity // This tells Hibernate to make a table out of this class
@Table(name = "order_tracks")
@Getter
@Setter
public class OrderTrack {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private BigInteger id;
    private Integer status_position_id=1;
    private Integer sub_status_position_id=0;
    private String notes;
    private Timestamp created_at= Timestamp.valueOf(java.time.LocalDateTime.now());
    private Timestamp updated_at= Timestamp.valueOf(java.time.LocalDateTime.now());
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id")
    private Order order;

}
