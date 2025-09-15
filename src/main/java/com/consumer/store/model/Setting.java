package com.consumer.store.model;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;

import javax.persistence.*;
import java.math.BigInteger;
import java.sql.Timestamp;

@Entity // This tells Hibernate to make a table out of this class
@Table(name = "settings")
@Getter
@Setter
public class Setting {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    @NaturalId
    @Column(name = "[key]")
    private String key_name;
    @Column(name = "[value]")
    private String key_value;
    private String notes;
    
    @Column( updatable = false)
    private Timestamp created_at = Timestamp.valueOf(java.time.LocalDateTime.now());
    private Timestamp updated_at = Timestamp.valueOf(java.time.LocalDateTime.now());

    public String getValue(){
        return this.key_value;
    }

    public void setValue(String value){
        this.key_value = value;
    }

    public void valueInc(){
        this.key_value = String.valueOf( Integer.parseInt( this.key_value ) + 1);
    }
}
