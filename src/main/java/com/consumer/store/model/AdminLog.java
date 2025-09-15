package com.consumer.store.model;

import com.google.api.client.json.Json;
import lombok.*;
import org.apache.kafka.common.protocol.types.Field;

import javax.persistence.*;
import java.math.BigInteger;
import java.sql.Timestamp;

@Entity // This tells Hibernate to make a table out of this class
@Table(name = "logs")
@Getter
@Setter
@ToString
public class AdminLog {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    private String name;
    private String description;
    private String input_files;
    private String output_files;
    private Timestamp updated_at;
    private Timestamp created_at;
    private String status;
    private String topic;

    public Log getClone(){
        Log clone=new Log();
        clone.setName(this.name);
        clone.setDescription(this.description);
        clone.setInput_files(this.input_files);
        clone.setOutput_files(this.output_files);
        clone.setStatus(this.status);
        clone.setTopic(this.topic);
        return clone;
    }
}
