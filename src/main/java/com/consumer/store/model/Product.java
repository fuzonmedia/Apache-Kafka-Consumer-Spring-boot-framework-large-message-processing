package com.consumer.store.model;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;

import javax.persistence.*;

import java.math.BigInteger;
import java.util.Map;
import java.sql.Timestamp;

@Entity // This tells Hibernate to make a table out of this class
@Table(name = "products")
@Getter
@Setter
public class Product {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    @NaturalId(mutable = true)
    private String sku;
    private Double tax_rate=0.0;
    private String hsn_code="";
    private String asin;
    private String name;
    private String description;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "category_id")
    private Category category;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name="brand_id")
    private Brand brand;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name="manufacturer_id")
    private Manufacturer manufacturer;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name="country_id")
    private Country country;

    private String amazon_listing_id;
    private Timestamp launch_date;
    private String pictures;
    @Lob
    private String rank;
    private Byte active;


}
