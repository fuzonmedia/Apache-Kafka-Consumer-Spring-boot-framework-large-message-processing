package com.consumer.store.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.math.BigInteger;
import java.sql.Timestamp;

@Entity // This tells Hibernate to make a table out of this class
@Table(name = "items")
@Getter
@Setter
@ToString
public class Item {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private BigInteger id;
    @NaturalId(mutable = true)
    private String sku;
    private String name;
    private Double price;
    private Integer quantity;
    private Double mrp;
    private Byte active;
    private Double flipkart_mrp;
    private Double flipkart_selling_price;
    private Integer flipkart_quantity;
    private Byte flipkart_active_listing;
    private String flipkart_listing_id;
    private String flipkart_product_id;
    private String inventory_location;
    private Timestamp flipkart_last_sync_at;
    private Timestamp inventory_updated_at;
    private Byte sync_flipcart;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "product_id")
    private Product product;
    private String dimentions;
    private String dimension_unit;
    private String variants;
    private String amazon_listing_id;
    private String weight;
    private String weight_unit;
    private Byte sync_amazon;
    private Timestamp updated_at;
    private Timestamp created_at;
    private String asin;
    @Lob
    private String rank;
    private String inventory_type;
    private Boolean newly_added;
    private String amazon_sku_alias;
    private String flipkart_sku_alias;
    private Timestamp amazon_qty_updated_at;
    private Timestamp flipkart_qty_updated_at;


}
