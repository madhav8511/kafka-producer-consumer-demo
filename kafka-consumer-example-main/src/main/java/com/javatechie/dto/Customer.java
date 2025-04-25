package com.javatechie.dto;
import lombok.Data;
import jakarta.persistence.*;

@Entity
@Table(name = "Customer")
@Data
public class Customer {

    @Id
    private int id;
    private String name;
    private String email;
    private String contactNo;

}
