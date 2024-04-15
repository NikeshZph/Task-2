package org.example.tasktworeadingcsv.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.elasticsearch.annotations.Document;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Document(indexName = "task2two")
@JsonIgnoreProperties(value = {"aggregations"})
public class CSVEntity {
    @Id
    private String id;
    private String bCls;
    private String bC;
    private String bCT;
    private String negA;
    private String npi;
    private String tin;
    private String tinT;
    private String zip;
    private String negT;
    private Double negR;
    private String posH;
    private String mdH;
    private String nrP;
    private String _dT;
}
