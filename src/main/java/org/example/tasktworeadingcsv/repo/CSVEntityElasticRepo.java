package org.example.tasktworeadingcsv.repo;

import org.example.tasktworeadingcsv.entity.CSVEntity;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;


@Repository
public interface CSVEntityElasticRepo extends ElasticsearchRepository<CSVEntity,String> {
}
