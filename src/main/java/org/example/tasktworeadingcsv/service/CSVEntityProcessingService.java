package org.example.tasktworeadingcsv.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.tasktworeadingcsv.entity.CSVEntity;
import org.example.tasktworeadingcsv.repo.CSVEntityElasticRepo;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.ResourceUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


@Service
@Slf4j
@RequiredArgsConstructor
public class CSVEntityProcessingService {

    private final CSVEntityElasticRepo csvEntityElasticRepo;
    private final ElasticsearchOperations elasticsearchOperations;

    public void processCSVFileInChunks(int batchSize) {
        try {
            URL resource = ResourceUtils.getURL("classpath:static/csv/merged.csv");
            try (BufferedReader reader = new BufferedReader(new FileReader(resource.getFile()))) {
                reader.readLine();

                String line;
                List<CSVEntity> entities = new ArrayList<>();
                int count = 0;
                while ((line = reader.readLine()) != null) {
                    String[] values = line.split(",");
                    if (values.length < 14) {
                        log.error("Incomplete data in CSV line: {}", line);
                        continue;
                    }
                    CSVEntity csvEntity = new CSVEntity();
                    csvEntity.setBCls(values[0]);
                    csvEntity.setBC(values[1]);
                    csvEntity.setBCT(values[2]);
                    csvEntity.setNegA(values[3]);
                    csvEntity.setNpi(values[4]);
                    csvEntity.setTin(values[5]);
                    csvEntity.setTinT(values[6]);
                    csvEntity.setZip(values[7]);
                    csvEntity.setNegT(values[8]);
                    csvEntity.setNegR(Double.parseDouble(values[9]));
                    csvEntity.setPosH(values[10]);
                    csvEntity.setMdH(values[11]);
                    csvEntity.setNrP(values[12]);
                    csvEntity.set_dT(values[13]);

                    entities.add(csvEntity);
                    count++;

                    if (count % batchSize == 0) {
                        saveEntitiesToElasticsearch(entities);
                        entities.clear();
                        log.info("Processed {} records", count);
                    }
                }

                if (!entities.isEmpty()) {
                    saveEntitiesToElasticsearch(entities);
                    log.info("Processed {} records", count);
                }
            }
        } catch (IOException e) {
            log.error("Failed to read CSV file: {}", e.getMessage());
        }
    }

    private void saveEntitiesToElasticsearch(List<CSVEntity> entities) {
        for (CSVEntity entity : entities) {
            csvEntityElasticRepo.save(entity);
            log.info("Saved CSV entity: {}", entity);
        }
    }
    public void seeConnectedIndex() {
        String indexName = elasticsearchOperations.getIndexCoordinatesFor(CSVEntity.class).getIndexName();
        System.out.println("Connected to index: " + indexName);
    }

    public ResponseEntity<?> executeElasticsearchQuery() {
        seeConnectedIndex();
        if (elasticsearchOperations == null) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("ElasticsearchOperations is not initialized");
        }

        Criteria criteria = Criteria.where("bCT.keyword").is("CPT");
        CriteriaQuery query = new CriteriaQuery(criteria);
        query.addSourceFilter(new FetchSourceFilter(new String[]{"bC", "bCT", "negR"}, null));
        query.setPageable(PageRequest.of(0, 10));
        query.addSort(Sort.by(Sort.Direction.DESC, "negR"));

        SearchHits<CSVEntity> searchHits = elasticsearchOperations.search(query, CSVEntity.class);
        List<CSVEntity> top10Results = searchHits.getSearchHits().stream()
                .map(SearchHit::getContent)
                .filter(csvEntity -> csvEntity.getBC() != null && csvEntity.getBCT() != null && csvEntity.getNegR() != null)
                .limit(10)
                .collect(Collectors.toList());
        return ResponseEntity.ok(top10Results);
    }
}



