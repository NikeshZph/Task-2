package org.example.tasktworeadingcsv;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.tasktworeadingcsv.entity.CSVEntity;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipFile;
@Slf4j
@SpringBootApplication
@EnableElasticsearchRepositories(basePackages = {"org.example.tasktworeadingcsv.repo"})
public class TaskTwoReadingCsvApplication implements CommandLineRunner {



    public static void main(String[] args) {
        SpringApplication.run(TaskTwoReadingCsvApplication.class, args);
    }




    @Override
    public void run(String... args) throws Exception {
        String zipFilePath = "/home/zakipoint/Desktop/testtask2/src/main/resources/static/sample-data.zip";
        String destinationDir = "/home/zakipoint/Desktop/testtask2/src/main/resources/static/csv";
        String mergedCsvFilePath = "/home/zakipoint/Desktop/testtask2/src/main/resources/static/csv/merged.csv";

        File destination = new File(destinationDir);
        if (!destination.exists()) {
            destination.mkdirs();
        }
        File mergedCsvFile = new File(mergedCsvFilePath);
        if (mergedCsvFile.exists()) {
            System.out.println("Merged CSV file already exists: " + mergedCsvFilePath);
            return;
        }
        try (BufferedWriter mergedWriter = new BufferedWriter(new FileWriter(mergedCsvFilePath))) {
            mergedWriter.write("bCls,bC,bCT,negA,npi,tin,tinT,zip,negT,negR,posH,mdH,nrP,_dT");
            mergedWriter.newLine();
            try (ZipFile zipFile = new ZipFile(zipFilePath)) {
                zipFile.stream().forEach(entry -> {
                    try {
                        String entryFileName = new File(entry.getName()).getName();
                        if (!entry.isDirectory() && entryFileName.toLowerCase().endsWith(".gz")) {
                            String extractedGzFilePath = destinationDir + File.separator + entryFileName;
                            try (InputStream inputStream = zipFile.getInputStream(entry);
                                 GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
                                 BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream))) {
                                reader.readLine();
                                String line;
                                while ((line = reader.readLine()) != null) {
                                    JSONObject jsonObject = new JSONObject(line);
                                    StringBuilder csvLine = new StringBuilder();
                                    csvLine.append(jsonObject.getInt("bCls")).append(",")
                                            .append(jsonObject.getString("bC")).append(",")
                                            .append(jsonObject.getString("bCT")).append(",")
                                            .append(jsonObject.getInt("negA")).append(",")
                                            .append(jsonObject.getString("npi")).append(",")
                                            .append(jsonObject.getString("tin")).append(",")
                                            .append(jsonObject.getInt("tinT")).append(",")
                                            .append(jsonObject.getInt("zip")).append(",")
                                            .append(jsonObject.getInt("negT")).append(",")
                                            .append(jsonObject.getDouble("negR")).append(",")
                                            .append(jsonObject.getInt("posH")).append(",")
                                            .append(jsonObject.getInt("mdH")).append(",")
                                            .append(jsonObject.getInt("nrP")).append(",")
                                            .append(jsonObject.getString("_dT"));
                                    mergedWriter.write(csvLine.toString());
                                    mergedWriter.newLine();
                                }
                                System.out.println("Extracted and merged from .gz: " + entryFileName);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}