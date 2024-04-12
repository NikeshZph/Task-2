package org.example.tasktworeadingcsv.controller;


import lombok.RequiredArgsConstructor;
import org.example.tasktworeadingcsv.entity.CSVEntity;
import org.example.tasktworeadingcsv.service.CSVEntityProcessingService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import java.util.List;
@Controller
@RequestMapping("/api/csv")
@RequiredArgsConstructor
public class CSVEntityController {

    private final CSVEntityProcessingService csvEntityProcessingService;

    @PostMapping("/process")
    public ResponseEntity<String> processCSV() {
        try {
            csvEntityProcessingService.processCSVFileInChunks(100000);
            return ResponseEntity.ok("CSV file processed successfully.");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to process CSV file: " + e.getMessage());
        }
    }

    @GetMapping("/get")
    public String displayResults(Model model) {
        ResponseEntity<List<CSVEntity>> responseEntity = (ResponseEntity<List<CSVEntity>>) csvEntityProcessingService.executeElasticsearchQuery();
        if (responseEntity.getStatusCode() == HttpStatus.OK) {
            List<CSVEntity> top10Results = responseEntity.getBody();
            model.addAttribute("top10Results", top10Results);
            return "results";
        } else {
            return "error";
        }
    }
    










}


