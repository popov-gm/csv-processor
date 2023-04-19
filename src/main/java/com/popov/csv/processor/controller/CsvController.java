package com.popov.csv.processor.controller;

import com.opencsv.exceptions.CsvValidationException;
import com.popov.csv.processor.core.InvoiceCsvSplitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;

@RequestMapping("/api/v1/csv")
@RestController
public class CsvController {
    private final Logger LOGGER = LoggerFactory.getLogger(CsvController.class);

    private static final String MISSING_CSV_INPUT_FILE = "Missing csv input file.";
    private static final String UNABLE_TO_PROCESS_THE_REQUEST = "Unable to process the request";
    private static final String INVALID_CSV_INPUT_FILE = "Invalid csv input file.";

    @Autowired
    private final InvoiceCsvSplitter invoiceCsvSplitter;

    public CsvController(InvoiceCsvSplitter invoiceCsvSplitter) {
        this.invoiceCsvSplitter = invoiceCsvSplitter;
    }

    @PostMapping("/split")
    public ResponseEntity<String> splitInvoiceCsv(@RequestParam(value = "file") MultipartFile file){
        if (file == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(MISSING_CSV_INPUT_FILE);
        }

        try {
            this.invoiceCsvSplitter.splitCsvInvoicesByBuyer(file.getInputStream());
        } catch (CsvValidationException | RuntimeException e) {
            LOGGER.error(INVALID_CSV_INPUT_FILE, e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(INVALID_CSV_INPUT_FILE);
        } catch (IOException e) {
            LOGGER.error(UNABLE_TO_PROCESS_THE_REQUEST, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(UNABLE_TO_PROCESS_THE_REQUEST);
        }

        return ResponseEntity.status(HttpStatus.OK).build();
    }

}
