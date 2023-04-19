package com.popov.csv.processor.core;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.popov.csv.processor.config.OutputDirectoryResolver;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.IntStream;

public abstract class InvoiceCsvSplitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(InvoiceCsvSplitter.class);

    private static final String STARTING_FILE_SPLIT_MESSAGE = "Splitting invoices file by buyers.";
    private static final String SUCCESSFUL_FILE_SPLIT_MESSAGE = "Finished splitting invoices file by buyers.";

    private static final String UTF8_BOM = "\uFEFF";
    private static final String BUYER_HEADER = "buyer";
    private static final String IMAGE_NAME_HEADER = "image_name";
    private static final String INVOICE_IMAGE_HEADER = "invoice_image";
    private static final String MISSING_BUYER_MESSAGE = "Invalid input csv - buyer is missing.";

    protected int buyerIndex = -1;
    protected int imageNameIndex = -1;
    protected int invoiceImageIndex = -1;

    @Autowired
    protected OutputDirectoryResolver outputDirectoryResolver;

    protected String outputDirectory;

    public InvoiceCsvSplitter(OutputDirectoryResolver outputDirectoryResolver) {
        this.outputDirectoryResolver = outputDirectoryResolver;
    }

    public void splitCsvInvoicesByBuyer(InputStream inputStream) throws IOException, CsvValidationException {
        LOGGER.debug(STARTING_FILE_SPLIT_MESSAGE);

        try(InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            CSVReader csvReader = new CSVReader(bufferedReader)) {

            this.outputDirectory = this.outputDirectoryResolver.getOutputDirectory();
            String[] firstLine = csvReader.readNext();

            if(firstLine != null) {
                String[] headers = this.removeUtf8BomFromHeaders(firstLine);

                this.initializeWriters();
                this.setIndices(headers);

                String[] line;

                while ((line = csvReader.readNext()) != null) {
                    if(!isValidIndex(buyerIndex, line)) {
                        this.finalizeWriters();
                        throw new RuntimeException(MISSING_BUYER_MESSAGE);
                    }
                    if(StringUtils.isEmpty(line[buyerIndex])) {
                        // Skip line if buyer value is missing
                        continue;
                    }

                    this.writeInvoice(line, headers, line[buyerIndex]);
                }

                this.finalizeWriters();
            }
        }

        inputStream.close();

        LOGGER.debug(SUCCESSFUL_FILE_SPLIT_MESSAGE);
    }

    protected abstract void initializeWriters();

    protected abstract void writeInvoice(String[] line, String[] headers, String buyer) throws IOException;

    protected abstract void finalizeWriters() throws IOException;

    protected boolean isValidIndex(int index, Object[] arr) {
        return 0 <= index && index < arr.length;
    }

    protected File createFile(String fileName) throws IOException {
        Files.createDirectories(Paths.get(outputDirectory));
        return new File(this.outputDirectory + File.separator + fileName);
    }

    private String[] removeUtf8BomFromHeaders(String[] headers) {
        return Arrays
                .stream(headers)
                .map(this::removeUtf8Bom)
                .toArray(String[]::new);
    }

    private String removeUtf8Bom(String str) {
        if(str.startsWith(UTF8_BOM)) {
            str = str.substring(1);
        }
        return str;
    }

    private void setIndices(String[] headers) {
        buyerIndex = getElementIndex(BUYER_HEADER, headers);
        imageNameIndex = getElementIndex(IMAGE_NAME_HEADER, headers);
        invoiceImageIndex = getElementIndex(INVOICE_IMAGE_HEADER, headers);
    }

    private int getElementIndex(String element, String[] elements) {
        return IntStream
                .range(0, elements.length)
                .filter(i -> elements[i].equals(element))
                .findFirst()
                .orElse(-1);
    }

}
