package com.popov.csv.processor.core;

import com.popov.csv.processor.config.OutputDirectoryResolver;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component
@ConditionalOnProperty(prefix = "output.file", name = "format", havingValue = "csv", matchIfMissing = true)
public class InvoiceCsvToCsvSplitter extends InvoiceCsvSplitter {

    private static final String CSV_EXTENSION = ".csv";
    private static final String COMMA_DELIMITER = ",";

    private  Map<String, BufferedWriter> writerMap = new HashMap<>();

    public InvoiceCsvToCsvSplitter(OutputDirectoryResolver outputDirectoryResolver) {
        super(outputDirectoryResolver);
    }

    @Override
    protected void initializeWriters() {
        this.writerMap = new HashMap<>();
    }

    @Override
    protected void writeInvoice(String[] line, String[] headers, String buyer) throws IOException {
        if(!this.writerMap.containsKey(buyer)) {
            BufferedWriter newBufferedWriter = createBufferedWriter(buyer);
            this.writerMap.put(buyer, newBufferedWriter);
            this.writeNewLine(newBufferedWriter, headers);
        }

        BufferedWriter bufferedWriter = this.writerMap.get(buyer);
        this.writeNewLine(bufferedWriter, line);
    }

    @Override
    protected void finalizeWriters() throws IOException {
        for (BufferedWriter writer: this.writerMap.values()) {
            writer.flush();
            writer.close();
        }
    }

    private BufferedWriter createBufferedWriter(String fileName) throws IOException {
        File newFile = this.createFile(fileName + CSV_EXTENSION);
        FileWriter fileWriter = new FileWriter(newFile);
        return new BufferedWriter(fileWriter);
    }

    private void writeNewLine(BufferedWriter bufferedWriter, String[] line) throws IOException {
        bufferedWriter.write(String.join(COMMA_DELIMITER, line));
        bufferedWriter.newLine();
    }
}
