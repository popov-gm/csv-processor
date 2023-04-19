package com.popov.csv.processor.core;

import com.opencsv.exceptions.CsvValidationException;
import com.popov.csv.processor.controller.CsvController;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
public class CsvControllerTest {

    public static final String FILE = "file";
    public static final String TEST_CSV = "test.csv";
    public static final String TEXT_CSV = "text/csv";

    public static final String MISSING_CSV_INPUT_FILE_MESSAGE = "Missing csv input file.";
    public static final String INVALID_CSV_INPUT_FILE_MESSAGE = "Invalid csv input file.";
    public static final String UNABLE_TO_PROCESS_THE_REQUEST_MESSAGE = "Unable to process the request";

    @Mock
    private InvoiceCsvSplitter invoiceCsvSplitter;

    @Mock
    private InputStream inputStream;

    @InjectMocks
    private CsvController csvController;

    @Test
    public void splitInvoiceCsvWithValidFile() throws IOException {
        MockMultipartFile file = new MockMultipartFile(FILE, TEST_CSV, TEXT_CSV, inputStream);

        ResponseEntity<String> response = csvController.splitInvoiceCsv(file);

        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    public void splitInvoiceCsvWithNullFile()  {
        MockMultipartFile file = null;

        ResponseEntity<String> response = csvController.splitInvoiceCsv(file);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals(MISSING_CSV_INPUT_FILE_MESSAGE, response.getBody());

    }

    @Test
    public void splitInvoiceCsvWithInvalidFile() throws IOException, CsvValidationException {
        this.verifyThrowsException(new CsvValidationException(), HttpStatus.BAD_REQUEST, INVALID_CSV_INPUT_FILE_MESSAGE);
    }

    @Test
    public void splitInvoiceCsvWithIOException() throws IOException, CsvValidationException {
        this.verifyThrowsException(new IOException(),HttpStatus.INTERNAL_SERVER_ERROR, UNABLE_TO_PROCESS_THE_REQUEST_MESSAGE);
    }

    private void verifyThrowsException(Throwable throwable, HttpStatus expectedStatus, String expectedMessage) throws IOException, CsvValidationException {
        MockMultipartFile file = new MockMultipartFile(FILE, TEST_CSV, TEXT_CSV, inputStream);
        doThrow(throwable).when(invoiceCsvSplitter).splitCsvInvoicesByBuyer(any());

        ResponseEntity<String> response = csvController.splitInvoiceCsv(file);

        assertEquals(expectedStatus, response.getStatusCode());
        assertEquals(expectedMessage, response.getBody());
    }

}
