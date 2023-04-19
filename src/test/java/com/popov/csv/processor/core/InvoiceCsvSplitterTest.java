package com.popov.csv.processor.core;

import com.opencsv.exceptions.CsvValidationException;
import com.popov.csv.processor.config.OutputDirectoryResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@SpringBootTest
@ExtendWith(SpringExtension.class)
public class InvoiceCsvSplitterTest {

	public static final String BASE64_JPG = "/9j/4AAQSkZJRgABAQEAeAB4AAD/4QAiRXhpZgAATU0AKgAAAAgAAQESAAMAAAABAAEAAAAAAAD/2wBDAAIBAQIBAQICAgICAgICAwUDAwMDAwYEBAMFBwYHBwcGBwcICQsJCAgKCAcHCg0KCgsMDAwMBwkODw0MDgsMDAz/2wBDAQICAgMDAwYDAwYMCAcIDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAz/wAARCAABAAEDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwD9/KKKKAP/2Q==";
	public static final String BASE64_PNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAAEnQAABJ0Ad5mH3gAAAANSURBVBhXY/j///9/AAn7A/0FQ0XKAAAAAElFTkSuQmCC";
	public static final String BASE64_TIF = "SUkqAA4AAACAP+BP+AgTAP4ABAABAAAAAAAAAAABBAABAAAAAQAAAAEBBAABAAAAAQAAAAIBAwAEAAAA+AAAAAMBAwABAAAABQAAAAYBAwABAAAAAgAAABEBBAABAAAACAAAABIBAwABAAAAAQAAABUBAwABAAAABAAAABYBBAABAAAAAQAAABcBBAABAAAABgAAABoBBQABAAAAAAEAABsBBQABAAAACAEAABwBAwABAAAAAQAAACgBAwABAAAAAgAAAD0BAwABAAAAAgAAAFIBAwABAAAAAgAAAJBQAwBAAAAAEAEAAJFQAwBAAAAAkAEAAAAAAAAIAAgACAAIALbUAQDoAwAAttQBAOgDAAACAAEAAQACAAMABQAGAAcAAQABAAIAAgADAAcABwAHAAIAAgACAAMABQAHAAgABwACAAIAAwADAAYACgAKAAcAAgADAAQABwAIAA0ADAAJAAMABAAHAAgACgAMAA4ACwAGAAgACQAKAAwADwAOAAwACQALAAsADAANAAwADAAMAAIAAgADAAYADAAMAAwADAACAAMAAwAIAAwADAAMAAwAAwADAAcADAAMAAwADAAMAAYACAAMAAwADAAMAAwADAAMAAwADAAMAAwADAAMAAwADAAMAAwADAAMAAwADAAMAAwADAAMAAwADAAMAAwADAAMAAwADAAMAAwADAAMAAwA";

	public static final String INPUT_CSV = "buyer,image_name,invoice_image\n" +
			String.format("John,image1.jpg,%s\n", BASE64_JPG) +
			String.format("Jane,image2.png,%s\n", BASE64_PNG) +
			String.format("John,image3.tif,%s\n", BASE64_TIF);
	public static final String MISSING_BUYER_VALUE_INPUT_CSV = "buyer,image_name,invoice_image\n" +
			String.format("John,image1.jpg,%s\n", BASE64_JPG) +
			String.format(",image2.png,%s\n", BASE64_PNG) +
			String.format("Jane,image3.tif,%s\n", BASE64_TIF);
	public static final String MISSING_BUYER_HEADER_INPUT_CSV = "seller,image_name,invoice_image\n" +
			"John,Doe,123\n" +
			"Jane,Smith,456\n";
	public static final String ONLY_HEADERS_CSV = "buyer,image_name,invoice_image";
	public static final String JOHN_CSV = "John.csv";
	public static final String JANE_CSV = "Jane.csv";
	public static final String JOHN_XML = "John.xml";
	public static final String JANE_XML = "Jane.xml";
	public static final String IMAGE_1 = "image1.jpg";
	public static final String IMAGE_2 = "image2.png";
	public static final String IMAGE_3 = "image3.tif";

	@Mock
	private OutputDirectoryResolver outputDirectoryResolver;

	@Autowired
	private InvoiceCsvSplitter invoiceCsvSplitter;

	@TempDir
	private Path tempDir;

	private InputStream inputStream;

	@BeforeEach
	public void setUp() {
		when(this.outputDirectoryResolver.getOutputDirectory()).thenReturn(this.tempDir.toString());
	}

	@Test
	public void testSplitCsvInvoicesByBuyerToCsv() throws IOException, CsvValidationException {
		this.arrange(new InvoiceCsvToCsvSplitter(this.outputDirectoryResolver),INPUT_CSV);

		this.invoiceCsvSplitter.splitCsvInvoicesByBuyer(inputStream);

		Path johnCsv = this.tempDir.resolve(JOHN_CSV);
		Path janeCsv = this.tempDir.resolve(JANE_CSV);

		assertTrue(Files.exists(johnCsv));
		assertTrue(Files.exists(janeCsv));
	}

	@Test
	public void testSplitCsvInvoicesByBuyerToXml() throws IOException, CsvValidationException {
		this.arrange(new InvoiceCsvToXmlSplitter(this.outputDirectoryResolver), INPUT_CSV);

		this.invoiceCsvSplitter.splitCsvInvoicesByBuyer(this.inputStream);

		Path johnXml = this.tempDir.resolve(JOHN_XML);
		Path janeXml = this.tempDir.resolve(JANE_XML);
		Path image1 = this.tempDir.resolve(IMAGE_1);
		Path image2 = this.tempDir.resolve(IMAGE_2);
		Path image3 = this.tempDir.resolve(IMAGE_3);

		assertTrue(Files.exists(johnXml));
		assertTrue(Files.exists(janeXml));
		assertTrue(Files.exists(image1));
		assertTrue(Files.exists(image2));
		assertTrue(Files.exists(image3));
	}

	@Test
	public void testSplitCsvInvoicesByBuyerMissingBuyerValue() throws CsvValidationException, IOException {
		this.arrange(new InvoiceCsvToCsvSplitter(this.outputDirectoryResolver), MISSING_BUYER_VALUE_INPUT_CSV);

		this.invoiceCsvSplitter.splitCsvInvoicesByBuyer(inputStream);

		Path johnCsv = this.tempDir.resolve(JOHN_CSV);
		Path janeCsv = this.tempDir.resolve(JANE_CSV);

		assertTrue(Files.exists(johnCsv));
		assertTrue(Files.exists(janeCsv));
	}

	@Test
	public void testSplitCsvInvoicesByBuyerMissingBuyerHeader() {
		this.arrange(new InvoiceCsvToCsvSplitter(this.outputDirectoryResolver), MISSING_BUYER_HEADER_INPUT_CSV);

		assertThrows(RuntimeException.class, () -> invoiceCsvSplitter.splitCsvInvoicesByBuyer(inputStream));
	}

	@Test
	public void testSplitCsvInvoicesByBuyerOnlyHeaders() throws CsvValidationException, IOException {
		this.arrange(new InvoiceCsvToCsvSplitter(this.outputDirectoryResolver), ONLY_HEADERS_CSV);

		invoiceCsvSplitter.splitCsvInvoicesByBuyer(inputStream);

		assertTrue(isEmpty(tempDir));
	}

	private boolean isEmpty(Path path) throws IOException {
		if (Files.isDirectory(path)) {
			try (Stream<Path> entries = Files.list(path)) {
				return entries.findFirst().isEmpty();
			}
		}

		return false;
	}

	private void arrange(InvoiceCsvSplitter invoiceCsvSplitter, String inputCsv) {
		this.invoiceCsvSplitter = invoiceCsvSplitter;
		this.inputStream = new ByteArrayInputStream(inputCsv.getBytes(StandardCharsets.UTF_8));
	}
}