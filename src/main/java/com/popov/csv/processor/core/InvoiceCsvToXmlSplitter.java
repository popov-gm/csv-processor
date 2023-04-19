package com.popov.csv.processor.core;

import com.popov.csv.processor.config.OutputDirectoryResolver;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

@Component
@ConditionalOnProperty(prefix = "output.file", name = "format", havingValue = "xml")
public class InvoiceCsvToXmlSplitter extends InvoiceCsvSplitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(InvoiceCsvToXmlSplitter.class);

    private static final String INVOICES_ELEMENT = "invoices";
    private static final String INVOICE_ELEMENT = "invoice";
    private static final String XML_EXTENSION = ".xml";

    private static final String INVALID_IMAGE_COLUMN_MESSAGE = "Invalid value in invoice_image column.";
    private static final String ERROR_FINALIZING_XMLSTREAM_MESSAGE = "Error finalizing XMLStreamWriter.";

    private XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();
    private Map<String, XMLStreamWriter> writerMap = new HashMap<>();
    private Set<FileWriter> writerSet = new HashSet<>();

    public InvoiceCsvToXmlSplitter(OutputDirectoryResolver outputDirectoryResolver) {
        super(outputDirectoryResolver);
    }

    @Override
    protected void initializeWriters() {
        this.xmlOutputFactory = XMLOutputFactory.newFactory();
        this.writerMap = new HashMap<>();
        this.writerSet = new HashSet<>();
    }

    @Override
    protected void writeInvoice(String[] line, String[] headers, String buyer) throws IOException {
        try {
            if(!this.writerMap.containsKey(buyer)) {
                XMLStreamWriter xmlStreamWriter = createXMLStreamWriter(buyer);
                this.writerMap.put(buyer, xmlStreamWriter);

                xmlStreamWriter.writeStartDocument();
                xmlStreamWriter.writeStartElement(INVOICES_ELEMENT);
            }

            XMLStreamWriter xmlStreamWriter = this.writerMap.get(buyer);
            this.writeNewRow(xmlStreamWriter, line, headers);
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void finalizeWriters() throws IOException {
        for (XMLStreamWriter writer: this.writerMap.values()) {
            try {
                writer.writeEndDocument();
                writer.flush();
                writer.close();
            } catch (XMLStreamException e) {
                LOGGER.error(ERROR_FINALIZING_XMLSTREAM_MESSAGE);
                throw new IOException(e);
            }
        }

        for (FileWriter writer: this.writerSet) {
            writer.close();
        }
    }

    private void writeNewRow(XMLStreamWriter xmlStreamWriter, String[] line, String[] headers) throws XMLStreamException, IOException {
        xmlStreamWriter.writeStartElement(INVOICE_ELEMENT);
        for (int i = 0; i < headers.length; i++) {
            if(i == invoiceImageIndex && isValidIndex(imageNameIndex, line) && isValidIndex(invoiceImageIndex, line)) {
                this.writeImageToFile(line[imageNameIndex], line[invoiceImageIndex]);
            } else {
                this.writeNewRowItem(xmlStreamWriter, headers[i], line[i]);
            }
        }
        xmlStreamWriter.writeEndElement();
    }

    private void writeNewRowItem(XMLStreamWriter xmlStreamWriter, String header, String value) throws XMLStreamException {
        xmlStreamWriter.writeStartElement(header);
        xmlStreamWriter.writeCharacters(value);
        xmlStreamWriter.writeEndElement();
    }

    private void writeImageToFile(String fileName, String content) throws IOException {
        if(StringUtils.isNotEmpty(fileName) && StringUtils.isNotEmpty(content)) {
            byte[] binaryData = this.decodeBase64(content);
            File file = this.createFile(fileName);
            try( FileOutputStream fos = new FileOutputStream(file)) {
                fos.write(binaryData);
            }
        }
    }

    private XMLStreamWriter createXMLStreamWriter(String fileName) throws IOException, XMLStreamException {
        File newFile = this.createFile(fileName + XML_EXTENSION);
        FileWriter fileWriter = new FileWriter(newFile);
        this.writerSet.add(fileWriter);
        return this.xmlOutputFactory.createXMLStreamWriter(fileWriter);
    }

    private byte[] decodeBase64(String base64) {
        try {
            return Base64.getDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            LOGGER.error(INVALID_IMAGE_COLUMN_MESSAGE);
            throw new RuntimeException(INVALID_IMAGE_COLUMN_MESSAGE, e);
        }
    }
}
