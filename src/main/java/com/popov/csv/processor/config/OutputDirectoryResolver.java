package com.popov.csv.processor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
public class OutputDirectoryResolver {

    @Value("${output.directory:outputFiles}")
    private String outputDirectory;

    public String getOutputDirectory() {
        return this.outputDirectory + File.separator + System.currentTimeMillis();
    }
}
