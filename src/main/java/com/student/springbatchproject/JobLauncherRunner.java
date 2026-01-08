package com.student.springbatchproject;

import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
public class JobLauncherRunner implements CommandLineRunner {

    private final JobLauncher jobLauncher;
    private final Job job;

    @Value("${batch.job.auto-start:true}")
    private boolean autoStart;

    @Value("${app.input-file}")
    private String inputFilePath;

    public JobLauncherRunner(JobLauncher jobLauncher, Job dynamicCsvJob) {
        this.jobLauncher = jobLauncher;
        this.job = dynamicCsvJob;
    }

    @Override
    public void run(String... args) throws Exception {

        if (!autoStart) {
            System.out.println("Auto-start disabled. Job will not run automatically.");
            return;
        }

        System.out.println("Reading CSV file from application.properties...");
        System.out.println("Configured input file: " + inputFilePath);

        if (inputFilePath == null || inputFilePath.isBlank()) {
            System.out.println("Error: 'app.input-file' is empty in application.properties");
            return;
        }

        File csvFile = new File(inputFilePath);

        if (!csvFile.exists()) {
            System.out.println("Error: CSV file not found: " + inputFilePath);
            return;
        }

        System.out.println("Launching batch job for: " + csvFile.getName());

        JobParameters params = new JobParametersBuilder()
                .addString("input.file", csvFile.getAbsolutePath())
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        try {
            JobExecution exec = jobLauncher.run(job, params);
            System.out.println("Job completed with status: " + exec.getStatus());
        } catch (Exception e) {
            System.err.println("Failed to process " + csvFile.getName() + ": " + e.getMessage());
        }

        System.out.println("Job finished!");
    }
}
