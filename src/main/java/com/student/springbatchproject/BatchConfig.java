package com.student.springbatchproject;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

@Configuration
@EnableBatchProcessing
public class BatchConfig {


    @Value("${app.chunk-size}")
    private int chunkSize;

    @Bean
    @StepScope
    public HeaderHolder headerHolder(@Value("#{jobParameters['input.file']}") String inputFilePath) {
        try {
            if (inputFilePath == null) return new HeaderHolder(new String[0]);
            FileSystemResource res = new FileSystemResource(inputFilePath);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(res.getInputStream(), StandardCharsets.UTF_8))) {
                String headerLine = br.readLine();
                if (headerLine == null) return new HeaderHolder(new String[0]);
                headerLine = headerLine.replace("\uFEFF", "");
                String[] raw = headerLine.split(",", -1);
                for (int i = 0; i < raw.length; i++) raw[i] = raw[i].trim();
                return new HeaderHolder(raw);
            }
        } catch (Exception e) {
            return new HeaderHolder(new String[0]);
        }
    }

    @Bean
    @StepScope
    public FlatFileItemReader<String[]> dynamicReader(@Value("#{jobParameters['input.file']}") String inputFilePath) {
        FlatFileItemReader<String[]> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(inputFilePath));

        DefaultLineMapper<String[]> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setStrict(false);

        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new DynamicFieldSetMapper());

        reader.setLineMapper(lineMapper);
        reader.setLinesToSkip(1);
        return reader;
    }


    @Bean
    public DynamicItemProcessor processor() { return new DynamicItemProcessor(); }

    @Bean
    @StepScope
    public DynamicItemWriter writer(
            @org.springframework.beans.factory.annotation.Qualifier("targetJdbcTemplate") JdbcTemplate jdbcTemplate,
            HeaderHolder headerHolder,
            @Value("#{jobParameters['input.file']}") String inputFilePath
    ) {
        return new DynamicItemWriter(jdbcTemplate, headerHolder, inputFilePath);
    }



    @Bean
    public Step dynamicStep(JobRepository jobRepository,
                            PlatformTransactionManager transactionManager,
                            FlatFileItemReader<String[]> reader,
                            DynamicItemProcessor processor,
                            DynamicItemWriter writer) {

        return new StepBuilder("dynamicStep", jobRepository)
                .<String[], String[]>chunk(chunkSize, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public Job dynamicJob(JobRepository jobRepository, Step dynamicStep) {
        return new JobBuilder("dynamicCsvJob", jobRepository)
                .start(dynamicStep)
                .build();
    }
}
