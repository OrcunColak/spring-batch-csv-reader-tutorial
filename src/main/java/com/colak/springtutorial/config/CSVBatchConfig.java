package com.colak.springtutorial.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@Slf4j
public class CSVBatchConfig {

    @Value("classpath:/market-data.csv")
    private Resource csvFile;

    @Bean
    public Job job(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("job", jobRepository)
                .start(marketDataCsvStep(jobRepository, transactionManager))
                .build();
    }


    @Bean
    public Step marketDataCsvStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step_first", jobRepository).<MarketData, MarketData>chunk(4, transactionManager)
                .reader(marketDataCsvReader())
                .writer(chunk -> chunk.forEach(item -> log.info("Market Data: {}", item)))
                .build();
    }

    @Bean
    public ItemReader<MarketData> marketDataCsvReader() {
        FlatFileItemReader<MarketData> reader = new FlatFileItemReader<>();
        reader.setLinesToSkip(1);
        reader.setResource(csvFile);

        DefaultLineMapper<MarketData> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(",");
        tokenizer.setNames("TID", "TickerName", "TickerDescription");

        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new MarketDataFieldSetMapper());

        reader.setLineMapper(lineMapper);
        return reader;
    }
}