package com.colak.springtutorial.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;

// Spring Batch 4 to 5 migration breaking changes
// See https://levelup.gitconnected.com/spring-batch-4-to-5-migration-breaking-changes-9bac1c063dc5
@Configuration
@Slf4j
public class CSVBatchConfig {

    @Value("classpath:/market-data.csv")
    private Resource csvFile;

    @Bean
    public Job job(JobRepository jobRepository, Step marketDataCsvStep) {
        // We are now required to pass in JobRepository upon using JobBuilder
        return new JobBuilder("job", jobRepository)
                .start(marketDataCsvStep)
                .build();
    }


    @Bean
    public Step faultTolerantMarketDataCsvStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        // We are now required to pass in JobRepository upon using StepBuilder
        // Tasklet and Chunk processing in the Step bean requires a PlatformTransactionManager.
        return new StepBuilder("step_first", jobRepository)
                .<MarketData, MarketData>chunk(4, transactionManager)
                .reader(marketDataCsvReader())
                .processor(compositeProcessor())
                // Chunk processing now processes Chunk datatype instead of a List
                .writer(chunk -> chunk.forEach(item -> log.info("Market Data: {}", item)))

                // Enable fault tolerance
                .faultTolerant()
                .skipLimit(5) // Allow skipping up to 5 items
                .skip(FlatFileParseException.class) // Skip if a parsing error happens
                .retryLimit(3) // Retry up to 3 times
                .retry(IOException.class) // Retry on IOException
                // Add a SkipListener to log invalid lines
                .listener(new SkipListener<>() {
                    @Override
                    public void onSkipInRead(Throwable throwable) {
                        if (throwable instanceof FlatFileParseException flatFileParseException) {
                            log.error("Skipped invalid line: {}", flatFileParseException.getInput());
                        }
                    }
                })
                .build();
    }

    @Bean
    public CompositeItemProcessor<MarketData, MarketData> compositeProcessor() {
        ItemProcessor<MarketData, MarketData> processor1 = marketData -> {
            log.info("ItemProcessor1 : {}", marketData);
            return marketData;
        };

        ItemProcessor<MarketData, MarketData> processor2 = marketData -> {
            log.info("ItemProcessor2 : {}", marketData);
            return marketData;
        };
        // compositeProcessor.setDelegates(processors);
        return new CompositeItemProcessor<>(processor1, processor2);
    }

    @Bean
    public ItemReader<MarketData> marketDataCsvReader() {
        return new FlatFileItemReaderBuilder<MarketData>()
                .name("marketDataCsvReader") // Reader name
                .resource(csvFile)           // Resource (CSV file)
                .linesToSkip(1)              // Skip the header line
                .delimited()                 // Enable delimited tokenizer
                // These method are just examples
                // .delimiter(DelimitedLineTokenizer.DELIMITER_COMMA)              // Specify the delimiter (comma)
                // .quoteCharacter(DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER)

                // Set names of FieldSet that we can use in MarketDataFieldSetMapper
                .names("TID", "TickerName", "TickerDescription") // Column names
                .fieldSetMapper(new MarketDataFieldSetMapper()) // Custom FieldSetMapper
                .build();
    }
}