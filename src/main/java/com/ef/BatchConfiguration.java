package com.ef;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.format.Formatter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.validation.DataBinder;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.Locale;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Value("#{T(java.time.LocalDateTime).parse('${startDate}', T(java.time.format.DateTimeFormatter).ofPattern('yyyy-MM-dd.HH:mm:ss'))}")
    LocalDateTime startDate;

    @Value("${duration}")
    String duration;

    @Value("${threshold}")
    Integer threshold;


    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public FlatFileItemReader<LogEntry> logEntryFlatFileItemReader() {
        return new FlatFileItemReaderBuilder<LogEntry>()
                .name("logEntryReader")
                .resource(new FileSystemResource("access.log"))
                .delimited()
                .delimiter("|")
                .names(new String[]{"entryDate", "ip", "request", "status", "userAgent"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<LogEntry>() {
                    {
                        setTargetType(LogEntry.class);
                    }
                    @Override
                    protected void initBinder(DataBinder binder){
                        binder.addCustomFormatter(new Formatter<LocalDateTime>() {
                            private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                            @Override
                            public LocalDateTime parse(String text, Locale locale) {
                                return LocalDateTime.parse(text, formatter);
                            }
                            @Override
                            public String print(LocalDateTime object, Locale locale) {
                                return formatter.format(object);
                            }
                        });
                    }
                })
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<LogEntry> longEntryWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<LogEntry>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO log_entries (entry_date, ip, request, status) " +
                        "VALUES (:entryDate, :ip, :request, :status)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public JobExecutionListenerSupport jobExecutionListenerSupport() {
        return new JobExecutionListenerSupport() {
            @Override
            public void afterJob(JobExecution jobExecution) {
                System.out.println("Finished");
            }
        };
    }

    @Bean
    public Job importLogEntryJob(JobExecutionListenerSupport listener, Step importStep, Step processStep) {
        return jobBuilderFactory.get("importLogEntryJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(importStep)
                .next(processStep)
                .build();
    }

    @Bean
    public Step importStep(JdbcBatchItemWriter<LogEntry> writer) {
        return stepBuilderFactory.get("importStep")
                .<LogEntry, LogEntry> chunk(10)
                .reader(logEntryFlatFileItemReader())
                .writer(writer)
                .build();
    }

    @Bean
    JdbcCursorItemReader<BlockedEntry> processReader(DataSource dataSource) throws Exception {
        Timestamp startTimestamp = Timestamp.valueOf(startDate);
        LocalDateTime endDate = startDate
                .plus(1, duration.equals("hourly") ? ChronoUnit.HOURS : ChronoUnit.DAYS);
        Timestamp endTimestamp = Timestamp.valueOf(endDate);
        LinkedList<Object> params = new LinkedList();
        params.add(startTimestamp);
        params.add(endTimestamp);
        params.add(threshold);
        return new JdbcCursorItemReaderBuilder<BlockedEntry>()
                    .name("processReader")
                    .dataSource(dataSource)
                    .queryArguments(params)
                    .sql("select ip as ip, " +
                            "connectionCount as connectionCount " +
                            "from (select " +
                            "   ip as ip, " +
                            "   count(ip) as connectionCount " +
                            "   from log_entries " +
                            "   where entry_date >= ? " +
                            "   and entry_date < ? " +
                            "   group by (ip) ) AS counts " +
                            "where " +
                            "connectionCount > ?")
                    .rowMapper(new RowMapper<BlockedEntry>() {
                        @Override
                        public BlockedEntry mapRow(ResultSet rs, int rowNum) throws SQLException {
                            BlockedEntry result =  new BlockedEntry(
                                    rs.getString("ip"),
                                    String.format("Made %s (threshold is %s) connections between %s and %s",
                                            rs.getString("connectionCount"), threshold, startDate, endDate));
                            System.out.println(result);
                            return result;
                        }
                    })
                .build();

    }

    @Bean
    public JdbcBatchItemWriter<BlockedEntry> blockedEntryWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<BlockedEntry>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO blocked_entries (ip, comment) " +
                        "VALUES (:ip, :comment)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Step processStep(JdbcBatchItemWriter<BlockedEntry> writer, JdbcCursorItemReader<BlockedEntry> processReader) throws Exception {
        return stepBuilderFactory.get("processStep")
                .<BlockedEntry, BlockedEntry> chunk(10)
                .reader(processReader)
                .writer(writer)
                .build();
    }

}
