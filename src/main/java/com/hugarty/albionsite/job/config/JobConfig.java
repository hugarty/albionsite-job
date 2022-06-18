package com.hugarty.albionsite.job.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.RestTemplate;

import com.hugarty.albionsite.job.item.JdbcPagingItemReaderProviderBuilder;
import com.hugarty.albionsite.job.item.stepone.AllianceGuildCompositeItemWriterProvider;
import com.hugarty.albionsite.job.item.stepone.FetchAlliancesAndDetachedGuildsItemProcessor;
import com.hugarty.albionsite.job.item.stepthree.BuildAlliancesDailyItemProcessor;
import com.hugarty.albionsite.job.item.steptwo.FetchGuildDailyAndInvalidAllianceItemProcessor;
import com.hugarty.albionsite.job.item.steptwo.GuildDailyCompositeItemWriterProvider;
import com.hugarty.albionsite.job.model.Alliance;
import com.hugarty.albionsite.job.model.AllianceDaily;
import com.hugarty.albionsite.job.model.Guild;
import com.hugarty.albionsite.job.model.WrapperAllianceGuilds;
import com.hugarty.albionsite.job.model.WrapperInvalidAllianceGuildDaily;

@Configuration
@EnableRetry
public class JobConfig {

  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;

  public JobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
  }

  @Bean
  public RestTemplate restTemplate() {
    return new RestTemplateBuilder()
        .rootUri("https://gameinfo.albiononline.com/api/gameinfo")
        .build();
  }

  @Bean
  public JdbcTemplate jdbcTemplate(DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }

  private ThreadPoolTaskExecutor threadPoolTaskExecutor () {
    ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
    taskExecutor.setMaxPoolSize(10);
    taskExecutor.setQueueCapacity(10);
    taskExecutor.setCorePoolSize(10);
    taskExecutor.setPrestartAllCoreThreads(true);
    taskExecutor.afterPropertiesSet();
    return taskExecutor;
  }

  @Bean
  public Job job(@Qualifier("fixInvalidAlliancesAddNewGuilds") Step fixInvalidAlliancesAddNewGuilds, 
      @Qualifier("fetchGuildDailyAndInvalidAlliances") Step fetchGuildDailyAndInvalidAlliances,
      @Qualifier("insertGuildDaily") Step insertGuildDaily) {
    
    return jobBuilderFactory.get("Fetch alliances and guilds to populate database")
        .start(fixInvalidAlliancesAddNewGuilds)
        .next(fetchGuildDailyAndInvalidAlliances)
        .next(insertGuildDaily)
        .build();
  }

  /**
   * Step one responsable to fetch alliance data for invalid alliances.
   * If needed, It will insert new guilds.
   */
  @Bean(name = "fixInvalidAlliancesAddNewGuilds")
  public Step fixInvalidAlliancesAddNewGuilds(
      @Qualifier("invalidAlliancesItemReader") ItemReader<Alliance> reader,
      FetchAlliancesAndDetachedGuildsItemProcessor processor,
      @Qualifier("updateAllianceAndInsertGuildItemWriter") CompositeItemWriter<WrapperAllianceGuilds> writer) {

    return stepBuilderFactory.get("Fix invalid alliances and add their new guilds")
        .<Alliance, WrapperAllianceGuilds>chunk(10)
        .reader(reader)
        .processor(processor)
        .writer(writer)
        .taskExecutor(threadPoolTaskExecutor())
        .build();
  }

  @Bean(name = "invalidAlliancesItemReader") 
  public JdbcPagingItemReader<Alliance> invalidAlliancesItemReader(DataSource dataSource) {
    return new JdbcPagingItemReaderProviderBuilder<Alliance>()
        .name("invalid alliances item reader")
        .selectClause("select id, albion_id")
        .fromClause("from alliance")
        .whereClause("where alliance.name is null")
        .sortKey("id")
        .rowMapper((rs, rowNum) -> new Alliance(rs.getLong(1), rs.getString(2)))
        .build(dataSource);
  }

  @Bean(name = "updateAllianceAndInsertGuildItemWriter")
  public CompositeItemWriter<WrapperAllianceGuilds> updateAllianceAndInsertGuildItemWriter(DataSource dataSource) {
    return new AllianceGuildCompositeItemWriterProvider().get(dataSource);
  }

  /**
   * Step two responsable to fetch guild_daily data.
   * If needed, It will update guild alliance relationship.
   * If needed, It will insert invalid alliance.
   */
  @Bean(name = "fetchGuildDailyAndInvalidAlliances")
  public Step fetchGuildDailyAndInvalidAlliances(
      @Qualifier("allGuildsItemReader") ItemReader<Guild> reader,
      FetchGuildDailyAndInvalidAllianceItemProcessor processor,
      @Qualifier("insertInvalidAllianceAndInsertGuildDaily") CompositeItemWriter<WrapperInvalidAllianceGuildDaily> writer) {

    return stepBuilderFactory.get("Fetch guild_daily, insert invalid alliance and update guild alliance")
        .<Guild, WrapperInvalidAllianceGuildDaily>chunk(10) 
        .reader(reader)
        .processor(processor)
        .writer(writer)
        .taskExecutor(threadPoolTaskExecutor())
        .throttleLimit(10)
        .build();
  } 

  @Bean(name = "allGuildsItemReader")
  public JdbcPagingItemReader<Guild> allGuildsItemReader (DataSource dataSource) {
    return new JdbcPagingItemReaderProviderBuilder<Guild>()
        .name("All guilds item reader")
        .selectClause("select id, albion_id, alliance_albion_id")
        .fromClause("from guild")
        .sortKey("id")
        .rowMapper((rs, rowNum)
            -> new Guild(rs.getLong(1), rs.getString(2), rs.getString(3)))
        .build(dataSource);
  }

  @Bean("insertInvalidAllianceAndInsertGuildDaily")
  public CompositeItemWriter<WrapperInvalidAllianceGuildDaily> compositeItemWriter (DataSource dataSource) {
    return new GuildDailyCompositeItemWriterProvider().get(dataSource);
  }

  /**
   * Step three responsable to insert alliance_daily data.
   */
  @Bean(name = "insertGuildDaily")
  public Step insertGuildDaily(
      @Qualifier("allAlliancesItemReader") ItemReader<Alliance> reader,
      BuildAlliancesDailyItemProcessor processor,
      @Qualifier("insertAlliancesDailyItemWriter") ItemWriter<AllianceDaily> writer) {

    return stepBuilderFactory.get("Fetch guild_daily, insert invalid alliance and update guild alliance")
        .<Alliance, AllianceDaily>chunk(20)
        .reader(reader)
        .processor(processor)
        .writer(writer)
        .build();
  } 

  @Bean(name = "allAlliancesItemReader") 
  public ItemReader<Alliance> allAlliancesItemReader (DataSource dataSource) {
    return new JdbcPagingItemReaderProviderBuilder<Alliance>()
        .name("All alliances item reader")
        .selectClause("select id, albion_id")
        .fromClause("from alliance")
        .sortKey("id")
        .rowMapper((rs, rowNum) -> new Alliance(rs.getLong(1), rs.getString(2)))
        .build(dataSource);
  }

  @Bean(name = "insertAlliancesDailyItemWriter")
  public ItemWriter<AllianceDaily> insertAlliancesDailyItemWriter(DataSource dataSource) {
    String insert = " INSERT INTO alliance_daily (date, alliance_id, guildcount, membercount) ";
    String values = " VALUES (:date, :allianceId, :guildCount, :memberCount)";
    String sql = insert + values;

    JdbcBatchItemWriter<AllianceDaily> jdbcBatchItemWriter = new JdbcBatchItemWriterBuilder<AllianceDaily>()
      .beanMapped()
      .dataSource(dataSource)
      .sql(sql)
      .build();
    jdbcBatchItemWriter.afterPropertiesSet();
    return jdbcBatchItemWriter;
  }
}
