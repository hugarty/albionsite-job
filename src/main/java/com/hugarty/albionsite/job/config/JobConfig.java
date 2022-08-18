package com.hugarty.albionsite.job.config;

import java.security.InvalidParameterException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.RestTemplate;

import com.hugarty.albionsite.job.item.JdbcBatchItemWritterBeanMappedProvider;
import com.hugarty.albionsite.job.item.JdbcPagingItemReaderProviderBuilder;
import com.hugarty.albionsite.job.item.cleanstep.HerokuLimitsConstants;
import com.hugarty.albionsite.job.item.cleanstep.allianceremoval.AllianceRemovalItemProcessor;
import com.hugarty.albionsite.job.item.cleanstep.allianceremoval.CompositeAllianceRemovalItemWriterProvider;
import com.hugarty.albionsite.job.item.cleanstep.batchremoval.BatchRemovalItemProcessor;
import com.hugarty.albionsite.job.item.cleanstep.batchremoval.CompositeBatchRemovalItemWriterProvider;
import com.hugarty.albionsite.job.item.cleanstep.genericremoval.GenericRemovalItemProcessor;
import com.hugarty.albionsite.job.item.cleanstep.guildremoval.CompositeGuildRemovalItemWriterProvider;
import com.hugarty.albionsite.job.item.cleanstep.guildremoval.GuildRemovalItemProcessor;
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
import com.hugarty.albionsite.job.model.clean.WrapperGenericRemoval;
import com.hugarty.albionsite.job.model.clean.WrapperAllianceRemoval;
import com.hugarty.albionsite.job.model.clean.WrapperBatchRemoval;


// TODO quebrar esse configuration em vários configurations diferentes. Um para cada JOB ? STEP? 
// TODO quebrar esse configuration em vários configurations diferentes. Um para cada JOB ? STEP? 
// TODO quebrar esse configuration em vários configurations diferentes. Um para cada JOB ? STEP? 

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

  /**
   * This Job only exists because heroku free postgres has a 10k lines limit.
   * {@link #cleanDailyJob} delete old data from database.
   */
  @Bean("cleanDailyJob")
  public Job cleanDailyJob(
      @Qualifier("deleteOlderBatchDataFromDatabase") Step deleteOlderBatchDataFromDatabase,
      @Qualifier("deleteAlliancesOverLimitFromDatabase") Step deleteAlliancesOverLimitFromDatabase) {
    return jobBuilderFactory.get("Clean database, batch tables and older data")
        .start(deleteOlderBatchDataFromDatabase)
        .next(deleteAlliancesOverLimitFromDatabase)
        .build();
  }

  @Bean(name = "deleteOlderBatchDataFromDatabase")
  public TaskletStep deleteOlderBatchDataFromDatabase(
      @Qualifier("jobExecutionsBeforeTodayItemReader") JdbcPagingItemReader<WrapperBatchRemoval> reader,
      BatchRemovalItemProcessor batchRemovalItemProcessor,
      @Qualifier("batchRemovalItemWriter") CompositeItemWriter<WrapperBatchRemoval> batchRemovalItemWriter) {

    return stepBuilderFactory.get("Delete older batch lines")
        .<WrapperBatchRemoval, WrapperBatchRemoval>chunk(20)
        .reader(reader)
        .processor(batchRemovalItemProcessor)
        .writer(batchRemovalItemWriter)
        .build();
  }

  @Bean(name = "jobExecutionsBeforeTodayItemReader")
  @StepScope // It's needed to recalculate dateBatchSearch
  public JdbcPagingItemReader<WrapperBatchRemoval> jobExecutionsBeforeTodayItemReader(DataSource dataSource) {
    LocalDate now = LocalDate.now();
    LocalDateTime dateBatchSearch = LocalDateTime.of(now.getYear(), now.getMonth(), now.getDayOfMonth(), 0, 0, 0);

    return new JdbcPagingItemReaderProviderBuilder<WrapperBatchRemoval>()
        .name("batch lines to be deleted")
        .selectClause("select job_execution_id")
        .fromClause("from batch_job_execution")


        // TODO ME DELETA SÓ PARA TESTAR
        // TODO ME DELETA SÓ PARA TESTAR
        // TODO ME DELETA SÓ PARA TESTAR
        .whereClause("where job_execution_id = 3")

        // TODO me coloca de novo
        // TODO me coloca de novo
        // TODO me coloca de novo
        // .whereClause("where create_time < " + dateBatchSearch.toString())
        
        .sortKey("job_execution_id")
        .rowMapper((rs, rowNum) -> new WrapperBatchRemoval(rs.getLong(1)))
        .build(dataSource);
  }

  @Bean(name = "batchRemovalItemWriter")
  public CompositeItemWriter<WrapperBatchRemoval> batchRemovalItemWriter(DataSource dataSource) {
    return new CompositeBatchRemovalItemWriterProvider().get(dataSource);
  }

  //----------------

  @Bean(name = "deleteAlliancesOverLimitFromDatabase")
  public TaskletStep deleteAlliancesOverLimitFromDatabase(
      AllianceRemovalItemProcessor itemProcessor,
      @Qualifier("allianceRemovalItemWriter") CompositeItemWriter<WrapperAllianceRemoval> writer, 
      DataSource dataSource) {

    JdbcPagingItemReader<Long> reader = countAllLinesFromItemReader("alliance", dataSource);
    return stepBuilderFactory.get("Remove alliances if table alliance are above limit")
        .<Long, WrapperAllianceRemoval>chunk(20)
        .reader(reader)
        .processor(itemProcessor)
        .writer(writer)
        .build();
  }

  @Bean(name = "allianceRemovalItemWriter")
  public CompositeItemWriter<WrapperAllianceRemoval> allianceRemovalItemWriter(DataSource dataSource) {
    return new CompositeAllianceRemovalItemWriterProvider().get(dataSource);
  }

  // ---------

  @Bean(name = "deleteAlliancesDailyOverLimitFromDatabase")
  public TaskletStep deleteAlliancesDailyOverLimitFromDatabase(NamedParameterJdbcTemplate namedParameterJdbcTemplate,
      DataSource dataSource) {

    return getTaskletDeleteOlderData(HerokuLimitsConstants.ALLIANCE_DAILY, namedParameterJdbcTemplate, dataSource);
  }

  //--------------

  @Bean(name = "deleteAlliancesWeeklyOverLimitFromDatabase")
  public TaskletStep deleteAlliancesWeeklyOverLimitFromDatabase(NamedParameterJdbcTemplate namedParameterJdbcTemplate, 
      DataSource dataSource) {
    return getTaskletDeleteOlderData(HerokuLimitsConstants.ALLIANCE_WEEKLY, namedParameterJdbcTemplate, dataSource);
  }

  //--------------

  @Bean(name = "deleteGuildDailyOverLimitFromDatabase")
  public TaskletStep deleteGuildDailyOverLimitFromDatabase(NamedParameterJdbcTemplate namedParameterJdbcTemplate, 
      DataSource dataSource) {
    return getTaskletDeleteOlderData(HerokuLimitsConstants.GUILD_DAILY, namedParameterJdbcTemplate, dataSource);
  }

  //--------------

  public TaskletStep getTaskletDeleteOlderData(HerokuLimitsConstants herokuLimitsConstants, 
      NamedParameterJdbcTemplate namedParameterJdbcTemplate, DataSource dataSource) {
    String stepName = "Remove older "+herokuLimitsConstants.getTableName()+ " if table "+herokuLimitsConstants.getTableName()+ " are above limit";
    String writerSQL = "DELETE FROM "+herokuLimitsConstants.getTableName()+" where id IN (:ids) ";

    return stepBuilderFactory.get(stepName)
        .<Long, WrapperGenericRemoval>chunk(20)
        .reader(countAllLinesFromItemReader(herokuLimitsConstants.getTableName(), dataSource))
        .processor(new GenericRemovalItemProcessor(namedParameterJdbcTemplate, herokuLimitsConstants))
        .writer(JdbcBatchItemWritterBeanMappedProvider.<WrapperGenericRemoval>get(dataSource, writerSQL))
        .build();
  }

  //--------------

  @Bean(name = "deleteGuildsOverLimitFromDatabase")
  public TaskletStep deleteGuildsOverLimitFromDatabase(
      GuildRemovalItemProcessor itemProcessor,
      @Qualifier("guildRemovalItemWriter") CompositeItemWriter<WrapperAllianceRemoval> writer, 
      DataSource dataSource) {

    JdbcPagingItemReader<Long> reader = countAllLinesFromItemReader("guild", dataSource);

    return stepBuilderFactory.get("Remove guilds if table guild are above limit")
        .<Long, WrapperAllianceRemoval>chunk(20)
        .reader(reader)
        .processor(itemProcessor)
        .writer(writer)
        .build();
  }

  @Bean(name = "guildRemovalItemWriter")
  public CompositeItemWriter<WrapperAllianceRemoval> guildRemovalItemWriter(DataSource dataSource) {
    return new CompositeGuildRemovalItemWriterProvider().get(dataSource);
  }

  //--------------

  private JdbcPagingItemReader<Long> countAllLinesFromItemReader(String tableName, DataSource dataSource) {
    Map<String, Order> sortMap = new HashMap<>();
    sortMap.put("count", Order.ASCENDING);

    JdbcPagingItemReader<Long> reader = new JdbcPagingItemReaderBuilder<Long>()
      .name("Count total lines in "+ tableName + " table")
      .dataSource(dataSource)
      .pageSize(1)
      .maxItemCount(1)
      .selectClause("select count(1) as count")
      .fromClause("from "+ tableName)
      .sortKeys(sortMap)
      .rowMapper((rs, rowNum) -> rs.getLong(1))
      .build();

    try {
      reader.afterPropertiesSet();
    } catch (Exception e) {
      throw new InvalidParameterException("Was not possible create itemReader in " + tableName);
    }
    return reader;
  }

  //--------------

  /**
   * The {@link #dailyJob} collect data from Albion API daily.
   */
  @Bean("dailyJob")
  public Job dailyJob(@Qualifier("fixInvalidAlliancesAddNewGuilds") Step fixInvalidAlliancesAddNewGuilds, 
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

  // TODO - Para criar um Batch Reloadable é necessário recuperar as guildas que não tem guild_daily hoje. OUTER JOIN ?
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

    return stepBuilderFactory.get("Insert guild daily information")
        .<Alliance, AllianceDaily>chunk(20)
        .reader(reader)
        .processor(processor)
        .writer(writer)
        .build();
  } 

  // TODO - Para criar um Batch Reloadable é necessário recuperar as alliances que não tem alliance_daily hoje. OUTER JOIN ?
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

    return JdbcBatchItemWritterBeanMappedProvider.<AllianceDaily>get(dataSource, sql);
  }
}
