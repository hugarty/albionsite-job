package com.hugarty.albionsite.job.config;

import java.security.InvalidParameterException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.RestTemplate;

import com.hugarty.albionsite.job.item.JdbcBatchItemWritterBeanMappedProvider;
import com.hugarty.albionsite.job.item.JdbcPagingItemReaderProviderBuilder;
import com.hugarty.albionsite.job.item.cleanstep.HerokuLimitsConstants;
import com.hugarty.albionsite.job.item.cleanstep.allianceremoval.AllianceRemovalItemProcessor;
import com.hugarty.albionsite.job.item.cleanstep.allianceremoval.DeleteAlliancesOverLimitFromDatabaseStepProvider;
import com.hugarty.albionsite.job.item.cleanstep.batchremoval.BatchRemovalItemProcessor;
import com.hugarty.albionsite.job.item.cleanstep.batchremoval.CompositeBatchRemovalItemWriterProvider;
import com.hugarty.albionsite.job.item.cleanstep.genericremoval.GenericRemovalItemProcessor;
import com.hugarty.albionsite.job.item.cleanstep.guildremoval.DeleteGuildsOverLimitFromDatabaseStepProvider;
import com.hugarty.albionsite.job.item.cleanstep.guildremoval.GuildRemovalItemProcessor;
import com.hugarty.albionsite.job.item.stepone.FetchAlliancesAndDetachedGuildsItemProcessor;
import com.hugarty.albionsite.job.item.stepone.FixInvalidAlliancesAddNewGuildsStepProvider;
import com.hugarty.albionsite.job.item.stepthree.BuildAlliancesDailyItemProcessor;
import com.hugarty.albionsite.job.item.stepthree.InsertAllianceDailyStepProvider;
import com.hugarty.albionsite.job.item.steptwo.FetchGuildDailyAndInvalidAllianceItemProcessor;
import com.hugarty.albionsite.job.item.steptwo.FetchGuildDailyAndInvalidAlliancesStepProvider;
import com.hugarty.albionsite.job.model.clean.WrapperBatchRemoval;
import com.hugarty.albionsite.job.model.clean.WrapperGenericRemoval;


@Configuration
@EnableRetry
public class JobConfig {

  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;
  private final DataSource dataSource;

  public JobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
    this.dataSource = dataSource;
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

  private ThreadPoolTaskExecutor threadPoolTaskExecutor() {
    ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
    taskExecutor.setMaxPoolSize(10);
    taskExecutor.setQueueCapacity(10);
    taskExecutor.setCorePoolSize(10);
    taskExecutor.setPrestartAllCoreThreads(true);
    taskExecutor.afterPropertiesSet();
    return taskExecutor;
  }

  /**
   * Step one responsable to fetch alliance data for invalid alliances.
   * If needed, It will insert new guilds.
   */
  @Bean(name = "fixInvalidAlliancesAddNewGuilds")
  public Step fixInvalidAlliancesAddNewGuilds(FetchAlliancesAndDetachedGuildsItemProcessor processor) {

    return new FixInvalidAlliancesAddNewGuildsStepProvider(dataSource, stepBuilderFactory, threadPoolTaskExecutor(),
        processor).get();
  }

  /**
   * Step two responsable to fetch guild_daily data.
   * If needed, It will update guild alliance relationship.
   * If needed, It will insert invalid alliance.
   */
  @Bean(name = "fetchGuildDailyAndInvalidAlliances")
  public Step fetchGuildDailyAndInvalidAlliances(FetchGuildDailyAndInvalidAllianceItemProcessor processor) {
    return new FetchGuildDailyAndInvalidAlliancesStepProvider(dataSource, stepBuilderFactory, threadPoolTaskExecutor(),
      processor).get();
  }

  /**
   * Step three responsable to insert alliance_daily data.
   */
  @Bean(name = "insertAllianceDaily")
  public Step insertAllianceDaily(BuildAlliancesDailyItemProcessor processor) {
    return new InsertAllianceDailyStepProvider(dataSource, stepBuilderFactory, processor).get();
  }

  /**
   * The {@link #dailyJob} collect data from Albion API daily.
   */
  @Bean("dailyJob")
  public Job dailyJob(@Qualifier("fixInvalidAlliancesAddNewGuilds") Step fixInvalidAlliancesAddNewGuilds,
      @Qualifier("fetchGuildDailyAndInvalidAlliances") Step fetchGuildDailyAndInvalidAlliances,
      @Qualifier("insertAllianceDaily") Step insertAllianceDaily,
      @Qualifier("stepCleanDailyJob") Step stepCleanDailyJob) {

    return jobBuilderFactory.get("Fetch alliances and guilds to populate database")
        .start(fixInvalidAlliancesAddNewGuilds)
        .next(fetchGuildDailyAndInvalidAlliances)
        .next(insertAllianceDaily)
        .next(stepCleanDailyJob)
        .build();
  }

  // --------------

  @Bean(name = "stepCleanDailyJob")
  public Step stepCleanDailyJob(
    @Qualifier("cleanDailyJob") Job cleanDailyJob,
    JobLauncher jobLauncher) {
    return this.stepBuilderFactory.get("Step cleanDailyJob")
          .job(cleanDailyJob)
          .launcher(jobLauncher)
          .build();
  }
  
  /**
   * This Job only exists because heroku free postgres has a 10k lines limit.
   * {@link #cleanDailyJob} delete old data from database.
   */
  @Bean(name = "cleanDailyJob")
  public Job cleanDailyJob(
      @Qualifier("deleteOlderBatchDataFromDatabase") Step deleteOlderBatchDataFromDatabase,
      @Qualifier("deleteAlliancesOverLimitFromDatabase") Step deleteAlliancesOverLimitFromDatabase,
      @Qualifier("deleteGuildsOverLimitFromDatabase") Step deleteGuildsOverLimitFromDatabase,
      @Qualifier("deleteAlliancesDailyOverLimitFromDatabase") Step deleteAlliancesDailyOverLimitFromDatabase,
      @Qualifier("deleteAlliancesWeeklyOverLimitFromDatabase") Step deleteAlliancesWeeklyOverLimitFromDatabase,
      @Qualifier("deleteGuildDailyOverLimitFromDatabase") Step deleteGuildDailyOverLimitFromDatabase
      
      ) {
    return jobBuilderFactory.get("Clean database, batch tables and older data")
        .start(deleteOlderBatchDataFromDatabase)
        .next(deleteAlliancesOverLimitFromDatabase)
        .next(deleteGuildsOverLimitFromDatabase)
        .next(deleteAlliancesDailyOverLimitFromDatabase)
        .next(deleteAlliancesWeeklyOverLimitFromDatabase)
        .next(deleteGuildDailyOverLimitFromDatabase)
        .build();
  }

  @Bean(name = "deleteOlderBatchDataFromDatabase")
  public TaskletStep deleteOlderBatchDataFromDatabase(
      @Qualifier("jobExecutionsBeforeTodayItemReader") JdbcPagingItemReader<WrapperBatchRemoval> reader,
      BatchRemovalItemProcessor batchRemovalItemProcessor,
      DataSource dataSource) {

    return stepBuilderFactory.get("Delete older batch lines")
        .<WrapperBatchRemoval, WrapperBatchRemoval>chunk(20)
        .reader(reader)
        .processor(batchRemovalItemProcessor)
        .writer(new CompositeBatchRemovalItemWriterProvider(dataSource).get())
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
        .whereClause("where create_time < '" + dateBatchSearch.toString() + "'")
        .sortKey("job_execution_id")
        .rowMapper((rs, rowNum) -> new WrapperBatchRemoval(rs.getLong(1)))
        .build(dataSource);
  }

  @Bean(name = "deleteAlliancesOverLimitFromDatabase")
  public TaskletStep deleteAlliancesOverLimitFromDatabase(
      AllianceRemovalItemProcessor itemProcessor, DataSource dataSource) {
    JdbcPagingItemReader<Long> itemReader = countAllLinesFromItemReader("alliance");
    return new DeleteAlliancesOverLimitFromDatabaseStepProvider(dataSource, stepBuilderFactory, itemReader,
        itemProcessor)
        .get();
  }

  @Bean(name = "deleteGuildsOverLimitFromDatabase")
  public TaskletStep deleteGuildsOverLimitFromDatabase(GuildRemovalItemProcessor itemProcessor) {
    JdbcPagingItemReader<Long> reader = countAllLinesFromItemReader("guild");
    return new DeleteGuildsOverLimitFromDatabaseStepProvider(dataSource, stepBuilderFactory, reader, itemProcessor)
      .get();
  }

  @Bean(name = "deleteAlliancesDailyOverLimitFromDatabase")
  public TaskletStep deleteAlliancesDailyOverLimitFromDatabase(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    return getTaskletDeleteOlderData(HerokuLimitsConstants.ALLIANCE_DAILY, namedParameterJdbcTemplate);
  }

  @Bean(name = "deleteAlliancesWeeklyOverLimitFromDatabase")
  public TaskletStep deleteAlliancesWeeklyOverLimitFromDatabase(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    return getTaskletDeleteOlderData(HerokuLimitsConstants.ALLIANCE_WEEKLY, namedParameterJdbcTemplate);
  }

  @Bean(name = "deleteGuildDailyOverLimitFromDatabase")
  public TaskletStep deleteGuildDailyOverLimitFromDatabase(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    return getTaskletDeleteOlderData(HerokuLimitsConstants.GUILD_DAILY, namedParameterJdbcTemplate);
  }

  private TaskletStep getTaskletDeleteOlderData(HerokuLimitsConstants herokuLimitsConstants, NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    String tableName = herokuLimitsConstants.getTableName();
    String stepName = "Remove older " + tableName + " if table " + tableName + " are above limit";
    String writerSQL = " DELETE FROM " + tableName + " where id IN (:ids) ";

    return stepBuilderFactory.get(stepName)
        .<Long, WrapperGenericRemoval>chunk(20)
        .reader(countAllLinesFromItemReader(tableName))
        .processor(new GenericRemovalItemProcessor(namedParameterJdbcTemplate, herokuLimitsConstants))
        .writer(JdbcBatchItemWritterBeanMappedProvider.<WrapperGenericRemoval>get(dataSource, writerSQL))
        .build();
  }

  private JdbcPagingItemReader<Long> countAllLinesFromItemReader(String tableName) {
    Map<String, Order> sortMap = new HashMap<>();
    sortMap.put("count", Order.ASCENDING);

    JdbcPagingItemReader<Long> reader = new JdbcPagingItemReaderBuilder<Long>()
        .name("Count total lines in " + tableName + " table")
        .dataSource(dataSource)
        .pageSize(1)
        .maxItemCount(1)
        .selectClause("select count(1) as count")
        .fromClause("from " + tableName)
        .sortKeys(sortMap)
        .rowMapper((rs, rowNum) -> rs.getLong(1))
        .build();

    try {
      reader.afterPropertiesSet();
    } catch (Exception e) {
      throw new InvalidParameterException("Was not possible create itemReader for " + tableName);
    }
    return reader;
  }

}
