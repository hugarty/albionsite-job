package com.hugarty.albionsite.job.item.stepone;

import javax.sql.DataSource;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.hugarty.albionsite.job.item.JdbcPagingItemReaderProviderBuilder;
import com.hugarty.albionsite.job.model.Alliance;
import com.hugarty.albionsite.job.model.WrapperAllianceGuilds;

/**
 * Step one responsable to fetch alliance data for invalid alliances.
 * If needed, It will insert new guilds.
 */
public class FixInvalidAlliancesAddNewGuildsStepProvider {

  private final StepBuilderFactory stepBuilderFactory;
  private final ThreadPoolTaskExecutor threadPoolTaskExecutor;
  private final DataSource dataSource;
  private final FetchAlliancesAndDetachedGuildsItemProcessor fetchAlliancesAndDetachedGuildsItemProcessor;

  public FixInvalidAlliancesAddNewGuildsStepProvider(
      DataSource dataSource,
      StepBuilderFactory stepBuilderFactory,
      ThreadPoolTaskExecutor threadPoolTaskExecutor,
      FetchAlliancesAndDetachedGuildsItemProcessor processor) {
    this.dataSource = dataSource;
    this.stepBuilderFactory = stepBuilderFactory;
    this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    this.fetchAlliancesAndDetachedGuildsItemProcessor = processor;
  }

  public Step get() {
    return stepBuilderFactory.get("Fix invalid alliances and add their new guilds")
        .<Alliance, WrapperAllianceGuilds>chunk(10)
        .reader(invalidAlliancesItemReader())
        .processor(fetchAlliancesAndDetachedGuildsItemProcessor)
        .writer(new AllianceGuildCompositeItemWriterProvider(dataSource).get())
        .taskExecutor(threadPoolTaskExecutor)
        .build();
  }

  private JdbcPagingItemReader<Alliance> invalidAlliancesItemReader() {
    return new JdbcPagingItemReaderProviderBuilder<Alliance>()
      .name("invalid alliances item reader")
      .selectClause("select id, albion_id")
      .fromClause("from alliance")
      .whereClause("where alliance.name is null")
      .sortKey("id")
      .rowMapper((rs, rowNum) -> new Alliance(rs.getLong(1), rs.getString(2)))
      .build(dataSource);
  }

}