package com.hugarty.albionsite.job.item.steptwo;

import javax.sql.DataSource;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.hugarty.albionsite.job.item.JdbcPagingItemReaderProviderBuilder;
import com.hugarty.albionsite.job.model.Guild;
import com.hugarty.albionsite.job.model.WrapperInvalidAllianceGuildDaily;

/**
 * Step two responsable to fetch guild_daily data.
 * If needed, It will update guild alliance relationship.
 * If needed, It will insert invalid alliance.
 */
public class FetchGuildDailyAndInvalidAlliancesStepProvider {

  private final StepBuilderFactory stepBuilderFactory;
  private final ThreadPoolTaskExecutor threadPoolTaskExecutor;
  private final DataSource dataSource;
  private final FetchGuildDailyAndInvalidAllianceItemProcessor fetchGuildDailyAndInvalidAllianceItemProcessor;

  public FetchGuildDailyAndInvalidAlliancesStepProvider(
      DataSource dataSource,
      StepBuilderFactory stepBuilderFactory,
      ThreadPoolTaskExecutor threadPoolTaskExecutor,
      FetchGuildDailyAndInvalidAllianceItemProcessor processor) {
    this.dataSource = dataSource;
    this.stepBuilderFactory = stepBuilderFactory;
    this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    this.fetchGuildDailyAndInvalidAllianceItemProcessor = processor;
  }

  public Step get() {
    return stepBuilderFactory.get("Fetch guild_daily, insert invalid alliance and update guild alliance")
        .<Guild, WrapperInvalidAllianceGuildDaily>chunk(10)
        .reader(allGuildsItemReader())
        .processor(fetchGuildDailyAndInvalidAllianceItemProcessor)
        .writer(new GuildDailyCompositeItemWriterProvider(dataSource).get())
        .taskExecutor(threadPoolTaskExecutor)
        .throttleLimit(10)
        .build();
  }

  // TODO - Para criar um Batch Reloadable é necessário recuperar as guildas que não tem guild_daily hoje. OUTER JOIN ?
  private JdbcPagingItemReader<Guild> allGuildsItemReader() {
    return new JdbcPagingItemReaderProviderBuilder<Guild>()
        .name("All guilds item reader")
        .selectClause("select id, albion_id, alliance_albion_id")
        .fromClause("from guild")
        .sortKey("id")
        .rowMapper((rs, rowNum) -> new Guild(rs.getLong(1), rs.getString(2), rs.getString(3)))
        .build(dataSource);
  }

}
