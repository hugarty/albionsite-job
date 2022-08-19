package com.hugarty.albionsite.job.item.stepthree;

import javax.sql.DataSource;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.hugarty.albionsite.job.item.JdbcBatchItemWritterBeanMappedProvider;
import com.hugarty.albionsite.job.item.JdbcPagingItemReaderProviderBuilder;
import com.hugarty.albionsite.job.model.Alliance;
import com.hugarty.albionsite.job.model.AllianceDaily;

public class InsertAllianceDailyStepProvider {
  
  private final StepBuilderFactory stepBuilderFactory;
  private final DataSource dataSource;
  private final BuildAlliancesDailyItemProcessor buildAlliancesDailyItemProcessor;
  
  public InsertAllianceDailyStepProvider (
      DataSource dataSource,
      StepBuilderFactory stepBuilderFactory,
      BuildAlliancesDailyItemProcessor processor) {
    this.dataSource = dataSource;
    this.stepBuilderFactory = stepBuilderFactory;
    this.buildAlliancesDailyItemProcessor = processor;
  }

  public Step get(){
    return stepBuilderFactory.get("Insert guild daily data")
        .<Alliance, AllianceDaily>chunk(20)
        .reader(allAlliancesItemReader())
        .processor(buildAlliancesDailyItemProcessor)
        .writer(insertAlliancesDailyItemWriter())
        .build();
  }

  // TODO - Para criar um Batch Reloadable é necessário recuperar as alliances que não tem alliance_daily hoje. OUTER JOIN ?
  private ItemReader<Alliance> allAlliancesItemReader() {
    return new JdbcPagingItemReaderProviderBuilder<Alliance>()
        .name("All alliances item reader")
        .selectClause("select id, albion_id")
        .fromClause("from alliance")
        .sortKey("id")
        .rowMapper((rs, rowNum) -> new Alliance(rs.getLong(1), rs.getString(2)))
        .build(dataSource);

  }

  private ItemWriter<AllianceDaily> insertAlliancesDailyItemWriter() {
    String insert = " INSERT INTO alliance_daily (date, alliance_id, guildcount, membercount) ";
    String values = " VALUES (:date, :allianceId, :guildCount, :memberCount)";
    String sql = insert + values;

    return JdbcBatchItemWritterBeanMappedProvider.<AllianceDaily>get(dataSource, sql);
  }
}
