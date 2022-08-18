package com.hugarty.albionsite.job.item.cleanstep.allianceremoval;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;

import com.hugarty.albionsite.job.item.JdbcBatchItemWritterBeanMappedProvider;
import com.hugarty.albionsite.job.model.clean.WrapperAllianceRemoval;

public class CompositeAllianceRemovalItemWriterProvider {

  public CompositeItemWriter<WrapperAllianceRemoval> get(DataSource dataSource) {
    return new CompositeItemWriterBuilder<WrapperAllianceRemoval>()
        .delegates(
          deleteAlliancesDaily(dataSource),
          deleteAlliancesWeekly(dataSource),
          deleteGuildsDaily(dataSource),
          deleteGuilds(dataSource),
          deleteAlliance(dataSource)
        )
        .build();
  }
  
  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteAlliancesDaily (DataSource dataSource) {
    String sql = "DELETE FROM alliance_daily where id IN (:idsAlliancesDaily) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }

  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteAlliancesWeekly (DataSource dataSource) {
    String sql = "DELETE FROM alliance_weekly where id IN (:idsAlliancesWeekly) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }

  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteGuildsDaily (DataSource dataSource) {
    String sql = "DELETE FROM guild_daily where id IN (:idsGuildsDaily) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }

  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteGuilds (DataSource dataSource) {
    String sql = "DELETE FROM guild where id IN (:idsGuilds) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }

  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteAlliance (DataSource dataSource) {
    String sql = "DELETE FROM alliance where id IN (:idsAlliances) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }
}

