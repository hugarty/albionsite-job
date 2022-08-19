package com.hugarty.albionsite.job.item.cleanstep.allianceremoval;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;

import com.hugarty.albionsite.job.item.JdbcBatchItemWritterBeanMappedProvider;
import com.hugarty.albionsite.job.model.clean.WrapperAllianceRemoval;

public class CompositeAllianceRemovalItemWriterProvider {

  DataSource dataSource;

  public CompositeAllianceRemovalItemWriterProvider(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public CompositeItemWriter<WrapperAllianceRemoval> get() {
    return new CompositeItemWriterBuilder<WrapperAllianceRemoval>()
        .delegates(
          deleteAlliancesDaily(),
          deleteAlliancesWeekly(),
          deleteGuildsDaily(),
          deleteGuilds(),
          deleteAlliance()
        )
        .build();
  }
  
  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteAlliancesDaily () {
    String sql = "DELETE FROM alliance_daily where id IN (:idsAlliancesDaily) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }

  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteAlliancesWeekly () {
    String sql = "DELETE FROM alliance_weekly where id IN (:idsAlliancesWeekly) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }

  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteGuildsDaily () {
    String sql = "DELETE FROM guild_daily where id IN (:idsGuildsDaily) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }

  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteGuilds () {
    String sql = "DELETE FROM guild where id IN (:idsGuilds) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }

  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteAlliance () {
    String sql = "DELETE FROM alliance where id IN (:idsAlliances) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }
}

