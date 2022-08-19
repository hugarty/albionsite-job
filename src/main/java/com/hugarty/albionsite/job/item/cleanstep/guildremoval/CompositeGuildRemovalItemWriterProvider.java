package com.hugarty.albionsite.job.item.cleanstep.guildremoval;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;

import com.hugarty.albionsite.job.item.JdbcBatchItemWritterBeanMappedProvider;
import com.hugarty.albionsite.job.model.clean.WrapperAllianceRemoval;

public class CompositeGuildRemovalItemWriterProvider {

  private DataSource dataSource;

  public CompositeGuildRemovalItemWriterProvider(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public CompositeItemWriter<WrapperAllianceRemoval> get() {
    return new CompositeItemWriterBuilder<WrapperAllianceRemoval>()
        .delegates(
          deleteGuildsDaily(),
          deleteGuilds()
        )
        .build();
  }

  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteGuildsDaily () {
    String sql = "DELETE FROM guild_daily where id IN (:idsGuildsDaily) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }

  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteGuilds () {
    String sql = "DELETE FROM guild where id IN (:idsGuilds) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }

}

