package com.hugarty.albionsite.job.item.cleanstep.guildremoval;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;

import com.hugarty.albionsite.job.item.JdbcBatchItemWritterBeanMappedProvider;
import com.hugarty.albionsite.job.model.clean.WrapperAllianceRemoval;

public class CompositeGuildRemovalItemWriterProvider {

  public CompositeItemWriter<WrapperAllianceRemoval> get(DataSource dataSource) {
    return new CompositeItemWriterBuilder<WrapperAllianceRemoval>()
        .delegates(
          deleteGuildsDaily(dataSource),
          deleteGuilds(dataSource)
        )
        .build();
  }


  // TODO esse método está "duplicado"
  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteGuildsDaily (DataSource dataSource) {
    String sql = "DELETE FROM guild_daily where id IN (:idsGuildsDaily) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }

  // TODO esse método está "duplicado"
  private JdbcBatchItemWriter<WrapperAllianceRemoval> deleteGuilds (DataSource dataSource) {
    String sql = "DELETE FROM guild where id IN (:idsGuilds) ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperAllianceRemoval>get(dataSource, sql);
  }

}

