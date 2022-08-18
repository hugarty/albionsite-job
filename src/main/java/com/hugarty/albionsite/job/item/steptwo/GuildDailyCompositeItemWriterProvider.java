package com.hugarty.albionsite.job.item.steptwo;

import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;

import com.hugarty.albionsite.job.item.JdbcBatchItemWriterMapper;
import com.hugarty.albionsite.job.model.Alliance;
import com.hugarty.albionsite.job.model.Guild;
import com.hugarty.albionsite.job.model.GuildDaily;
import com.hugarty.albionsite.job.model.WrapperInvalidAllianceGuildDaily;


public class GuildDailyCompositeItemWriterProvider {
  
  public CompositeItemWriter<WrapperInvalidAllianceGuildDaily> get(DataSource dataSource) {
    return new CompositeItemWriterBuilder<WrapperInvalidAllianceGuildDaily>()
        .delegates(
            insertInvalidAllianceItemWriter(dataSource),
            insertGuildDailyItemWriter(dataSource),
            updateGuildAllianceRelationship(dataSource))
        .build();
  }

  private ItemWriter<WrapperInvalidAllianceGuildDaily> insertInvalidAllianceItemWriter(DataSource dataSource) {
    return new JdbcBatchItemWriterMapper.Builder<WrapperInvalidAllianceGuildDaily, Alliance>()
        .dataSource(dataSource)
        .mapper(items -> items.stream()
            .filter(wrapper -> wrapper.getInvalidAlliance().isPresent())
            .map(wrapper -> wrapper.getInvalidAlliance().get())
            .collect(Collectors.toList()))
        .sql("INSERT INTO alliance (albion_id) VALUES (:albionId)")
        .build();
  }

  private ItemWriter<WrapperInvalidAllianceGuildDaily> insertGuildDailyItemWriter(DataSource dataSource) {
    String insert = " INSERT INTO guild_daily (date, guild_id, fame, killfame, deathfame, gvgkills, gvgdeaths, kills, deaths, ratio, membercount) ";
    String values = " VALUES (:date, :guildId, :fame, :killFame, :deathFame, :gvgKills, :gvgDeaths, :kills, :deaths, :ratio, :memberCount)";
    String sql = insert + values;

    return new JdbcBatchItemWriterMapper.Builder<WrapperInvalidAllianceGuildDaily, GuildDaily>()
        .dataSource(dataSource)
        .mapper(items -> items.stream()
            .map(WrapperInvalidAllianceGuildDaily::getGuildDaily)
            .collect(Collectors.toList()))
        .sql(sql)
        .build();
  }

  private ItemWriter<WrapperInvalidAllianceGuildDaily> updateGuildAllianceRelationship(DataSource dataSource) {
    return new JdbcBatchItemWriterMapper.Builder<WrapperInvalidAllianceGuildDaily, Guild>()
        .dataSource(dataSource)
        .mapper(items -> items.stream()
            .filter(wrapper -> wrapper.getGuildWithNewAlliance().isPresent())
            .map(wrapper -> wrapper.getGuildWithNewAlliance().get())
            .collect(Collectors.toList()))
        .sql("UPDATE guild SET alliance_albion_id = :allianceAlbionId WHERE id = :id")
        .build();
  }

}