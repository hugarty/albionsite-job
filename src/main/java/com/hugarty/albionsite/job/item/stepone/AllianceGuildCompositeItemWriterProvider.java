package com.hugarty.albionsite.job.item.stepone;


import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;

import com.hugarty.albionsite.job.item.JdbcBatchItemWriterMapper;
import com.hugarty.albionsite.job.model.Alliance;
import com.hugarty.albionsite.job.model.Guild;
import com.hugarty.albionsite.job.model.WrapperAllianceGuilds;

public class AllianceGuildCompositeItemWriterProvider {
  
  public CompositeItemWriter<WrapperAllianceGuilds> get(DataSource dataSource) {
    return new CompositeItemWriterBuilder<WrapperAllianceGuilds>()
        .delegates(
            updateAllianceItemWriter(dataSource),
            insertGuildItemWriter(dataSource))
        .build();
  }

  public ItemWriter<WrapperAllianceGuilds> updateAllianceItemWriter(DataSource dataSource) {
    return new JdbcBatchItemWriterMapper.Builder<WrapperAllianceGuilds, Alliance>()
        .dataSource(dataSource)
        .mapper(items -> items.stream()
            .map(WrapperAllianceGuilds::getAlliance)
            .collect(Collectors.toList()))
        .sql("UPDATE alliance SET name = :name, tag = :tag WHERE id = :id")
        .build();
  }

  public ItemWriter<WrapperAllianceGuilds> insertGuildItemWriter(DataSource dataSource) {
    return new JdbcBatchItemWriterMapper.Builder<WrapperAllianceGuilds, Guild>()
        .dataSource(dataSource)
        .mapper(items -> items.stream()
            .flatMap(wrapper -> wrapper.getGuilds().stream())
            .collect(Collectors.toList()))
        .sql("INSERT INTO guild (albion_id, name, alliance_albion_id) VALUES (:albionId, :name, :allianceAlbionId)")
        .build();
  }
}
