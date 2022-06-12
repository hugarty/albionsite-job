package com.hugarty.albionsite.job.config;

import java.io.InvalidObjectException;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.hugarty.albionsite.job.dto.guild.GuildRestResultWrapperDTO;
import com.hugarty.albionsite.job.item.JdbcBatchItemWriterMapper;
import com.hugarty.albionsite.job.model.Alliance;
import com.hugarty.albionsite.job.model.Guild;
import com.hugarty.albionsite.job.model.GuildDaily;
import com.hugarty.albionsite.job.model.WrapperAllianceGuilds;
import com.hugarty.albionsite.job.model.WrapperInvalidAllianceGuildDaily;

@Configuration
public class JobConfig {

  public static final int RETRY_NUMBER = 5;

  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;

  public JobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
  }

  @Bean
  public Job job(@Qualifier("fixInvalidAlliancesAddNewGuilds") Step fixInvalidAlliancesAddNewGuilds, 
      @Qualifier("fetchGuildDailyAndInvalidAlliances") Step fetchGuildDailyAndInvalidAlliances) {
    
    return jobBuilderFactory.get("Fetch alliances and guilds to populate database")
        .start(fixInvalidAlliancesAddNewGuilds)
        .next(fetchGuildDailyAndInvalidAlliances)
        // TODO .next() populate ALLIANCE_DAILY need to collect data from our Database
        //   call for valid alliances each guild count his members and the guild count, put on the table
        .build();
  }

  @Bean(name = "fixInvalidAlliancesAddNewGuilds")
  public Step fixInvalidAlliancesAddNewGuilds(
      @Qualifier("invalidAlliancesItemReader") ItemReader<Alliance> reader,
      @Qualifier("FetchAlliancesAndDetachedGuildsItemProcessor") ItemProcessor<Alliance, WrapperAllianceGuilds> processor,
      @Qualifier("updateAllianceAndInsertGuildItemWriter") CompositeItemWriter<WrapperAllianceGuilds> writer) {

    return stepBuilderFactory.get("Fix invalid alliances and add their new guilds")
        .<Alliance, WrapperAllianceGuilds>chunk(1) // TODO PRECISA PARALELIZAR // TODO ACHO QUE ESSE TAMANHO DE CHUNK ESTÁ ÓTIMO, pensar depois
        .reader(reader)
        .processor(processor)
        .writer(writer)
        .faultTolerant()
        .retryLimit(RETRY_NUMBER )
        .retry(RestClientException.class)
        .build();
  }

  @Bean(name = "fetchGuildDailyAndInvalidAlliances")
  public Step fetchGuildDailyAndInvalidAlliances(
      @Qualifier("allGuildsItemReader") ItemReader<Guild> reader,
      @Qualifier("fetchGuildDailyAndInvalidAlliance") ItemProcessor<Guild, WrapperInvalidAllianceGuildDaily> processor,
      @Qualifier("insertInvalidAllianceAndInsertGuildDaily") CompositeItemWriter<WrapperInvalidAllianceGuildDaily> writer) {

    return stepBuilderFactory.get("Fetch guild_daily and invalid alliance ")
        .<Guild, WrapperInvalidAllianceGuildDaily>chunk(1) 
        .reader(reader)
        .processor(processor)
        .writer(writer)
        .faultTolerant()
        .retryLimit(RETRY_NUMBER)
        .retry(RestClientException.class)
        .retry(HttpServerErrorException.class)
        .build();
  }

  @Bean(name = "fetchGuildDailyAndInvalidAlliance")
  public ItemProcessor<Guild, WrapperInvalidAllianceGuildDaily> fetchGuildDailyAndInvalidAlliance(
      RestTemplate restTemplate, JdbcTemplate jdbcTemplate) {
    return new ItemProcessor<Guild, WrapperInvalidAllianceGuildDaily>() {
      @Override
      public WrapperInvalidAllianceGuildDaily process(Guild guild) throws Exception {
        GuildRestResultWrapperDTO dto = restGetGuildWrapperDto(guild);
        dto.checkIsValid(); 
        return buildWrapper(guild, dto);
      }

      private WrapperInvalidAllianceGuildDaily buildWrapper(Guild guild, GuildRestResultWrapperDTO dto) {
        if (dto.guildHasAllianceId()) {
          boolean isDtoAlliancePersisted = isAlliancePersisted(dto.getGuildAllianceId());
          return new WrapperInvalidAllianceGuildDaily(guild, dto, isDtoAlliancePersisted);
        }
        return new WrapperInvalidAllianceGuildDaily(guild, dto);
      }

      private GuildRestResultWrapperDTO restGetGuildWrapperDto(Guild guild) {
        String url = "/guilds/" + guild.getAlbionId() + "/data";
        ResponseEntity<GuildRestResultWrapperDTO> forEntity = restTemplate.getForEntity(url,
            GuildRestResultWrapperDTO.class);

        if (!HttpStatus.OK.equals(forEntity.getStatusCode())) {
          String message = String.format("Fail to recover Guild information: %s", forEntity.toString());
          throw new RestClientException(message);
        }

        GuildRestResultWrapperDTO guildWrapper = forEntity.getBody();
        guildWrapper.checkIsValid();
        return guildWrapper;
      }
          
      private boolean isAlliancePersisted(String allianceAlbionId) {
        String sql = "SELECT EXISTS(SELECT FROM alliance WHERE albion_id = ?)";
        return jdbcTemplate.queryForObject(sql, Boolean.class, allianceAlbionId);
      }
    };
  }

  //////////////////////////////////

  @Bean("insertInvalidAllianceAndInsertGuildDaily")
  @StepScope
  public CompositeItemWriter<WrapperInvalidAllianceGuildDaily> insertInvalidAllianceAndInsertGuildDaily(DataSource dataSource) {
    return new CompositeItemWriterBuilder<WrapperInvalidAllianceGuildDaily>()
        .delegates(
            insertInvalidAllianceItemWriter(dataSource),
            insertGuildDailyItemWriterAAAAAAAA(dataSource),
            updateGuildAlliance(dataSource))
        .build();
  }

  public ItemWriter<WrapperInvalidAllianceGuildDaily> insertInvalidAllianceItemWriter(DataSource dataSource) {
    return new JdbcBatchItemWriterMapper.Builder<WrapperInvalidAllianceGuildDaily, Alliance>()
        .dataSource(dataSource)
        .mapper(items -> items.stream()
            .filter(wrapper -> wrapper.getInvalidAlliance().isPresent())
            .map(wrapper -> wrapper.getInvalidAlliance().get())
            .collect(Collectors.toList()))
        .sql("INSERT INTO alliance (albion_id) VALUES (:albionId)")
        .build();
  }

  public ItemWriter<WrapperInvalidAllianceGuildDaily> insertGuildDailyItemWriterAAAAAAAA(DataSource dataSource) {
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

  public ItemWriter<WrapperInvalidAllianceGuildDaily> updateGuildAlliance(DataSource dataSource) {
    return new JdbcBatchItemWriterMapper.Builder<WrapperInvalidAllianceGuildDaily, Guild>()
        .dataSource(dataSource)
        .mapper(items -> items.stream()
            .filter(wrapper -> wrapper.getGuildWithNewAlliance().isPresent())
            .map(wrapper -> wrapper.getGuildWithNewAlliance().get())
            .collect(Collectors.toList()))
        .sql("UPDATE guild SET alliance_albion_id = :allianceAlbionId WHERE id = :id")
        .build();
  }

  public ItemWriter<WrapperInvalidAllianceGuildDaily> updateGuildAllianceItemWriterAAAAAAAA(DataSource dataSource) {
    String insert = " UPDATE INTO guild_daily (date, guild_id, fame, killfame, deathfame, gvgkills, gvgdeaths, kills, deaths, ratio, membercount) ";
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

  //////////////////////

  @Bean(name = "invalidAlliancesItemReader") // TODO transformar isso aqui em algo mais genérico KAOSIE
  public JdbcPagingItemReader<Alliance> invalidAlliancesItemReader(DataSource dataSource) {
    SqlPagingQueryProviderFactoryBean provider = new SqlPagingQueryProviderFactoryBean();
    provider.setDataSource(dataSource);
    provider.setSelectClause("select id, albion_id");
    provider.setFromClause("from alliance");
    provider.setWhereClause("where alliance.name is null");
    provider.setSortKey("id");

    PagingQueryProvider queryProvider;
    try {
      queryProvider = provider.getObject();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    RowMapper<Alliance> rowMapper = (rs, rowNum) -> new Alliance(rs.getLong(1), rs.getString(2));

    return new JdbcPagingItemReaderBuilder<Alliance>()
        .name("invalid alliances item reader")
        .dataSource(dataSource)
        .queryProvider(queryProvider)
        .rowMapper(rowMapper)
        .pageSize(20)
        .build();
  }

  @Bean(name = "allGuildsItemReader") // TODO transformar isso aqui em algo mais genérico KAOSIE
  public JdbcPagingItemReader<Guild> allGuildsItemReader(DataSource dataSource) {
    SqlPagingQueryProviderFactoryBean provider = new SqlPagingQueryProviderFactoryBean();
    provider.setDataSource(dataSource);
    provider.setSelectClause("select id, albion_id, alliance_albion_id");
    provider.setFromClause("from guild");
    provider.setSortKey("id");

    PagingQueryProvider queryProvider;
    try {
      queryProvider = provider.getObject();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    RowMapper<Guild> rowMapper = (rs, rowNum) 
        -> new Guild(rs.getLong(1), rs.getString(2), rs.getString(3));

    return new JdbcPagingItemReaderBuilder<Guild>()
        .name("all guilds item reader")
        .dataSource(dataSource)
        .queryProvider(queryProvider)
        .rowMapper(rowMapper)
        .pageSize(20)
        .build();
  }

  @Bean(name = "updateAllianceAndInsertGuildItemWriter")
  @StepScope
  public CompositeItemWriter<WrapperAllianceGuilds> updateAllianceAndInsertGuildItemWriter(DataSource dataSource) {
    return new CompositeItemWriterBuilder<WrapperAllianceGuilds>()
        .delegates(
            updateAllianceItemWriter(dataSource),
            insertGuildItemWriter(dataSource))
        .build();
  }

  @Bean
  public ItemWriter<WrapperAllianceGuilds> updateAllianceItemWriter(DataSource dataSource) {
    return new JdbcBatchItemWriterMapper.Builder<WrapperAllianceGuilds, Alliance>()
        .dataSource(dataSource)
        .mapper(items -> items.stream()
            .map(WrapperAllianceGuilds::getAlliance)
            .collect(Collectors.toList()))
        .sql("UPDATE alliance SET name = :name, tag = :tag WHERE id = :id")
        .build();
  }

  @Bean
  public ItemWriter<WrapperAllianceGuilds> insertGuildItemWriter(DataSource dataSource) {
    return new JdbcBatchItemWriterMapper.Builder<WrapperAllianceGuilds, Guild>()
        .dataSource(dataSource)
        .mapper(items -> items.stream()
            .flatMap(wrapper -> wrapper.getGuilds().stream())
            .collect(Collectors.toList()))
        .sql("INSERT INTO guild (albion_id, name, alliance_albion_id) VALUES (:albionId, :name, :allianceAlbionId)")
        .build();
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




  /////////////////////// alliance_daily

  public void itemReader () {
    // select * from guild 
  }

  public void itemProcessor () {

  }

  public void itemWritter () {

  }

}
