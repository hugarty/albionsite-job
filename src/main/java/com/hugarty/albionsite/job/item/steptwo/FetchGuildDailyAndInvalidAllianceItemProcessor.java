package com.hugarty.albionsite.job.item.steptwo;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.hugarty.albionsite.job.dto.guild.GuildRestResultWrapperDTO;
import com.hugarty.albionsite.job.model.Guild;
import com.hugarty.albionsite.job.model.WrapperInvalidAllianceGuildDaily;
import com.hugarty.albionsite.job.rest.RestRetryableProvider;

@Component
public class FetchGuildDailyAndInvalidAllianceItemProcessor
    implements ItemProcessor<Guild, WrapperInvalidAllianceGuildDaily> {

  private final RestRetryableProvider restRetryableProvider;
  private final JdbcTemplate jdbcTemplate;

  public FetchGuildDailyAndInvalidAllianceItemProcessor(RestRetryableProvider restRetryableProvider,
      JdbcTemplate jdbcTemplate) {
    this.restRetryableProvider = restRetryableProvider;
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public WrapperInvalidAllianceGuildDaily process(Guild guild) throws Exception {
    GuildRestResultWrapperDTO dto = restRetryableProvider.getForEntity(getUrlGuildData(guild),
        GuildRestResultWrapperDTO.class);
    return buildWrapper(guild, dto);
  }

  private WrapperInvalidAllianceGuildDaily buildWrapper(Guild guild, GuildRestResultWrapperDTO dto) {
    dto.checkIsValid();
    if (dto.guildHasAllianceId()) {
      boolean isDtoAlliancePersisted = isAlliancePersisted(dto.getGuildAllianceId());
      return new WrapperInvalidAllianceGuildDaily(guild, dto, isDtoAlliancePersisted);
    }
    return new WrapperInvalidAllianceGuildDaily(guild, dto);
  }

  private String getUrlGuildData(Guild guild) {
    return "/guilds/" + guild.getAlbionId() + "/data";
  }

  private boolean isAlliancePersisted(String allianceAlbionId) {
    String sql = "SELECT EXISTS(SELECT FROM alliance WHERE albion_id = ?)";
    return jdbcTemplate.queryForObject(sql, Boolean.class, allianceAlbionId);
  }
}