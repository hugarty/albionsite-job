package com.hugarty.albionsite.job.model;

import java.util.Optional;

import com.hugarty.albionsite.job.dto.guild.GuildRestResultWrapperDTO;

public class WrapperInvalidAllianceGuildDaily {

  private GuildDaily guildDaily;
  private Optional<Alliance> invalidAlliance = Optional.empty();
  private Optional<Guild> guildWithNewAlliance = Optional.empty();

  public WrapperInvalidAllianceGuildDaily(Guild guild, GuildRestResultWrapperDTO dto) {
    this.guildDaily = new GuildDaily(guild, dto);

    if (guild.getAllianceAlbionId() != null) {
      guildWithNewAlliance = buildOptionalGuildNewAlliance(guild, dto);
    }
  }

  public WrapperInvalidAllianceGuildDaily(Guild guild, GuildRestResultWrapperDTO dto, boolean isAlliancePersisted) {
    this.guildDaily = new GuildDaily(guild, dto);

    if (isAlliancePersisted) {
      if (dto.getGuildAllianceId().equals(guild.getAllianceAlbionId())) {
        return;   
      }
      guildWithNewAlliance = buildOptionalGuildNewAlliance(guild, dto);

    } else {
      invalidAlliance = Optional.of(new Alliance(null, dto.getGuildAllianceId()));
      if (!dto.getGuildAllianceId().equals(guild.getAllianceAlbionId())) {
        guildWithNewAlliance = buildOptionalGuildNewAlliance(guild, dto);
      }
    }
  }

  private Optional<Guild> buildOptionalGuildNewAlliance(Guild guild, GuildRestResultWrapperDTO dto) {
    return Optional.of(new Guild(guild.getId(), guild.getAlbionId(), dto.getGuildAllianceId()));
  }

  public GuildDaily getGuildDaily() {
    return guildDaily;
  }

  public Optional<Alliance> getInvalidAlliance() {
    return invalidAlliance;
  }
  
  public Optional<Guild> getGuildWithNewAlliance() {
    return guildWithNewAlliance;
  }
}