package com.hugarty.albionsite.job.model;

import java.util.List;
import java.util.stream.Collectors;

import com.hugarty.albionsite.job.dto.alliance.AllianceDTO;
import com.hugarty.albionsite.job.dto.guild.GuildDTO;

public class WrapperAllianceGuilds {
  private Alliance alliance;
  private List<Guild> guilds;
    
  public WrapperAllianceGuilds(AllianceDTO allianceDTO, Alliance alliance, List<GuildDTO> guilds) {
    this.alliance = alliance;
    this.alliance.setName(allianceDTO.allianceName);
    this.alliance.setTag(allianceDTO.allianceTag);

    this.guilds = guilds.stream()
      .map(guild -> new Guild(guild, alliance.getAlbionId()))
      .collect(Collectors.toList());
  }

  public List<Guild> getGuilds() {
    return guilds;
  }

  public Alliance getAlliance() {
    return alliance;
  }
}
