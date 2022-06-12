package com.hugarty.albionsite.job.model;

import com.hugarty.albionsite.job.dto.guild.GuildDTO;

public class Guild {

  private Long id;
  private String albionId;
  private String name;
  private String allianceAlbionId;

  public Guild(Long id, String albionId, String allianceAlbionId) {
    this.id = id;
    this.albionId = albionId;
    this.allianceAlbionId = allianceAlbionId;
  }

  public Guild(GuildDTO guildDTO, String allianceAlbionId) {
    this.albionId = guildDTO.albionId;
    this.name = guildDTO.name;
    this.allianceAlbionId = allianceAlbionId;
  }

  public Long getId() {
    return id;
  }

  public String getAlbionId() {
    return albionId;
  }

  public String getName() {
    return name;
  }

  public String getAllianceAlbionId() {
    return allianceAlbionId;
  }

  @Override
  public String toString() {
    return "Guild [albionId=" + albionId + ", allianceAlbionId=" + allianceAlbionId + ", id=" + id + ", name=" + name + "]";
  }

}
