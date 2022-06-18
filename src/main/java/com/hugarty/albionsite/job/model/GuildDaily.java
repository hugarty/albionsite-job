package com.hugarty.albionsite.job.model;

import java.time.LocalDate;

import com.hugarty.albionsite.job.dto.guild.GuildRestResultWrapperDTO;

public class GuildDaily {

  private Long id;
  private LocalDate date;
  private Long guildId;
  private Long fame;
  private Long killFame;
  private Long deathFame;
  private Long gvgKills;
  private Long gvgDeaths;
  private Long kills;
  private Long deaths;
  private String ratio;
  private Integer memberCount;

  public GuildDaily(Guild guild, GuildRestResultWrapperDTO dto) {
    this.date = LocalDate.now();
    this.guildId = guild.getId();
    
    this.killFame = dto.guild.killFame;
    this.deathFame = dto.guild.deathFame;

    this.fame = dto.overall.fame;
    this.gvgKills = dto.overall.gvgKills;
    this.gvgDeaths = dto.overall.gvgDeaths;
    this.kills = dto.overall.kills;
    this.deaths = dto.overall.deaths;
    this.ratio = dto.overall.ratio;

    this.memberCount = dto.basic.memberCount;
  }

  public Long getId() {
    return id;
  }
  public LocalDate getDate() {
    return date;
  }
  public Long getGuildId() {
    return guildId;
  }
  public Long getFame() {
    return fame;
  }
  public Long getKillFame() {
    return killFame;
  }
  public Long getDeathFame() {
    return deathFame;
  }
  public Long getGvgKills() {
    return gvgKills;
  }
  public Long getGvgDeaths() {
    return gvgDeaths;
  }
  public Long getKills() {
    return kills;
  }
  public Long getDeaths() {
    return deaths;
  }
  public String getRatio() {
    return ratio;
  }
  public Integer getMemberCount() {
    return memberCount;
  }
  
  @Override
  public String toString() {
    return "GuildDaly [date=" + date + ", deathFame=" + deathFame + ", deaths=" + deaths + ", fame=" + fame
        + ", guildId=" + guildId + ", gvgDeaths=" + gvgDeaths + ", gvgKills=" + gvgKills + ", id=" + id + ", killFame="
        + killFame + ", kills=" + kills + ", memberCount=" + memberCount + ", ratio=" + ratio + "]";
  }
}
