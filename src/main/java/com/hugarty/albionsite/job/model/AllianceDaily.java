package com.hugarty.albionsite.job.model;

import java.time.LocalDate;

public class AllianceDaily {

  private Long id;
  private LocalDate date;
  private Long allianceId;
  private Integer guildCount;
  private Integer memberCount;

  public AllianceDaily(LocalDate date, Long allianceId, Integer guildCount, Integer memberCount) {
    this.date = date;
    this.allianceId = allianceId;
    this.guildCount = guildCount;
    this.memberCount = memberCount;
  }

  public Long getId() {
    return id;
  }

  public LocalDate getDate() {
    return date;
  }

  public Long getAllianceId() {
    return allianceId;
  }

  public Integer getGuildCount() {
    return guildCount;
  }

  public Integer getMemberCount() {
    return memberCount;
  }

}