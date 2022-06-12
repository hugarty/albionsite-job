package com.hugarty.albionsite.job.dto.guild;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GuildDTO {

  private static final String exceptionMessage = "GuildRestResultWrapper.GuildDTO.albionId is null";

  @JsonProperty("Id")
  public String albionId;
  @JsonProperty("Name")
  public String name;
  @JsonProperty("FounderId")
  public String founderId;
  @JsonProperty("FounderName")
  public String founderName;
  @JsonProperty("Founded")
  public String founded;
  @JsonProperty("AllianceTag")
  public String allianceTag;
  @JsonProperty("AllianceId")
  public String allianceAlbionId;
  @JsonProperty("AllianceName")
  public Object allianceName;
  @JsonProperty("Logo")
  public Object logo;
  @JsonProperty("killFame")
  public Long killFame;
  @JsonProperty("DeathFame")
  public Long deathFame;
  @JsonProperty("AttacksWon")
  public Object attacksWon;
  @JsonProperty("DefensesWon")
  public Object defensesWon;

  public void checkIsValid() {
    if (albionId == null) {
      throw new IllegalStateException(exceptionMessage); 
    }
  }
}
