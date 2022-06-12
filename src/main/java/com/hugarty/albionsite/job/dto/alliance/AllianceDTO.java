package com.hugarty.albionsite.job.dto.alliance;

import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hugarty.albionsite.job.dto.guild.GuildDTO;

public class AllianceDTO {

  @JsonProperty("AllianceId")
  public String allianceId;
  @JsonProperty("AllianceName")
  public String allianceName;
  @JsonProperty("AllianceTag")
  public String allianceTag;
  @JsonProperty("FounderId")
  public String founderId;
  @JsonProperty("FounderName")
  public String founderName;
  @JsonProperty("Founded")
  public String founded;
  @JsonProperty("Guilds")
  public List<GuildDTO> guilds = new ArrayList<>();
  @JsonProperty("NumPlayers")
  public Integer numPlayers;

}
