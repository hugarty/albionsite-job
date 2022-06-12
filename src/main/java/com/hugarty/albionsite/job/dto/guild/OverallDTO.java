package com.hugarty.albionsite.job.dto.guild;

import com.fasterxml.jackson.annotation.JsonProperty;


public class OverallDTO {
  
  @JsonProperty("kills")
  public Long kills;
  @JsonProperty("gvgKills")
  public Long gvgKills;
  @JsonProperty("deaths")
  public Long deaths;
  @JsonProperty("gvgDeaths")
  public Long gvgDeaths;
  @JsonProperty("fame")
  public Long fame;
  @JsonProperty("ratio")
  public String ratio;

}