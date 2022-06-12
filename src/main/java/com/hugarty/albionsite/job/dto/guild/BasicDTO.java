package com.hugarty.albionsite.job.dto.guild;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BasicDTO {

  private static final String exceptionMessage = "GuildRestResultWrapper.BasicDTO.memberCount is null";
  
  @JsonProperty("memberCount")
  public Integer memberCount;

  public void checkIsValid() {
    if (memberCount == null) {
      throw new IllegalStateException(exceptionMessage);
    }
  }
}