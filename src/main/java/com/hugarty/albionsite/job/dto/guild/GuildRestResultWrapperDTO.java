package com.hugarty.albionsite.job.dto.guild;

import static com.hugarty.albionsite.job.dto.StringUtil.getNull;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GuildRestResultWrapperDTO {

  private static final String errorMessageNullProperty = "GuildRestResultWrapper has some property null. guild: %s; overall: %s; basic: %s";

  @JsonProperty("guild")
  public GuildDTO guild;
  @JsonProperty("overall")
  public OverallDTO overall;
  @JsonProperty("basic")
  public BasicDTO basic;

  public void checkIsValid () {
    if (guild == null || overall == null || basic == null) {
      String exceptionMessage = String.format(errorMessageNullProperty, getNull(guild), getNull(overall), getNull(basic));
      throw new IllegalStateException(exceptionMessage);
    }
    
    guild.checkIsValid();
    basic.checkIsValid();
  }

  public boolean guildHasAllianceId() {
    return (guild.allianceAlbionId != null && !guild.allianceAlbionId.trim().isEmpty());
  }
  
  public String getGuildAllianceId() {
    return guild.allianceAlbionId;
  }
}