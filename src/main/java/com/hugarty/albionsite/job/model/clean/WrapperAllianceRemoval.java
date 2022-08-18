package com.hugarty.albionsite.job.model.clean;

import java.util.List;

import static com.hugarty.albionsite.job.model.clean.CleanUtils.defaultValue;

/**
 * This class only exist to remove data from batch tables
 * I remove data from these tables because heroku limit.
 */
public class WrapperAllianceRemoval {
  
  private List<Long> idsAlliances;
  private List<Long> idsAlliancesDaily;
  private List<Long> idsAlliancesWeekly;
  private List<Long> idsGuilds;
  private List<Long> idsGuildsDaily;

  public List<Long> getIdsAlliances() {
    return idsAlliances;
  }
  public void setIdsAlliances(List<Long> idsAlliances) {
    this.idsAlliances = idsAlliances;
  }
  public List<Long> getIdsAlliancesDaily() {
    return defaultValue(idsAlliancesDaily);
  }
  public void setIdsAlliancesDaily(List<Long> idsAlliancesDaily) {
    this.idsAlliancesDaily = idsAlliancesDaily;
  }
  public List<Long> getIdsAlliancesWeekly() {
    return defaultValue(idsAlliancesWeekly);
  }
  public void setIdsAlliancesWeekly(List<Long> idsAlliancesWeekly) {
    this.idsAlliancesWeekly = idsAlliancesWeekly;
  }
  public List<Long> getIdsGuilds() {
    return defaultValue(idsGuilds);
  }
  public void setIdsGuilds(List<Long> idsGuilds) {
    this.idsGuilds = idsGuilds;
  }
  public List<Long> getIdsGuildsDaily() {
    return defaultValue(idsGuildsDaily);
  }
  public void setIdsGuildsDaily(List<Long> idsGuildsDaily) {
    this.idsGuildsDaily = idsGuildsDaily;
  }

}
