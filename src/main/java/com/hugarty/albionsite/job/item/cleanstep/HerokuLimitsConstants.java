package com.hugarty.albionsite.job.item.cleanstep;

public enum HerokuLimitsConstants {
  
  // This only exists because heroku limit 10k lines
  ALLIANCE (300, "alliance"),  // It's 300 and not 500 because I need lines for batch tables.
  ALLIANCE_DAILY (2500, "alliance_daily"),
  ALLIANCE_WEEKLY (500, "alliance_weekly"),
  GUILD (1500, "guild"),
  GUILD_DAILY (5000, "guild_daily");

  private int limit;
  private String tableName;

  private HerokuLimitsConstants (int limit, String tableName) {
    this.limit = limit;
    this.tableName = tableName;
  }

  public int getLimit() {
    return limit;
  }

  public String getTableName() {
    return tableName;
  }
}