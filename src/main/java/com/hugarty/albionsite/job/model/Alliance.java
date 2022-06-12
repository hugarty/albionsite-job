package com.hugarty.albionsite.job.model;

public class Alliance {

  private Long id;
  private String albionId;
  private String name;
  private String tag;

  public Alliance(Long id, String albionId) {
    this.id = id;
    this.albionId = albionId;
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

  public void setName (String name) {
    this.name = name;
  }

  public String getTag() {
    return tag;
  }

  public void setTag (String tag) {
    this.tag = tag;
  }

  @Override
  public String toString() {
    return "Alliance [albionId=" + albionId + ", id=" + id + ", name=" + name + ", tag=" + tag + "]";
  }

}
