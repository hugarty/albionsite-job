package com.hugarty.albionsite.job.model.clean;

import java.util.List;

import static com.hugarty.albionsite.job.model.clean.CleanUtils.defaultValue;

/**
 * This class only exist to remove data from batch tables
 * I remove data from these tables because heroku limit.
 */
public class WrapperGenericRemoval {
  
  private List<Long> ids;

  public WrapperGenericRemoval(List<Long> ids) {
    this.ids = ids;
  }

  public List<Long> getIds() {
    return defaultValue(ids);
  }
}
