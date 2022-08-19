package com.hugarty.albionsite.job.item.cleanstep;

import org.springframework.batch.item.ItemProcessor;

public abstract class TemplateMethodRemovalItemProcessor<T> implements ItemProcessor<Long, T> {
  
  @Override
  public final T process(Long amountOfLines) throws Exception {
    if (amountOfLines == null) {
      return null;
    }
    if (geHerokuLimitsConstants().getLimit() >= amountOfLines) {
      return null;
    }

    long amountToDelete = (amountOfLines - geHerokuLimitsConstants().getLimit());
    return buildWrapperRemoval(amountToDelete);
  }

  protected abstract HerokuLimitsConstants geHerokuLimitsConstants ();

  protected abstract T buildWrapperRemoval (long amountToDelete);

}
