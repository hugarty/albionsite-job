package com.hugarty.albionsite.job.model.clean;

/**
 * This class only exist to remove data from batch tables
 * I remove data from these tables because heroku limit.
 */
public class BatchStepRemoval {
  private long stepExecutionId;

  public BatchStepRemoval (long stepExecutionId){
    this.stepExecutionId = stepExecutionId;
  }

  public long getStepExecutionId() {
    return stepExecutionId;
  }
}
