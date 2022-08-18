package com.hugarty.albionsite.job.model.clean;

public class BatchStepRemoval {
  private long stepExecutionId;

  public BatchStepRemoval (long stepExecutionId){
    this.stepExecutionId = stepExecutionId;
  }

  public long getStepExecutionId() {
    return stepExecutionId;
  }
}
