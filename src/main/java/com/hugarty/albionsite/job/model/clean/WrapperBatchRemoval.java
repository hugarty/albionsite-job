package com.hugarty.albionsite.job.model.clean;

import java.util.ArrayList;
import java.util.List;

/**
 * This class only exist to remove data from batch tables
 * I remove data from these tables because heroku limit.
 */
public class WrapperBatchRemoval {

  private long jobExecutionId;
  private List<BatchStepRemoval> batchStepRemovals = new ArrayList<>();

  public WrapperBatchRemoval (long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
  }

  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public List<BatchStepRemoval> getBatchStepRemovals() {
    return batchStepRemovals;
  }

  public void setBatchStepRemovals(List<BatchStepRemoval> batchStepRemovals) {
    this.batchStepRemovals = batchStepRemovals;
  }
}
