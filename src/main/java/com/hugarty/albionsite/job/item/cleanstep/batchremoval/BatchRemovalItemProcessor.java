package com.hugarty.albionsite.job.item.cleanstep.batchremoval;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.hugarty.albionsite.job.item.ResultSetExtractorProvider;
import com.hugarty.albionsite.job.model.clean.BatchStepRemoval;
import com.hugarty.albionsite.job.model.clean.WrapperBatchRemoval;

@Component
public class BatchRemovalItemProcessor implements ItemProcessor<WrapperBatchRemoval, WrapperBatchRemoval> {
  
  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public BatchRemovalItemProcessor (NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  @Override
  public WrapperBatchRemoval process(WrapperBatchRemoval item) throws Exception {
    List<Long> stepExecutionIdsToBeDeleted = getStepExecution(item.getJobExecutionId());
    item.setBatchStepRemovals(stepExecutionIdsToBeDeleted.stream()
        .map(BatchStepRemoval::new)
        .collect(Collectors.toList()));
    return item;
  }

  private List<Long> getStepExecution (long jobExecutionId) {
    return namedParameterJdbcTemplate.query( 
        "SELECT step_execution_id FROM batch_step_execution WHERE job_execution_id = :job_execution_id",
        new MapSqlParameterSource("job_execution_id", jobExecutionId),
        ResultSetExtractorProvider.getOneColumn(Long.class));
  }

}

