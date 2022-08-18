package com.hugarty.albionsite.job.item.cleanstep.batchremoval;


import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;

import com.hugarty.albionsite.job.item.JdbcBatchItemWriterMapper;
import com.hugarty.albionsite.job.item.JdbcBatchItemWritterBeanMappedProvider;
import com.hugarty.albionsite.job.model.clean.BatchStepRemoval;
import com.hugarty.albionsite.job.model.clean.WrapperBatchRemoval;


/**
 * This class only exist to remove data from batch tables
 * I remove data from these tables because heroku limit.
 */
public class CompositeBatchRemovalItemWriterProvider {

  private Function<List<? extends WrapperBatchRemoval>, List<? extends BatchStepRemoval>> mapper = 
    items -> items.stream()
      .flatMap(item -> item.getBatchStepRemovals().stream())
      .collect(Collectors.toList());


  public CompositeItemWriter<WrapperBatchRemoval> get(DataSource dataSource) {
    return new CompositeItemWriterBuilder<WrapperBatchRemoval>()
        .delegates(
          deleteStepExecutionContext(dataSource),
          deleteStepExecution(dataSource),
          deleteJobExecutionContext(dataSource),
          deleteJobExecutionParams(dataSource),
          deleteJobExecution(dataSource)
        )
        .build();
  }

  /**
   * Doesn't need to use {@link JdbcBatchItemWriterMapper}, used here only to show this possibility.
   * But it's possible use SQL IN operator and pass a list of IDs
   */
  private ItemWriter<WrapperBatchRemoval> deleteStepExecutionContext (DataSource dataSource) {
    return new JdbcBatchItemWriterMapper.Builder<WrapperBatchRemoval, BatchStepRemoval>()
        .dataSource(dataSource)
        .mapper(mapper)
        .sql("DELETE FROM batch_step_execution_context where step_execution_id = :stepExecutionId ")
        .build();
  }

  /**
   * Doesn't need to use {@link JdbcBatchItemWriterMapper}, used here only to show this possibility.
   * But it's possible use SQL IN operator and pass a list of IDs
   */
  private ItemWriter<WrapperBatchRemoval> deleteStepExecution (DataSource dataSource) {
    return new JdbcBatchItemWriterMapper.Builder<WrapperBatchRemoval, BatchStepRemoval>()
        .dataSource(dataSource)
        .mapper(mapper)
        .sql("DELETE FROM batch_step_execution where step_execution_id = :stepExecutionId ")
        .build();
  }

  private JdbcBatchItemWriter<WrapperBatchRemoval> deleteJobExecutionContext (DataSource dataSource) {
    String sql = "DELETE FROM batch_job_execution_context where job_execution_id = :jobExecutionId ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperBatchRemoval>get(dataSource, sql);
  }

  private JdbcBatchItemWriter<WrapperBatchRemoval> deleteJobExecutionParams (DataSource dataSource) {
    String sql = "DELETE FROM batch_job_execution_params where job_execution_id = :jobExecutionId ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperBatchRemoval>get(dataSource, sql);
  }

  private JdbcBatchItemWriter<WrapperBatchRemoval> deleteJobExecution (DataSource dataSource) {
    String sql = "DELETE FROM batch_job_execution where job_execution_id = :jobExecutionId ";
    return JdbcBatchItemWritterBeanMappedProvider.<WrapperBatchRemoval>get(dataSource, sql);
  }

}
