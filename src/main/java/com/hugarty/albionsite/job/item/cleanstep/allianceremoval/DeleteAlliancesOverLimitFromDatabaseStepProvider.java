package com.hugarty.albionsite.job.item.cleanstep.allianceremoval;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.database.JdbcPagingItemReader;

import com.hugarty.albionsite.job.model.clean.WrapperAllianceRemoval;

public class DeleteAlliancesOverLimitFromDatabaseStepProvider {

  DataSource dataSource;
  StepBuilderFactory stepBuilderFactory;
  JdbcPagingItemReader<Long> reader;
  AllianceRemovalItemProcessor itemProcessor;
  

  public DeleteAlliancesOverLimitFromDatabaseStepProvider(DataSource dataSource, StepBuilderFactory stepBuilderFactory,
      JdbcPagingItemReader<Long> reader, AllianceRemovalItemProcessor itemProcessor) {
    this.dataSource = dataSource;
    this.stepBuilderFactory = stepBuilderFactory;
    this.reader = reader;
    this.itemProcessor = itemProcessor;
  }

  public TaskletStep get() {
    return stepBuilderFactory.get("Remove alliances if table alliance are above limit")
        .<Long, WrapperAllianceRemoval>chunk(20)
        .reader(reader)
        .processor(itemProcessor)
        .writer(new CompositeAllianceRemovalItemWriterProvider(dataSource).get())
        .build();
  }
}
