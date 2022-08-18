package com.hugarty.albionsite.job.item;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;

public class JdbcBatchItemWritterBeanMappedProvider {

  private JdbcBatchItemWritterBeanMappedProvider () {}
  
  public static <T> JdbcBatchItemWriter<T> get (DataSource dataSource, String sql) {
    JdbcBatchItemWriter<T> jdbcWriter = new JdbcBatchItemWriterBuilder<T>()
      .beanMapped()
      .dataSource(dataSource)
      .sql(sql)
      .assertUpdates(false) // avoids org.springframework.dao.EmptyResultDataAccessException in case no line was affected
      .build();
    
    jdbcWriter.afterPropertiesSet(); // WE NEED TO CALL THIS TO USE BeanMapped instead of CollumnMapped
    return jdbcWriter;
  }
}
