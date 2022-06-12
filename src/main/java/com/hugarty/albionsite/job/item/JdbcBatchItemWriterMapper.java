package com.hugarty.albionsite.job.item;

import java.util.List;
import java.util.function.Function;

import javax.sql.DataSource;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;

public class JdbcBatchItemWriterMapper<T, U> implements ItemWriter<T> {

  private JdbcBatchItemWriter<U> jdbcBatchItemWriter;
  private Function<List<? extends T>, List<? extends U>> mapper;

  public JdbcBatchItemWriterMapper(DataSource dataSource, String sql, Function<List<? extends T>, List<? extends U>> mapper) {
    this.mapper = mapper;
    this.jdbcBatchItemWriter = new JdbcBatchItemWriterBuilder<U>()
      .beanMapped()
      .dataSource(dataSource)
      .sql(sql)
      .build();
    
    jdbcBatchItemWriter.afterPropertiesSet(); // WE NEED TO CALL THIS TO USE BeanMapped instead CollumnMapped
  }

  @Override
  public void write(List<? extends T> items) throws Exception {
    List<? extends U> mappedItems = mapper.apply(items);
    jdbcBatchItemWriter.write(mappedItems);
  }

  public static class Builder<T, U> {
    
    private DataSource dataSource;
    private String sql;
    private Function<List<? extends T>, List<? extends U>> mapper;
    
    public Builder<T, U> dataSource (DataSource dataSource){
      this.dataSource = dataSource;
      return this;
    }
    public Builder<T, U> sql (String sql){
      this.sql = sql;
      return this;
    }
    public Builder<T, U> mapper (Function<List<? extends T>, List<? extends U>> mapper){
      this.mapper = mapper;
      return this;
    }

    public JdbcBatchItemWriterMapper<T,U> build () {
      if (dataSource == null || sql == null || mapper == null) {
        throw new IllegalStateException("All params need to be populated");
      }
      return new JdbcBatchItemWriterMapper<>(dataSource, sql, mapper);
    }

  }
  
}
