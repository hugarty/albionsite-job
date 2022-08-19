package com.hugarty.albionsite.job.item;

import java.security.InvalidParameterException;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.jdbc.core.RowMapper;

public class JdbcPagingItemReaderProviderBuilder<T> {
  private String name;

  private String selectClause;
  private String fromClause;
  private String whereClause;
  private String sortKey;

  private int pageSize = 20;
  private RowMapper<T> rowMapper;

  public JdbcPagingItemReader<T> build(DataSource dataSource) {
    SqlPagingQueryProviderFactoryBean provider = new SqlPagingQueryProviderFactoryBean();
    provider.setDataSource(dataSource);
    provider.setSelectClause(selectClause);
    provider.setFromClause(fromClause);
    provider.setWhereClause(whereClause);
    provider.setSortKey(sortKey);

    PagingQueryProvider queryProvider;
    try {
      queryProvider = provider.getObject();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    JdbcPagingItemReader<T> jdbcPagingItemReader = new JdbcPagingItemReaderBuilder<T>()
        .name(name)
        .dataSource(dataSource)
        .queryProvider(queryProvider)
        .rowMapper(rowMapper)
        .pageSize(pageSize)
        .build();

    try {
      jdbcPagingItemReader.afterPropertiesSet();
      return jdbcPagingItemReader;
    } catch (Exception e) {
      throw new InvalidParameterException("Was not possible create JdbcPagingItemReader.");
    }
  }
  
  public JdbcPagingItemReaderProviderBuilder<T> name(String name) {
    this.name = name;
    return this;
  }
  public JdbcPagingItemReaderProviderBuilder<T> selectClause(String selectClause) {
    this.selectClause = selectClause;
    return this;
  }
  public JdbcPagingItemReaderProviderBuilder<T> fromClause(String fromClause) {
    this.fromClause = fromClause;
    return this;
  }
  public JdbcPagingItemReaderProviderBuilder<T> whereClause(String whereClause) {
    this.whereClause = whereClause;
    return this;
  }
  public JdbcPagingItemReaderProviderBuilder<T> sortKey(String sortKey) {
    this.sortKey = sortKey;
    return this;
  }
  public JdbcPagingItemReaderProviderBuilder<T> pageSize(int pageSize) {
    this.pageSize = pageSize;
    return this;
  }
  public JdbcPagingItemReaderProviderBuilder<T> rowMapper(RowMapper<T> rowMapper) {
    this.rowMapper = rowMapper;
    return this;
  }

}
