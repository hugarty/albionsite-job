package com.hugarty.albionsite.job.model.clean;

import java.util.List;

/**
 * This class only exist to remove data from batch tables
 * I remove data from these tables because heroku limit.
 */
public class CleanUtils {

  private static final List<Long> DEFAULT_LIST_ID_ZERO = List.of(0L);

  private CleanUtils() {}

  /**
   * To avoid {@link org.springframework.jdbc.BadSqlGrammarException}  bad SQL grammar; 
   * that occours when this SQL "... where id IN (:emptyList)" happen. 
   * When the list is empty namedParameterJdbcTemplate can't avoid the exception
   */ 
  public static List<Long> defaultValue (List<Long> value) {
    if (value == null || value.isEmpty()) {
      return DEFAULT_LIST_ID_ZERO;  // To avoid jdbc.BadSqlGrammarException always return "... id IN (0)"
    }
    return value;
  }
}
