package com.hugarty.albionsite.job.item;

import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.ResultSetExtractor;

public class ResultSetExtractorProvider {
  
  private ResultSetExtractorProvider() {}

  public static <T> ResultSetExtractor<List<T>> getOneColumn (Class<T> clazz) {
    return (ResultSetExtractor<List<T>>) rs -> {
      List<T> list = new ArrayList<>();
      while(rs.next()){
        list.add(rs.getObject(1, clazz));
      }
      return list;
    };
  }

}
