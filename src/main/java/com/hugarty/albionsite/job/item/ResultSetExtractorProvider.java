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

  // Todo DELETAR ISSO AQUI SE NÃO FOR NECESSÁRIO
  // public static <T1, T2> ResultSetExtractor<List<TwoColumnsResult<T1,T2>>> getTwoColumns (Class<T1> clazz1, Class<T2> clazz2) {
  //   return (ResultSetExtractor<List<TwoColumnsResult<T1,T2>>>) rs -> {
  //     List<TwoColumnsResult<T1,T2>> list = new ArrayList<>();
  //     while(rs.next()){
  //       list.add(new TwoColumnsResult<>(rs.getObject(1, clazz1), rs.getObject(2, clazz2)));
  //     }
  //     return list;
  //   };
  // }

  // public static class TwoColumnsResult<T1, T2> {
  //   private T1 columnOne;
  //   private T2 columnTwo;

  //   public TwoColumnsResult(T1 columnOne, T2 columnTwo) {
  //     this.columnOne = columnOne;
  //     this.columnTwo = columnTwo;
  //   }

  //   public T1 getColumnOne() {
  //     return columnOne;
  //   }

  //   public T2 getColumnTwo() {
  //     return columnTwo;
  //   }
  // }

}
