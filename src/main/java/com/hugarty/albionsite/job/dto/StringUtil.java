package com.hugarty.albionsite.job.dto;

import java.util.Optional;

public class StringUtil {

  private StringUtil(){}

  public static Object getNull (Object object) {
    return Optional.ofNullable(object).orElse("null");
  }
}
