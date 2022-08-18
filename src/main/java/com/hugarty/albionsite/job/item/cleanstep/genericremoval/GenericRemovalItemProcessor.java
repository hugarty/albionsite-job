package com.hugarty.albionsite.job.item.cleanstep.genericremoval;

import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.hugarty.albionsite.job.item.ResultSetExtractorProvider;
import com.hugarty.albionsite.job.item.cleanstep.HerokuLimitsConstants;
import com.hugarty.albionsite.job.model.clean.WrapperGenericRemoval;

public class GenericRemovalItemProcessor implements ItemProcessor<Long, WrapperGenericRemoval> {
  
  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
  private final HerokuLimitsConstants herokuConstants;

  public GenericRemovalItemProcessor (NamedParameterJdbcTemplate namedParameterJdbcTemplate, HerokuLimitsConstants herokuConstants) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    this.herokuConstants = herokuConstants;
  }

  // TODO - TESTAR ISSO AQUI
  @Override
  public WrapperGenericRemoval process(Long amountOfLines) throws Exception {
    if (amountOfLines == null) {
      return null;
    }
    if (herokuConstants.getLimit() >= amountOfLines) {
      return null;
    }
    long amountToDelete = (amountOfLines - herokuConstants.getLimit());
    
    return new WrapperGenericRemoval(getIdsToDelete(amountToDelete));
  }

  private List<Long> getIdsToDelete (long amountToDelete) {
    String select = " SELECT id FROM "+ herokuConstants.getTableName() ;
    String orderBy = " ORDER BY date ASC ";
    String limit = " limit :amount_to_delete";
    String sql = select + orderBy + limit;

    MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource("amount_to_delete", amountToDelete);

    return namedParameterJdbcTemplate.query( 
        sql,
        mapSqlParameterSource,        
        ResultSetExtractorProvider.getOneColumn(Long.class));
  }
}