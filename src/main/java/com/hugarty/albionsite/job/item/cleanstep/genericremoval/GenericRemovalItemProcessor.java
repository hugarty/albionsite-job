package com.hugarty.albionsite.job.item.cleanstep.genericremoval;

import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.hugarty.albionsite.job.item.ResultSetExtractorProvider;
import com.hugarty.albionsite.job.item.cleanstep.HerokuLimitsConstants;
import com.hugarty.albionsite.job.item.cleanstep.TemplateMethodRemovalItemProcessor;
import com.hugarty.albionsite.job.model.clean.WrapperGenericRemoval;

public class GenericRemovalItemProcessor extends TemplateMethodRemovalItemProcessor<WrapperGenericRemoval> {
  
  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
  private final HerokuLimitsConstants herokuConstants;

  public GenericRemovalItemProcessor (NamedParameterJdbcTemplate namedParameterJdbcTemplate, HerokuLimitsConstants herokuConstants) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    this.herokuConstants = herokuConstants;
  }

  @Override
  protected HerokuLimitsConstants geHerokuLimitsConstants() {
    return herokuConstants;
  }

  @Override
  protected WrapperGenericRemoval buildWrapperRemoval(long amountToDelete) {
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