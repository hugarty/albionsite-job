package com.hugarty.albionsite.job.item.cleanstep.guildremoval;

import java.time.LocalDate;
import java.util.List;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.hugarty.albionsite.job.item.ResultSetExtractorProvider;
import com.hugarty.albionsite.job.item.cleanstep.HerokuLimitsConstants;
import com.hugarty.albionsite.job.model.clean.WrapperAllianceRemoval;

@Component
@StepScope // StepScope is needed because this item processor needs localDate.now() 
public class GuildRemovalItemProcessor implements ItemProcessor<Long, WrapperAllianceRemoval> {
  
  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public GuildRemovalItemProcessor (NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  // TODO testar isso aqui 

  @Override
  public WrapperAllianceRemoval process(Long amountOfLines) throws Exception {
    if (amountOfLines == null) {
      return null;
    }
    if (HerokuLimitsConstants.GUILD.getLimit() >= amountOfLines) {
      return null;
    }

    long amountToDelete = (amountOfLines - HerokuLimitsConstants.GUILD.getLimit());
    WrapperAllianceRemoval wrapper = new WrapperAllianceRemoval();
    wrapper.setIdsGuilds(getIdsGuildsToDelete(amountToDelete));
    wrapper.setIdsGuildsDaily(getIdsGuildsDailyToDelete(wrapper.getIdsGuilds()));
    
    return wrapper;
  }

  // TODO esse método está "duplicado"
  private List<Long> getIdsGuildsToDelete (long amountToDelete) {
    String select =   " SELECT guild_id FROM guild_daily " ;
    String where =    " WHERE date = :date_now ";
    String orderBy =  " ORDER BY membercount ASC ";
    String limit =    " LIMIT :amount_to_delete";
    String sql = select + where + orderBy + limit;

    MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource("amount_to_delete", amountToDelete);
    mapSqlParameterSource.addValue("date_now", LocalDate.now());

    return namedParameterJdbcTemplate.query( 
        sql,
        mapSqlParameterSource,        
        ResultSetExtractorProvider.getOneColumn(Long.class));
  }

  // TODO esse método está duplicado
  private List<Long> getIdsGuildsDailyToDelete(List<Long> idsGuilds) {
    String select = " SELECT id FROM guild_daily " ;
    String where = " WHERE guild_id IN (:ids) ";
    String sql = select + where;

    return namedParameterJdbcTemplate.query( 
        sql,
        new MapSqlParameterSource("ids", idsGuilds),
        ResultSetExtractorProvider.getOneColumn(Long.class));
  }

}
