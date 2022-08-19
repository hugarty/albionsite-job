package com.hugarty.albionsite.job.item.cleanstep.guildremoval;

import java.time.LocalDate;
import java.util.List;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.hugarty.albionsite.job.item.ResultSetExtractorProvider;
import com.hugarty.albionsite.job.item.cleanstep.HerokuLimitsConstants;
import com.hugarty.albionsite.job.item.cleanstep.TemplateMethodRemovalItemProcessor;
import com.hugarty.albionsite.job.model.clean.WrapperAllianceRemoval;

@Component
@StepScope // StepScope is needed because this item processor needs localDate.now() 
public class GuildRemovalItemProcessor extends TemplateMethodRemovalItemProcessor<WrapperAllianceRemoval> {

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public GuildRemovalItemProcessor (NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  @Override
  protected HerokuLimitsConstants geHerokuLimitsConstants() {
    return HerokuLimitsConstants.GUILD;
  }

  @Override
  protected WrapperAllianceRemoval buildWrapperRemoval(long amountToDelete) {
    WrapperAllianceRemoval wrapper = new WrapperAllianceRemoval();
    wrapper.setIdsGuilds(getIdsGuildsToDelete(amountToDelete));
    wrapper.setIdsGuildsDaily(getIdsGuildsDailyToDelete(wrapper.getIdsGuilds()));
    return wrapper;
  }

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
