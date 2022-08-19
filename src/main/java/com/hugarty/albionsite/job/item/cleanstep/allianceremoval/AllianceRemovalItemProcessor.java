package com.hugarty.albionsite.job.item.cleanstep.allianceremoval;

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
public class AllianceRemovalItemProcessor extends TemplateMethodRemovalItemProcessor<WrapperAllianceRemoval> {

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public AllianceRemovalItemProcessor (NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  @Override
  protected HerokuLimitsConstants geHerokuLimitsConstants() {
    return HerokuLimitsConstants.ALLIANCE;
  }

  @Override
  protected WrapperAllianceRemoval buildWrapperRemoval(long amountToDelete) {
    WrapperAllianceRemoval wrapper = new WrapperAllianceRemoval();
    wrapper.setIdsAlliances(getIdsAlliancesWillBeDeleted(amountToDelete));
    wrapper.setIdsAlliancesDaily(getAllIdsAlliancesDailyToDelete(wrapper.getIdsAlliances()));
    wrapper.setIdsAlliancesWeekly(getIdsAllAlliancesWeeklyToDelete(wrapper.getIdsAlliances()));
    wrapper.setIdsGuilds(getIdsGuildsToDelete(wrapper.getIdsAlliances()));
    wrapper.setIdsGuildsDaily(getIdsGuildsDailyToDelete(wrapper.getIdsGuilds()));
    return wrapper;
  }

  private List<Long> getIdsAlliancesWillBeDeleted (long amountToDelete) {
    String select =   " SELECT alliance_id FROM alliance_daily " ;
    String where =    " WHERE date = :date_now ";
    String orderBy =  " ORDER BY membercount ASC, guildcount ASC ";
    String limit =    " LIMIT :amount_to_delete";
    String sql = select + where + orderBy + limit;

    MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource("amount_to_delete", amountToDelete);
    mapSqlParameterSource.addValue("date_now", LocalDate.now());

    return namedParameterJdbcTemplate.query( 
        sql,
        mapSqlParameterSource,        
        ResultSetExtractorProvider.getOneColumn(Long.class));
  }

  private List<Long> getAllIdsAlliancesDailyToDelete(List<Long> idsAlliances) {
    String select = " SELECT id FROM alliance_daily " ;
    String where = " WHERE alliance_id IN (:ids) ";
    String sql = select + where;

    return namedParameterJdbcTemplate.query( 
        sql,
        new MapSqlParameterSource("ids", idsAlliances),
        ResultSetExtractorProvider.getOneColumn(Long.class));
  }

  private List<Long> getIdsAllAlliancesWeeklyToDelete(List<Long> idsAlliances) {
    String select = " SELECT id FROM alliance_weekly " ;
    String where = " WHERE alliance_id IN (:ids) ";
    String sql = select + where;

    return namedParameterJdbcTemplate.query( 
        sql,
        new MapSqlParameterSource("ids", idsAlliances),
        ResultSetExtractorProvider.getOneColumn(Long.class));
  }

  private List<Long> getIdsGuildsToDelete(List<Long> idsAlliances) {
    List<String> allianceAlbionsIds = getAlliancesAlbionId(idsAlliances);

    String select = " SELECT id FROM guild " ;
    String where = " WHERE alliance_albion_id IN (:allianceAlbionsids) ";
    String sql = select + where;

    return namedParameterJdbcTemplate.query( 
        sql,
        new MapSqlParameterSource("allianceAlbionsids", allianceAlbionsIds),
        ResultSetExtractorProvider.getOneColumn(Long.class));
  }

  private List<String> getAlliancesAlbionId(List<Long> idsAlliances) {
    String select = " SELECT albion_id FROM alliance " ;
    String where = " WHERE id IN (:ids) ";
    String sql = select + where;

    return namedParameterJdbcTemplate.query( 
        sql,
        new MapSqlParameterSource("ids", idsAlliances),
        ResultSetExtractorProvider.getOneColumn(String.class));
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
