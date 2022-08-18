package com.hugarty.albionsite.job.item.stepthree;

import java.time.LocalDate;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.hugarty.albionsite.job.item.ResultSetExtractorProvider;
import com.hugarty.albionsite.job.model.Alliance;
import com.hugarty.albionsite.job.model.AllianceDaily;

@Component
public class BuildAlliancesDailyItemProcessor implements ItemProcessor<Alliance, AllianceDaily> {
  
  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public BuildAlliancesDailyItemProcessor (NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  @Override
  public AllianceDaily process(Alliance item) throws Exception {
    LocalDate localDateNow = LocalDate.now();
    List<Long> guildsId = getGuildsIdsByAllianceAlbionId(item.getAlbionId());
    List<Integer> memberCountTodayslist = getTodaysMembersCountFromGuildDaily(guildsId, localDateNow);

    Integer totalMemberCountToday = memberCountTodayslist.stream()
        .reduce((a, b)-> a + b)
        .orElse(0);
    return new AllianceDaily(localDateNow, item.getId(), guildsId.size(), totalMemberCountToday);
  }

  private List<Long> getGuildsIdsByAllianceAlbionId(String albionId) {
    return namedParameterJdbcTemplate.query( 
        "SELECT id FROM guild WHERE alliance_albion_id = :alliance_albion_id",
        new MapSqlParameterSource("alliance_albion_id", albionId),
        ResultSetExtractorProvider.getOneColumn(Long.class));
  }

  private List<Integer> getTodaysMembersCountFromGuildDaily(List<Long> guildsId, LocalDate localDateNow) {
    MapSqlParameterSource parameters = new MapSqlParameterSource("ids", guildsId);
    parameters.addValue("date", localDateNow);
    return namedParameterJdbcTemplate.query( 
        "SELECT membercount FROM guild_daily WHERE date = :date AND guild_id IN (:ids)",
        parameters,
        ResultSetExtractorProvider.getOneColumn(Integer.class));
  }

}

