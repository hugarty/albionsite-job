package com.hugarty.albionsite.job.item.stepone;

import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

import com.hugarty.albionsite.job.dto.alliance.AllianceDTO;
import com.hugarty.albionsite.job.dto.guild.GuildDTO;
import com.hugarty.albionsite.job.model.Alliance;
import com.hugarty.albionsite.job.model.WrapperAllianceGuilds;
import com.hugarty.albionsite.job.rest.RestRetryableProvider;

@Component
public class FetchAlliancesAndDetachedGuildsItemProcessor implements ItemProcessor<Alliance, WrapperAllianceGuilds> {

  private static final String URL_ALLIANCES = "/alliances/";

  private final RestRetryableProvider restRetryableProvider;
  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public FetchAlliancesAndDetachedGuildsItemProcessor(RestRetryableProvider restRetryableProvider,
      NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.restRetryableProvider = restRetryableProvider;
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  @Override
  public WrapperAllianceGuilds process(Alliance alliance) throws Exception {
    AllianceDTO allianceDTO = restRetryableProvider.getForEntity(getUrlAlliance(alliance), AllianceDTO.class);
    if (allianceDTO.guilds == null) {
      return new WrapperAllianceGuilds(allianceDTO, alliance, Collections.emptyList());
    }
    List<GuildDTO> guildsNotSaved = getGuildsNotPersisted(allianceDTO.guilds);
    return new WrapperAllianceGuilds(allianceDTO, alliance, guildsNotSaved);
  }

  private String getUrlAlliance(Alliance alliance) {
    return URL_ALLIANCES + alliance.getAlbionId();
  }

  private List<GuildDTO> getGuildsNotPersisted(List<GuildDTO> guildsDTO) {
    List<String> ids = guildsDTO.stream()
        .map(guildDTO -> guildDTO.albionId)
        .collect(Collectors.toList());
    SqlParameterSource parameters = new MapSqlParameterSource("ids", ids);

    TreeSet<String> guildsAlbionIdSaved = namedParameterJdbcTemplate.query(
        "SELECT albion_id FROM guild WHERE albion_id IN (:ids)",
        parameters,
        (ResultSetExtractor<TreeSet<String>>) rs -> {
          TreeSet<String> tree = new TreeSet<>();
          while (rs.next()) {
            tree.add(rs.getString(1));
          }
          return tree;
        });

    return guildsDTO.stream()
        .filter(a -> !guildsAlbionIdSaved.contains(a.albionId))
        .collect(Collectors.toList());
  }

}
