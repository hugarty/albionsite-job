package com.hugarty.albionsite.job.item;

import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.hugarty.albionsite.job.dto.alliance.AllianceDTO;
import com.hugarty.albionsite.job.dto.guild.GuildDTO;
import com.hugarty.albionsite.job.model.Alliance;
import com.hugarty.albionsite.job.model.WrapperAllianceGuilds;


@Component("FetchAlliancesAndDetachedGuildsItemProcessor") // TODO testar sem qualificador
public class FetchAlliancesAndDetachedGuildsItemProcessor implements ItemProcessor<Alliance, WrapperAllianceGuilds> { 
  
  private static final String URL_ALLIANCES = "/alliances/";
  
  private final RestTemplate restTemplate;
  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public FetchAlliancesAndDetachedGuildsItemProcessor (RestTemplate restTemplate, NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.restTemplate = restTemplate;
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }


  @Override
  public WrapperAllianceGuilds process(Alliance alliance) throws Exception {
    AllianceDTO allianceDTO = restGetAlliance(alliance);
    if (allianceDTO.guilds == null) {
      return new WrapperAllianceGuilds(allianceDTO, alliance, Collections.emptyList());
    }
    List<GuildDTO> guildsNotSaved = getGuildsNotPersisted(allianceDTO.guilds);    
    return new WrapperAllianceGuilds(allianceDTO, alliance, guildsNotSaved);
  }


  private AllianceDTO restGetAlliance(Alliance alliance) { // TODO código duplicado no outro processor
    String url = URL_ALLIANCES + alliance.getAlbionId();
    ResponseEntity<AllianceDTO> forEntity = restTemplate.getForEntity(url, AllianceDTO.class);

    if (!HttpStatus.OK.equals(forEntity.getStatusCode())) {
      String message = String.format("Fail to recover Alliance information: %s", forEntity.toString());
      throw new RestClientException(message); 
    }
    return forEntity.getBody();
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
          while(rs.next()){
            tree.add(rs.getString(1));
          }
          return tree;
        });

    return guildsDTO.stream()
        .filter(a -> !guildsAlbionIdSaved.contains(a.albionId))
        .collect(Collectors.toList());
  }

}
 
