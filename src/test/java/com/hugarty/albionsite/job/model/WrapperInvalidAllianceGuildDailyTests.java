package com.hugarty.albionsite.job.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.hugarty.albionsite.job.dto.guild.BasicDTO;
import com.hugarty.albionsite.job.dto.guild.GuildDTO;
import com.hugarty.albionsite.job.dto.guild.GuildRestResultWrapperDTO;
import com.hugarty.albionsite.job.dto.guild.OverallDTO;

@RunWith(MockitoJUnitRunner.class)
public class WrapperInvalidAllianceGuildDailyTests {
  private final String ALLIANCE_EQUAL_ID = "ALLIANCE_EQUAL_ID";

  @Test
  public void alliancePersisted_dtoHasAllianceId_equal_guildAllianceId () {
    Guild guild = buildGuild(ALLIANCE_EQUAL_ID);
    GuildRestResultWrapperDTO dto = buildWrapperDto(ALLIANCE_EQUAL_ID);
    boolean isAlliancePersisted = true;

    WrapperInvalidAllianceGuildDaily wrapperResult = new WrapperInvalidAllianceGuildDaily(guild, dto, isAlliancePersisted);
    
    assertFalse(wrapperResult.getInvalidAlliance().isPresent());
    assertFalse(wrapperResult.getGuildWithNewAlliance().isPresent());
  }

  @Test
  public void alliancePersisted_dtoAllianceId_DIFFERENT_guildAllianceId () {
    boolean isAlliancePersisted = true;
    GuildRestResultWrapperDTO dto = buildWrapperDto("DIFFERENT");
    Guild guild = buildGuild(ALLIANCE_EQUAL_ID);

    WrapperInvalidAllianceGuildDaily wrapperResult = new WrapperInvalidAllianceGuildDaily(guild, dto, isAlliancePersisted);
    
    assertTrue(wrapperResult.getGuildWithNewAlliance().isPresent());
    assertFalse(wrapperResult.getInvalidAlliance().isPresent());
    assertEquals("DIFFERENT", wrapperResult.getGuildWithNewAlliance().get().getAllianceAlbionId());
  }

  @Test
  public void alliance_NOT_Persisted_dtoAllianceId_DIFFERENT_guildAllianceId () {
    boolean isAlliancePersisted = false;
    GuildRestResultWrapperDTO dto = buildWrapperDto("DIFFERENT");
    Guild guild = buildGuild(ALLIANCE_EQUAL_ID);

    WrapperInvalidAllianceGuildDaily wrapperResult = new WrapperInvalidAllianceGuildDaily(guild, dto, isAlliancePersisted);
    
    assertTrue(wrapperResult.getGuildWithNewAlliance().isPresent());
    assertTrue(wrapperResult.getInvalidAlliance().isPresent());
    assertEquals("DIFFERENT", wrapperResult.getGuildWithNewAlliance().get().getAllianceAlbionId());
  }

  @Test
  public void alliancePersisted_dtoHasAllianceId_guildAllianceIdNULL () {
    boolean isAlliancePersisted = true;
    GuildRestResultWrapperDTO dto = buildWrapperDto(ALLIANCE_EQUAL_ID);
    Guild guild = buildGuild(null);

    WrapperInvalidAllianceGuildDaily wrapperResult = new WrapperInvalidAllianceGuildDaily(guild, dto, isAlliancePersisted);
    
    assertFalse(wrapperResult.getInvalidAlliance().isPresent());
    assertTrue(wrapperResult.getGuildWithNewAlliance().isPresent());
  }

  @Test
  public void dtoAllianceIdNULL_guildHasAllianceId () {
    GuildRestResultWrapperDTO dto = buildWrapperDto(null);
    Guild guild = buildGuild(ALLIANCE_EQUAL_ID);

    WrapperInvalidAllianceGuildDaily wrapperResult = new WrapperInvalidAllianceGuildDaily(guild, dto);
    
    assertFalse(wrapperResult.getInvalidAlliance().isPresent());
    assertTrue(wrapperResult.getGuildWithNewAlliance().isPresent());
    assertEquals(null, wrapperResult.getGuildWithNewAlliance().get().getAllianceAlbionId());
  }

  @Test
  public void dtoAllianceIdNULL_guildAllianceIdNULL () {
    GuildRestResultWrapperDTO dto = buildWrapperDto(null);
    Guild guild = buildGuild(null);

    WrapperInvalidAllianceGuildDaily wrapperResult = new WrapperInvalidAllianceGuildDaily(guild, dto);
    
    assertFalse(wrapperResult.getInvalidAlliance().isPresent());
    assertFalse(wrapperResult.getGuildWithNewAlliance().isPresent());
  }

  private Guild buildGuild(String allianceAlbionId) {
    return new Guild(1L, "guildALbionId", allianceAlbionId);
  }

  private GuildRestResultWrapperDTO buildWrapperDto(String allianceAlbionId) {
    GuildRestResultWrapperDTO dto = new GuildRestResultWrapperDTO(); 
    dto.guild = new GuildDTO();
    dto.overall = new OverallDTO();
    dto.basic = new BasicDTO();

    dto.guild.allianceAlbionId = allianceAlbionId;
    return dto;
  }
}
