CREATE TABLE alliance  (
	id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY ,
	albion_id VARCHAR(50) NOT NULL UNIQUE,
  name VARCHAR(50),
  tag VARCHAR(5)
);

CREATE TABLE guild  (
	id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY ,
	albion_id VARCHAR(50) NOT NULL,
  name VARCHAR(50) NOT NULL,
  alliance_albion_id VARCHAR(50),

	constraint guild_alliance_id_fk foreign key (alliance_albion_id)
	references alliance(albion_id)
);

CREATE TABLE guild_daily (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  date DATE NOT NULL, 
  guild_id BIGINT NOT NULL,
  fame BIGINT NOT NULL, 
  killfame BIGINT NOT NULL, 
  deathfame BIGINT NOT NULL, 
  gvgkills BIGINT NOT NULL, 
  gvgdeaths BIGINT NOT NULL, 
  kills BIGINT NOT NULL, 
  deaths  BIGINT NOT NULL, 
  ratio VARCHAR(5) NOT NULL, 
  membercount INTEGER NOT NULL,

	constraint guild_daily_guild_id_fk foreign key (guild_id)
	references guild(id)
);

CREATE TABLE alliance_daily (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  date DATE NOT NULL, 
  alliance_id BIGINT NOT NULL,
  guildcount INTEGER NOT NULL,
  membercount BIGINT NOT NULL,

	constraint alliance_daily_alliance_id_fk foreign key (alliance_id)
	references alliance(id)
);

CREATE TABLE alliance_weekly (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  date DATE NOT NULL, 
  alliance_id BIGINT NOT NULL,
  territories BIGINT NOT NULL, 
  castles BIGINT NOT NULL, 

	constraint alliance_weekly_alliance_id_fk foreign key (alliance_id)
	references alliance(id) 
);
