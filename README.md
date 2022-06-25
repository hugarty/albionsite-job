# Albion Job
A JOB task that collect data from Albion Online API https://gameinfo.albiononline.com/api/gameinfo.

Conects with a database and based on the Alliances presented in _alliance_ table recover information from API.

Get data about:
- Guilds
- Alliances


To work you need to connect to a sql database, there is h2 and postgres migrations on src/main/resources and test/resources.
