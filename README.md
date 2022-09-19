The project collects daily data related to Guilds and Alliances from Albion Online API.

[Daily Job](https://github.com/hugarty/albionsite-job) (Java - Spring Batch)

[Backend](https://github.com/hugarty/albionsite-backend) (PHP - Symfony)

[Frontend](https://github.com/hugarty/albionsite-frontend) (JS - ReactJs)

The entire project runs on heroku free Dynos ([so after november it won't work anymore](https://blog.heroku.com/next-chapter))

### [Link to site](https://albionsite-frontend.herokuapp.com/)

Albion Job
---
A JOB task that collect data from (Albion Online API)[https://gameinfo.albiononline.com/api/gameinfo].

Conects with a database and based on the Alliances presented in _alliance_ table recover information from API.

Get data about:
- Guilds
- Alliances

To work you need to connect to a sql database, there is h2 and postgres migrations on src/main/resources and test/resources.
