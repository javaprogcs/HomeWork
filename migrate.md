# HomeWork


-- 分步创建

      create table lagou_city01 as
      select d.id, p.cityName as province, c.cityName as city, d.cityName as district from
        (select * from china_city where depth=3) d
          join china_city c on d.parentId = c.id and c.depth=2
          join china_city p on c.parentId = p.id and p.depth=1;

      insert into lagou_city01
      select c.id, p.cityName as province, c.cityName as city, null as district from (select * from china_city where depth=2) c
        join china_city p on c.parentId = p.id and p.depth = 1;

      select count(*) from lagou_city01;

-- 或者使用 union 语句

      create table lagou_city as
      select d.id, p.cityName as province, c.cityName as city, d.cityName as district from
        (select * from china_city where depth=3) d
          join china_city c on d.parentId = c.id and c.depth=2
          join china_city p on c.parentId = p.id and p.depth=1
      union
      select c.id, p.cityName as province, c.cityName as city, null as district from (select * from china_city where depth=2) c
        join china_city p on c.parentId = p.id and p.depth = 1;

lagou_company 公司分离:

      drop table if exists lagou_company;
      create table lagou_company as
        select distinct t.company_id         as cid,
                        t.company_short_name as short_name,
                        t.company_full_name  as full_name,
                        t.company_size       as size,
                        t.financestage
        from lagou_position_bk t;
将公司、城市信息从主表分离出去:

        create table lagou_position
        as
        select pid, cid as city, company_id as company, position, field, salary_min, salary_max, workyear, education, ptype, pnature,           advantage, published_at, updated_at from
        (
          -- position 表中 district 为空的数据
          select p.*, c.cid from (select * from lagou_position_bk where district is null) p
             join lagou_city c on c.city like concat(p.city, '%') and c.district is null

          union all


          -- position 表中 district 不为空的数据
          select p.*, c.cid from (select * from lagou_position_bk where district is not null) p
            join lagou_city c on c.city like concat(p.city, '%') and c.district like concat(p.district, '%')

        ) as ppp;


-- 也可以分步进行。使用 union 语句虽然会简化语句，但效率会比较低
        create table lagou_position as
        select pid, cid as city, company_id as company, position, field, salary_min, salary_max, workyear, education, ptype, pnature,           advantage, published_at, updated_at
        from (select * from lagou_position_bk where district is null) p
          join lagou_city c on c.city like concat(p.city, '%') and c.district is null;

        insert into lagou_position
        select pid, cid as city, company_id as company, position, field, salary_min, salary_max, workyear, education, ptype, pnature,           advantage, published_at, updated_at
        from (select * from lagou_position_bk where district is not null) p
          join lagou_city c on c.city like concat(p.city, '%') and c.district like concat(p.district, '%');
  
 总结： union all 和 union 的区别
       
       union all 的特点:效率快,适合用于分离大数据的表，
       
       union all 的缺点:数据大量都是重复的,而且分离出来后不排序,耗用资源
       
       union 的特点:数据不重复,且方便,分离出来默认为排序
       
       union 的缺点:效率慢,适合用于小数据的表,
       
       个人感觉分离表格的方式很多,没有最好,只有在适合的情况下用才会发挥最大的作用，有待开发....
 
 
 
 
 
  
 
