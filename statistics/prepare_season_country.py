import time

from statistics.ck_hook import CKServer
from statistics.config import clickhouse_server_info

ck_client = CKServer(host=clickhouse_server_info["HOST"],
                     port=clickhouse_server_info["PORT"],
                     user=clickhouse_server_info["USER"],
                     password=clickhouse_server_info["PASSWD"],
                     database=clickhouse_server_info["DATABASE"])


ck_client.execute_no_params("truncate table season_tz_raw_data_local on cluster replicated")

time.sleep(2)
ck_client.execute_no_params("""
insert into season_tz_raw_data
select
      toUnixTimestamp(now()),
       github_id,
       groupArray((season, tz, day_count, commit_count, total_day_count, total_commit_count, day_count_percentage,
                   commit_count_percentage))
from (select *
      from (select github_id,
                   season,
                   arrs.1                                            as tz,
                   arrs.2                                            as day_count,
                   arrs.3                                            as commit_count,
                   total_day_count,
                   total_commit_count,
                   round(day_count / total_day_count * 100, 1)       as day_count_percentage,
                   round(commit_count / total_commit_count * 100, 1) as commit_count_percentage
            from (select github_id,
                         season,
                         sum(day_count)    as                                 total_day_count,
                         sum(commit_count) as                                 total_commit_count,
                         groupArray((tz, day_count, commit_count)) arrs
                  from (select github_id,
                               tz,
                               if(tz in (0, 1, 2, 3) and month in (4, 5, 6, 7, 8, 9) or
                                  tz in (0, 1, 2, 3) and (month = 3 and day_ >= 25 or month = 10 and day_ <= 25) or
                                  tz in (-4, -5, -6, -7, -8, -9) and month in (4, 5, 6, 7, 8, 9, 10) or
                                  tz in (-4, -5, -6, -7, -8, -9) and
                                  (month = 3 and day_ >= 14 or month = 11 and day_ <= 7) or
                                  tz in (13) and month in (10,11,12,1,2,3) or
                                  tz in (13) and (month=9 and day_>=25 or month=4 and day_<=6) or
                                  tz in (11) and month in (11,12,1,2,3) or 
                                  tz in (11) and (month=10 and day_>=6 or month=4 and day_<6)
                                  ,
                                  'summer',
                                  'winter')                  as season,
                               count(distinct (month, day_)) as day_count,
                               count(distinct hexsha)        as commit_count
                        from (select a.*, b.github_id
                              from (select if((startsWith(email, '"') and endsWith(email, '"')) or
                                              (startsWith(email, '”') and endsWith(email, '”')) or
                                              (startsWith(email, '¨') and endsWith(email, '¨')),
                                              substring(email, 2, length(email) - 2), email) as email,
                                           month,
                                           day_,
                                           tz,
                                           hexsha
                                    from (
                                             select author_email                as email,
                                                    author_tz                   as tz,
                                                    toMonth(authored_date)      as month,
                                                    toDayOfMonth(authored_date) as day_,
                                                    hexsha
                                             from (select * from gits
--                                                             where search_key__repo = 'pytorch'
                                                 )
                                             union all
                                             select committer_email              as email,
                                                    committer_tz                 as tz,
                                                    toMonth(committed_date)      as month,
                                                    toDayOfMonth(committed_date) as day_,
                                                    hexsha
                                             from (select * from gits
--                                                             where search_key__repo = 'pytorch'
                                                 )
                                             )
                                    where email like '%@%'
                                    group by email, tz, hexsha, month, day_) as a global
                                       join (
                                  select email, github_id
                                  from (
                                           -- 这里用不用去重 取消去重
                                           select commit__author__email as email,
                                                  author__id            as github_id
                                           from github_commits
                                           where author__id != 0
--                                              and search_key__repo = 'pytorch'
                                           union all
                                           select commit__committer__email as email,
                                                  committer__id            as github_id
                                           from github_commits
                                           where committer__id != 0
--                                              and search_key__repo = 'pytorch'
                                           )
                                  group by email, github_id) as b on a.email = b.email)
                        group by github_id, tz, season
                        order by github_id, day_count desc, commit_count desc)
                  group by github_id, season
                  order by github_id)
                     array join arrs
            order by github_id, season, day_count_percentage desc,
                     commit_count_percentage desc))
group by github_id
""")