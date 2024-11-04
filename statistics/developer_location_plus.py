from statistics.ck_hook import CKServer
from statistics.config import clickhouse_server_info

ck_client = CKServer(host=clickhouse_server_info["HOST"],
                     port=clickhouse_server_info["PORT"],
                     user=clickhouse_server_info["USER"],
                     password=clickhouse_server_info["PASSWD"],
                     database=clickhouse_server_info["DATABASE"])
developer_cctlds = ck_client.execute_no_params("""

select github_id, groupArray((area,total_commit_count)) as areas
      from (select github_id, area,sum(commit_count) as total_commit_count
            from (select a.*, b.area
                  from (select email, github_id, concat('.', splitByChar('.', email)[-1]) as top_level_domain,count(distinct sha) as commit_count
                        from (
                                 -- 这里用不用去重 取消去重
                                 select commit__author__email as email,
                                        author__id            as github_id,
                                        sha
                                 from github_commits
                                 where author__id != 0
                                 union all
                                 select commit__committer__email as email,
                                        committer__id            as github_id,
                                        sha
                                 from github_commits
                                 where committer__id != 0
                                 )
                        group by email, github_id) as a global
                           join (select * from cctlds) as b
                                on a.top_level_domain = b.top_level_domain)
            group by github_id, area order by total_commit_count desc )
      group by github_id
""")
developer_cctlds_commit_count_map = dict(developer_cctlds)

github_id_main_tz_map = ck_client.execute_no_params("""
select github_id,main_tz_area,inferred_area,
location,top_n_tz_area,is_chinese_email,
raw_location,company,github_login 
from github_id_main_tz_map_v2 
limit 100""")

for data in github_id_main_tz_map:
    github_id = data[0]
    main_tz_area = data[1]
    inferred_area = data[2]
    location = data[3]
    top_n_tz_area = data[4]
    is_chinese_email = data[5]
    raw_location = data[6]
    company = data[7]
    github_login = data[8]
    if developer_cctlds_commit_count_map.get(github_id):
        print(data)
        print(developer_cctlds_commit_count_map.get(github_id))

    # print(data)