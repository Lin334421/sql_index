from statistics.ck_hook import CKServer
from statistics.config import clickhouse_server_info


class BasicContribution(object):
    def __init__(self, clickhouse_server_info):
        self.bulk_size = 50000
        self.ck_client = CKServer(host=clickhouse_server_info["HOST"],
                                  port=clickhouse_server_info["PORT"],
                                  user=clickhouse_server_info["USER"],
                                  password=clickhouse_server_info["PASSWD"],
                                  database=clickhouse_server_info["DATABASE"])
        self.client = ()


    def basic_contribution_rank(self,owner,repo,start_year=None,end_year=None):
        """
        开发者创建的issues 、创建的pr、参与过的issues的评论、review的pr count 、commit count 排行
        表样式 owner ,repo ,login ....实际贡献数量、排行指标.....
        """
        if start_year and end_year:
            sql_ = f"""
insert into table basic_contribution_rank
with {start_year} as start_year, {end_year} as end_year, '{owner}' as owner, '{repo}' as repo
select toUnixTimestamp(now()),if(a.search_key__owner != '', a.search_key__owner, b.search_key__owner) as search_key__owner,
       if(a.search_key__repo != '', a.search_key__repo, b.search_key__repo)    as search_key__repo,
       if(a.login != '', a.login, b.login)                                     as login,
       create_issues_count,
       create_issue_rank,
       issues_comments_count,
       issues_comments_rank,
       create_pr_count,
       create_pr_rank,
       reviewed_count,
       reviewed_rank,
       commit_count,
       commits_rank,
       concat(toString(start_year),'--',toString(end_year)) as year_range
from (select if(a.search_key__owner != '', a.search_key__owner, b.search_key__owner) as search_key__owner,
             if(a.search_key__repo != '', a.search_key__repo, b.search_key__repo)    as search_key__repo,
             if(a.login != '', a.login, b.login)                                     as login,
             create_issues_count,
             create_issue_rank,
             create_pr_count,
             create_pr_rank,
             reviewed_count,
             reviewed_rank,
             commit_count,
             commits_rank
      from (select if(a.search_key__owner != '', a.search_key__owner, b.search_key__owner) as search_key__owner,
                   if(a.search_key__repo != '', a.search_key__repo, b.search_key__repo)    as search_key__repo,
                   if(a.login != '', a.login, b.login)                                     as login,
                   create_issues_count,
                   create_issue_rank,
                   create_pr_count,
                   create_pr_rank,
                   reviewed_count,
                   reviewed_rank
            from (select if(a.search_key__owner != '', a.search_key__owner, b.search_key__owner) as search_key__owner,
                         if(a.search_key__repo != '', a.search_key__repo, b.search_key__repo)    as search_key__repo,
                         if(a.login != '', a.login, b.login)                                     as login,
                         create_issues_count,
                         create_issue_rank,
                         create_pr_count,
                         create_pr_rank
                  from (-- 统计issues提出次数 近三年
                           select search_key__owner,
                                  search_key__repo,
                                  login,
                                  count()                                                                       as create_issues_count,
                                  rank() over (partition by search_key__repo order by create_issues_count desc) as create_issue_rank
                           from (select search_key__owner, search_key__repo, argMax(user__login, created_at) as login
                                 from github_issues
                                 where search_key__owner = owner
                                   and search_key__repo = repo
                                   and pull_request__url = ''
                                   and toYear(created_at) >= start_year
                                   and toYear(created_at) <= end_year
                                 group by search_key__owner, search_key__repo, node_id)

                           group by search_key__owner, search_key__repo, login
                           order by search_key__repo, create_issues_count desc
                           ) as a global
                           full join (-- 统计pr提出次数 近三年
                      select search_key__owner,
                             search_key__repo,
                             login,
                             count()                                                                   as create_pr_count,
                             rank() over (partition by search_key__repo order by create_pr_count desc) as create_pr_rank
                      from (select search_key__owner, search_key__repo, argMax(user__login, created_at) as login
                            from github_pull_requests
                            where search_key__owner = owner
                              and search_key__repo = repo
                              and toYear(created_at) >= start_year
                              and toYear(created_at) <= end_year
                            group by search_key__owner, search_key__repo, node_id)

                      group by search_key__owner, search_key__repo, login
                      order by search_key__owner, search_key__repo, create_pr_count desc
                      ) as b on a.search_key__owner = b.search_key__owner and
                                a.search_key__repo = b.search_key__repo and
                                a.login = b.login) as a global
                     full join (select search_key__owner,
                                       search_key__repo,
                                       login,
                                       count()                                                                     reviewed_count,
                                       rank() over (partition by search_key__repo order by reviewed_count desc) as reviewed_rank
                                from (select search_key__owner,
                                             search_key__repo,
                                             JSONExtractString(timeline_raw, 'node_id')            as node_id,
                                             JSONExtractString(JSONExtractString(argMax(timeline_raw,
                                                                                        parseDateTimeBestEffort(JSONExtractString(timeline_raw, 'submitted_at'))),
                                                                                 'user'), 'login') as login
                                      from github_issues_timeline
                                      where search_key__owner = owner
                                        and search_key__repo = repo
                                        and toYear(parseDateTimeBestEffort(JSONExtractString(timeline_raw, 'submitted_at'))) >=
                                            start_year
                                        and toYear(parseDateTimeBestEffort(JSONExtractString(timeline_raw, 'submitted_at'))) <=
                                            end_year
                                        and search_key__event = 'reviewed'

                                      group by search_key__owner, search_key__repo, node_id)

                                group by search_key__owner, search_key__repo, login
                                order by search_key__owner, search_key__repo, reviewed_count desc
                ) as b on a.search_key__owner = b.search_key__owner and
                          a.search_key__repo = b.search_key__repo and
                          a.login = b.login) as a global
               full join (-- 作为一个committer
          select search_key__owner,
                 search_key__repo,
                 login,
                 count()                                                                as commit_count,
                 rank() over (partition by search_key__repo order by commit_count desc) as commits_rank
          from (select search_key__owner,
                       search_key__repo,
                       sha,
                       argMax(author__login, commit__author__date) as login
                from github_commits
                where search_key__owner = owner
                  and search_key__repo = repo
                  and toYear(commit__author__date) >= start_year
                  and toYear(commit__author__date) <= end_year
                  and length(parents.sha) = 1
                group by search_key__owner, search_key__repo, sha)
          group by search_key__owner, search_key__repo, login
          order by search_key__owner, search_key__repo, commit_count desc
          ) as b
                         on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and
                            a.login = b.login) as a global
         full join (select search_key__owner,
                           search_key__repo,
                           login,
                           count()                                                                         as issues_comments_count,
                           rank() over (partition by search_key__repo order by issues_comments_count desc) as issues_comments_rank
                    from (select search_key__owner, search_key__repo, node_id, argMax(user__login, created_at) as login
                          from github_issues_comments
                          where search_key__owner = owner
                            and search_key__repo = repo
                            and toYear(created_at) >= start_year
                            and toYear(created_at) <= end_year
                            and search_key__number global in (select number
                                                              from github_issues
                                                              where search_key__owner = owner
                                                                and search_key__repo = repo
                                                                and pull_request__url = ''
                                                              group by number)
                          group by search_key__owner, search_key__repo, node_id)
                    group by search_key__owner, search_key__repo, login
                    order by search_key__owner, search_key__repo, issues_comments_count
    ) as b on a.search_key__owner = b.search_key__owner and
              a.search_key__repo = b.search_key__repo and
              a.login = b.login
            """
        else:
            sql_ = f"""
            insert into table basic_contribution_rank
            with '{owner}' as owner, '{repo}' as repo
select toUnixTimestamp(now()), if(a.search_key__owner != '', a.search_key__owner, b.search_key__owner) as search_key__owner,
       if(a.search_key__repo != '', a.search_key__repo, b.search_key__repo)    as search_key__repo,
       if(a.login != '', a.login, b.login)                                     as login,
       create_issues_count,
       create_issue_rank,
       issues_comments_count,
       issues_comments_rank,
       create_pr_count,
       create_pr_rank,
       reviewed_count,
       reviewed_rank,
       commit_count,
       commits_rank,
       'all_year'                                                              as year_range
from (select if(a.search_key__owner != '', a.search_key__owner, b.search_key__owner) as search_key__owner,
             if(a.search_key__repo != '', a.search_key__repo, b.search_key__repo)    as search_key__repo,
             if(a.login != '', a.login, b.login)                                     as login,
             create_issues_count,
             create_issue_rank,
             create_pr_count,
             create_pr_rank,
             reviewed_count,
             reviewed_rank,
             commit_count,
             commits_rank
      from (select if(a.search_key__owner != '', a.search_key__owner, b.search_key__owner) as search_key__owner,
                   if(a.search_key__repo != '', a.search_key__repo, b.search_key__repo)    as search_key__repo,
                   if(a.login != '', a.login, b.login)                                     as login,
                   create_issues_count,
                   create_issue_rank,
                   create_pr_count,
                   create_pr_rank,
                   reviewed_count,
                   reviewed_rank
            from (select if(a.search_key__owner != '', a.search_key__owner, b.search_key__owner) as search_key__owner,
                         if(a.search_key__repo != '', a.search_key__repo, b.search_key__repo)    as search_key__repo,
                         if(a.login != '', a.login, b.login)                                     as login,
                         create_issues_count,
                         create_issue_rank,
                         create_pr_count,
                         create_pr_rank
                  from (-- 统计issues提出次数 近三年
                           select search_key__owner,
                                  search_key__repo,
                                  login,
                                  count()                                                                       as create_issues_count,
                                  rank() over (partition by search_key__repo order by create_issues_count desc) as create_issue_rank
                           from (select search_key__owner, search_key__repo, argMax(user__login, created_at) as login
                                 from github_issues
                                 where search_key__owner = owner
                                   and search_key__repo = repo
                                   and pull_request__url = ''

                                 group by search_key__owner, search_key__repo, node_id)

                           group by search_key__owner, search_key__repo, login
                           order by search_key__repo, create_issues_count desc
                           ) as a global
                           full join (-- 统计pr提出次数 近三年
                      select search_key__owner,
                             search_key__repo,
                             login,
                             count()                                                                   as create_pr_count,
                             rank() over (partition by search_key__repo order by create_pr_count desc) as create_pr_rank
                      from (select search_key__owner, search_key__repo, argMax(user__login, created_at) as login
                            from github_pull_requests
                            where search_key__owner = owner
                              and search_key__repo = repo

                            group by search_key__owner, search_key__repo, node_id)

                      group by search_key__owner, search_key__repo, login
                      order by search_key__owner, search_key__repo, create_pr_count desc
                      ) as b on a.search_key__owner = b.search_key__owner and
                                a.search_key__repo = b.search_key__repo and
                                a.login = b.login) as a global
                     full join (select search_key__owner,
                                       search_key__repo,
                                       login,
                                       count()                                                                     reviewed_count,
                                       rank() over (partition by search_key__repo order by reviewed_count desc) as reviewed_rank
                                from (select search_key__owner,
                                             search_key__repo,
                                             JSONExtractString(timeline_raw, 'node_id')            as node_id,
                                             JSONExtractString(JSONExtractString(argMax(timeline_raw,
                                                                                        parseDateTimeBestEffort(JSONExtractString(timeline_raw, 'submitted_at'))),
                                                                                 'user'), 'login') as login
                                      from github_issues_timeline
                                      where search_key__owner = owner
                                        and search_key__repo = repo

                                        and search_key__event = 'reviewed'

                                      group by search_key__owner, search_key__repo, node_id)

                                group by search_key__owner, search_key__repo, login
                                order by search_key__owner, search_key__repo, reviewed_count desc
                ) as b on a.search_key__owner = b.search_key__owner and
                          a.search_key__repo = b.search_key__repo and
                          a.login = b.login) as a global
               full join (-- 作为一个committer
          select search_key__owner,
                 search_key__repo,
                 login,
                 count()                                                                as commit_count,
                 rank() over (partition by search_key__repo order by commit_count desc) as commits_rank
          from (select search_key__owner,
                       search_key__repo,
                       sha,
                       argMax(author__login, commit__author__date) as login
                from github_commits
                where search_key__owner = owner
                  and search_key__repo = repo

                  and length(parents.sha) = 1
                group by search_key__owner, search_key__repo, sha)
          group by search_key__owner, search_key__repo, login
          order by search_key__owner, search_key__repo, commit_count desc
          ) as b
                         on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and
                            a.login = b.login) as a global
         full join (select search_key__owner,
                           search_key__repo,
                           login,
                           count()                                                                         as issues_comments_count,
                           rank() over (partition by search_key__repo order by issues_comments_count desc) as issues_comments_rank
                    from (select search_key__owner, search_key__repo, node_id, argMax(user__login, created_at) as login
                          from github_issues_comments
                          where search_key__owner = owner
                            and search_key__repo = repo

                            and search_key__number global in (select number
                                                              from github_issues
                                                              where search_key__owner = owner
                                                                and search_key__repo = repo
                                                                and pull_request__url = ''
                                                              group by number)
                          group by search_key__owner, search_key__repo, node_id)
                    group by search_key__owner, search_key__repo, login
                    order by search_key__owner, search_key__repo, issues_comments_count
    ) as b on a.search_key__owner = b.search_key__owner and
              a.search_key__repo = b.search_key__repo and
              a.login = b.login
            """
        count = self.ck_client.execute_no_params(sql_)
        print(count)
        self.ck_client.execute_no_params(f"optimize table basic_contribution_rank_local on cluster replicated partition '{owner}'")




c = BasicContribution(clickhouse_server_info)
c.basic_contribution_rank("kubernetes", "kubernetes",2019,2023)


"""
create table default.basic_contribution_rank_local on cluster replicated
(
    inserted_at           Int64,
    search_key__owner     String,
    search_key__repo      String,
    login                 String,
    create_issues_count   Int64,
    create_issue_rank     Int64,
    issues_comments_count Int64,
    issues_comments_rank  Int64,
    create_pr_count       Int64,
    create_pr_rank        Int64,
    reviewed_count        Int64,
    reviewed_rank         Int64,
    commit_count          Int64,
    commits_rank          Int64,
    year_range            String
)
    engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/basic_contribution_rank__', '{replica}',
             inserted_at)
        partition by search_key__owner
        ORDER BY (search_key__owner, search_key__repo, login,year_range)
        SETTINGS index_granularity = 8192;


create table default.basic_contribution_rank on cluster replicated as basic_contribution_rank_local
    engine = Distributed('replicated', 'default', 'basic_contribution_rank_local', halfMD5(login));

"""