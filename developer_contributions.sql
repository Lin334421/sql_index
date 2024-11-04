
-- 指定owner repo 年份（某年之后的数据 包括该年）
with 2022 as year, 'rust-lang' as owner, 'rust' as repo
select if(a.search_key__owner != '', a.search_key__owner, b.search_key__owner) as search_key__owner,
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
       commits_rank
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
                                   and toYear(created_at) >= year
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
                              and toYear(created_at) >= year
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
                                            year
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
                  and toYear(commit__author__date) >= year
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
                            and toYear(created_at) >= year
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
