-- 查看 从人获取 pr v0.3（nvidia版本）  有多少人 多少pr

;
select count() from nvidia_contributor_profile_v3
;
select count() from nvidia_contributor_pr_v3



-- 指定开发者查看此人在某项目 干了什么（pr title pr url commit login）

select github_login, owner, repo, title, url, `commits.author_github_login`, createdAt
from nvidia_contributor_pr_v3
where github_login global in ('mroeschke')
  and nvidia_contributor_pr_v3.createdAt > '2019'
  and merged = 1




-- 指定项目指定公司查看目录增加行数
with 'pandas-dev' as owner, 'pandas' as repo
select search_key__owner, search_key__repo, dir, sum(insertions) as total_insertions
from (select search_key__owner,
             search_key__repo,
             message,
             `files.file_name`  as file_name,
             `files.insertions` as insertions,
                   if(length(splitByChar('/', file_name)) > 3,
                concat(splitByChar('/', file_name)[1], '/', splitByChar('/', file_name)[2], '/',splitByChar('/', file_name)[3]
--                     , '/',splitByChar('/', file_name)[4]
                    )
                       , '') as dir
      from (select a.*, b.author__login
            from (select *
                  from gits
                  where search_key__owner = owner
                    and search_key__repo = repo
                    and length(parents) = 1
--                     and toYYYYMMDD(authored_date) > 20220518
                     ) as a global
                     join (select author__login, commit__author__email
                           from github_commits
                           where author__login != '' and  lower(commit__author__email) not like '%intel%'
  and lower(commit__author__email) not like '%google%'
  and lower(commit__author__email) not like '%meta%'
  and lower(commit__author__email) not like '%facebook%'
  and lower(commit__author__email) not like '%fb.com%'
  and lower(commit__author__email) not like '%microsoft%'
                           group by author__login, commit__author__email) as b
                          on a.author_email = b.commit__author__email
            where author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and lower(commit__author__email) like '%nvidia%'
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and lower(commit__committer__email) like '%nvidia%'
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where final_company_inferred_from_company = 'nvidia'
                                                             or lower(company) like '%nvidia%'
                                                             or lower(company) like '%rapidsai%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%nvidia%'
                                                             or lower(company) like '%nvidia%'
                                                             or lower(company) like '%rapidsai%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where company like '%nvidia%'
                                                       or company like '%rapidsai'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)) array join `files.insertions`, `files.file_name`
         )
where file_name not like '%test%'
  and file_name not like '%doc%' and dir !=''
group by search_key__owner, search_key__repo, dir
order by total_insertions desc
limit 50
















-- 指定厂商 查看厂商在指定项目中 commit数和人数
-- nvidia
select search_key__owner,
       search_key__repo,
       'nvidia' as company,
--        month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and (commit__author__email like '%nvidia%' or commit__author__email like '%rapidsai%' )
            and length(parents.sha) ==1
          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and (lower(commit__author__email) like '%nvidia%' or lower(commit__author__email) like '%rapidsai%')
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and (lower(commit__committer__email) like '%nvidia%' or lower(commit__committer__email) like '%rapidsai%')
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'nvidia'
                                                             or lower(company) like '%nvidia%'
                                                             or lower(company) like '%rapidsai%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%nvidia%' or lower(email) like '%rapidsai%'
                                                             or lower(company) like '%nvidia%'
                                                             or lower(company) like '%rapidsai%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where lower(company) like '%nvidia%'
                                                       or lower(company) like '%rapidsai'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%intel%'
              and lower(commit__author__email) not like '%google%'
              and lower(commit__author__email) not like '%meta%'
              and lower(commit__author__email) not like '%facebook%'
              and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%microsoft%'
              and lower(commit__author__email) not like '%anaconda%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'nvidia')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo
--        , toYYYYMM(commit__author__date) as month

union all


-- anaconda
select search_key__owner,
       search_key__repo,
              'anaconda' as company,

--        month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
            -- todo
              and lower(commit__author__email) like '%anaconda%'
              and length(`parents.sha`) = 1
          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and lower(commit__author__email) like '%anaconda%'
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and lower(commit__committer__email) like '%anaconda%'
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'anaconda'
                                                             or lower(company) like '%anaconda%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%anaconda%'
                                                             or lower(company) like '%anaconda%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where company like '%anaconda%'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%intel%'
              and lower(commit__author__email) not like '%google%'
              and lower(commit__author__email) not like '%meta%'
              and lower(commit__author__email) not like '%facebook%'
              and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%microsoft%'
              and lower(commit__author__email) not like '%nvidia%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'anaconda')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo



union all





-- google

select search_key__owner,
       search_key__repo,
              'google' as company,

--        month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
            -- todo
              and (commit__author__email like '%google%')
                          and length(`parents.sha`) = 1

          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and lower(commit__author__email) like '%google%'
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and lower(commit__committer__email) like '%google%'
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'google'
                                                             or lower(company) like '%google%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%google%'
                                                             or lower(company) like '%google%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where lower(company) like '%google%'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%intel%'
              and lower(commit__author__email) not like '%anaconda%'
              and lower(commit__author__email) not like '%meta%'
              and lower(commit__author__email) not like '%facebook%'
              and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%microsoft%'
              and lower(commit__author__email) not like '%nvidia%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'google')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo


union all
-- intel
select search_key__owner,
       search_key__repo,
              'intel' as company,

--        month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
            -- todo
              and (commit__author__email like '%intel%')
            and length(parents.sha) =1
          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and lower(commit__author__email) like '%intel%'
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and lower(commit__committer__email) like '%intel%'
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'intel'
                                                             or lower(company) like '%intel%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%intel%'
                                                             or lower(company) like '%intel%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where lower(company) like '%intel%'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%google%'
              and lower(commit__author__email) not like '%anaconda%'
              and lower(commit__author__email) not like '%meta%'
              and lower(commit__author__email) not like '%facebook%'
              and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%microsoft%'
              and lower(commit__author__email) not like '%nvidia%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'intel')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo


union all
-- facebook



select search_key__owner,
       search_key__repo,
              'meta' as company,

--        month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
            -- todo
              and (lower(commit__author__email) like '%meta%' or lower(commit__author__email) like '%fb.com%')
              and length(parents.sha) = 1
          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and (lower(commit__author__email) like '%meta%' or lower(commit__author__email) like '%fb.com%')
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and (lower(commit__committer__email) like '%meta%' or lower(commit__committer__email) like '%fb.com%')
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'mate'
                                                             or lower(company) like '%meta%' or lower(final_company_inferred_from_company) = 'facebook'
                                                             or lower(company) like '%facebook%'  or lower(company) = 'fb')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%meta%' or lower(email) like '%fb.com%'
                                                             or lower(company) like '%facebook%' or lower(company) like '%meta%'  or lower(company) = 'fb')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where (lower(company) like '%facebook%' or lower(company) like '%meta%' or lower(company) = 'fb')
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%google%'
              and lower(commit__author__email) not like '%anaconda%'
              and lower(commit__author__email) not like '%intel%'
--               and lower(commit__author__email) not like '%facebook%'
--               and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%microsoft%'
              and lower(commit__author__email) not like '%nvidia%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'meta')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo


union all

-- microsoft
select search_key__owner,
       search_key__repo,
              'microsoft' as company,

--        month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
            -- todo
              and (commit__author__email like '%microsoft%')
            and length(parents.sha) = 1
          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and lower(commit__author__email) like '%microsoft%'
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and lower(commit__committer__email) like '%microsoft%'
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'microsoft'
                                                             or lower(company) like '%microsoft%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%microsoft%'
                                                             or lower(company) like '%microsoft%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where lower(company) like '%microsoft%'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%google%'
              and lower(commit__author__email) not like '%anaconda%'
              and lower(commit__author__email) not like '%meta%'
              and lower(commit__author__email) not like '%facebook%'
              and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%intel%'
              and lower(commit__author__email) not like '%nvidia%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'microsoft')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo



union all

-- other
select search_key__owner,
       search_key__repo,
       'other'                       as company,

--        month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
      from github_commits
      where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
          or (search_key__owner = 'dask' and search_key__repo = 'dask')
          or (search_key__owner = 'dask' and search_key__repo = 'distributed')
          or (search_key__owner = 'numba' and search_key__repo = 'numba')
          or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
--               and (commit__author__email like '%nvidia%' or commit__author__email like '%rapidsai%' )
        and length(parents.sha) == 1)
where sha global not in (select sha
                         from (select sha,
                                      search_key__owner,
                                      if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                      author__login,
                                      commit__author__date
                               from (
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and (commit__author__email like '%nvidia%' or
                                               commit__author__email like '%rapidsai%')
                                          and length(parents.sha) == 1
                                        union all
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and author__login global in (select author__login
                                                                       from (
                                                                                -- github_commit 通过邮箱找nvidia
                                                                                select author__login
                                                                                from (select author__login
                                                                                      from github_commits
                                                                                      where author__login != ''
                                                                                        and author__login not like '%[bot]%'
                                                                                        and (lower(commit__author__email) like
                                                                                             '%nvidia%' or
                                                                                             lower(commit__author__email) like
                                                                                             '%rapidsai%')
                                                                                      group by author__login

                                                                                      union all

                                                                                      select committer__login
                                                                                      from github_commits
                                                                                      where committer__login != ''
                                                                                        and committer__login not like '%[bot]%'
                                                                                        and (lower(commit__committer__email) like
                                                                                             '%nvidia%' or
                                                                                             lower(commit__committer__email) like
                                                                                             '%rapidsai%')
                                                                                      group by committer__login)
                                                                                group by author__login

                                                                                union all
                                                                                -- 在github profile 里找
                                                                                select github_login
                                                                                from (select a.*, b.final_company_inferred_from_company
                                                                                      from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                                               left join (select company, final_company_inferred_from_company
                                                                                                          from github_profile
                                                                                                          where company != ''
                                                                                                            and final_company_inferred_from_company != ''
                                                                                                          group by company, final_company_inferred_from_company) as b
                                                                                                         on a.company = b.company
                                                                                      where lower(final_company_inferred_from_company) = 'nvidia'
                                                                                         or lower(company) like '%nvidia%'
                                                                                         or lower(company) like '%rapidsai%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                                                select github_login
                                                                                from (select commits.author_email            as email,
                                                                                             commits.author_github_login     as github_login,
                                                                                             `commits.author_github_company` as company
                                                                                      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                                      where lower(email) like '%nvidia%'
                                                                                         or lower(email) like '%rapidsai%'
                                                                                         or lower(company) like '%nvidia%'
                                                                                         or lower(company) like '%rapidsai%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 也算从profile出发
                                                                                select github_login
                                                                                from nvidia_contributor_profile_v3
                                                                                where lower(company) like '%nvidia%'
                                                                                   or lower(company) like '%rapidsai'
                                                                                group by github_login
                                                                                )
                                                                       where author__login != ''
                                                                       group by author__login)
                                          and length(`parents.sha`) = 1
                                          and lower(commit__author__email) not like '%intel%'
                                          and lower(commit__author__email) not like '%google%'
                                          and lower(commit__author__email) not like '%meta%'
                                          and lower(commit__author__email) not like '%facebook%'
                                          and lower(commit__author__email) not like '%fb.com%'
                                          and lower(commit__author__email) not like '%microsoft%'
                                          and lower(commit__author__email) not like '%anaconda%'
                                        union all
                                        select search_key__owner,
                                               if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                               sha,
                                               author__login,
                                               email,
                                               commit__author__date
                                        from (select search_key__owner,
                                                     search_key__repo,
                                                     sha,
                                                     author__login,
                                                     commit__author__email as email,
                                                     commit__author__date,
                                                     multiIf(endsWith(lower(email), 'google.com'), 'google',
                                                             endsWith(lower(email), 'huawei.com'), 'huawei',
                                                             endsWith(lower(email), 'intel.com'), 'intel',
                                                             endsWith(lower(email), 'fb.com'), 'meta',
                                                             endsWith(lower(email), 'meta.com'), 'intel',
                                                             endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'enthought.com'), 'enthought',
                                                             endsWith(lower(email), 'continuum.io'), 'anaconda',
                                                             endsWith(lower(email), 'arm.com'), 'arm',
                                                             endsWith(lower(email), 'ibm.com'), 'ibm',
                                                             endsWith(lower(email), 'twosigma.com'), 'twosigma',
                                                         -- 特殊情况 drtodd13@comcast.net
                                                             (endsWith(lower(email), 'jeff@reback.net') or
                                                              endsWith(lower(email), 'stan@mtrr.org')),
                                                             'anaconda',
                                                             (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                                              endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                                             (endsWith(lower(email), 'sebastian@sipsolutions.net')),
                                                             'nvidia',
                                                             '')           as company
                                              from github_commits
                                              where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                                  or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                                  or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                                and length(`parents.sha`) = 1
                                                and company = 'nvidia')
                                        )
                               group by sha, search_key__owner, search_key__repo, author__login, commit__author__date
                               union all
                               select sha,
                                      search_key__owner,
                                      if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                      author__login,
                                      commit__author__date
                               from (
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          -- todo
                                          and lower(commit__author__email) like '%anaconda%'
                                          and length(`parents.sha`) = 1
                                        union all
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and author__login global in (select author__login
                                                                       from (
                                                                                -- github_commit 通过邮箱找nvidia
                                                                                select author__login
                                                                                from (select author__login
                                                                                      from github_commits
                                                                                      where author__login != ''
                                                                                        and author__login not like '%[bot]%'
                                                                                        and lower(commit__author__email) like '%anaconda%'
                                                                                      group by author__login

                                                                                      union all

                                                                                      select committer__login
                                                                                      from github_commits
                                                                                      where committer__login != ''
                                                                                        and committer__login not like '%[bot]%'
                                                                                        and lower(commit__committer__email) like '%anaconda%'
                                                                                      group by committer__login)
                                                                                group by author__login

                                                                                union all
                                                                                -- 在github profile 里找
                                                                                select github_login
                                                                                from (select a.*, b.final_company_inferred_from_company
                                                                                      from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                                               left join (select company, final_company_inferred_from_company
                                                                                                          from github_profile
                                                                                                          where company != ''
                                                                                                            and final_company_inferred_from_company != ''
                                                                                                          group by company, final_company_inferred_from_company) as b
                                                                                                         on a.company = b.company
                                                                                      where lower(final_company_inferred_from_company) = 'anaconda'
                                                                                         or lower(company) like '%anaconda%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                                                select github_login
                                                                                from (select commits.author_email            as email,
                                                                                             commits.author_github_login     as github_login,
                                                                                             `commits.author_github_company` as company
                                                                                      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                                      where lower(email) like '%anaconda%'
                                                                                         or lower(company) like '%anaconda%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 也算从profile出发
                                                                                select github_login
                                                                                from nvidia_contributor_profile_v3
                                                                                where company like '%anaconda%'
                                                                                group by github_login
                                                                                )
                                                                       where author__login != ''
                                                                       group by author__login)
                                          and length(`parents.sha`) = 1
                                          and lower(commit__author__email) not like '%intel%'
                                          and lower(commit__author__email) not like '%google%'
                                          and lower(commit__author__email) not like '%meta%'
                                          and lower(commit__author__email) not like '%facebook%'
                                          and lower(commit__author__email) not like '%fb.com%'
                                          and lower(commit__author__email) not like '%microsoft%'
                                          and lower(commit__author__email) not like '%nvidia%'
                                        union all
                                        select search_key__owner,
                                               if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                               sha,
                                               author__login,
                                               email,
                                               commit__author__date
                                        from (select search_key__owner,
                                                     search_key__repo,
                                                     sha,
                                                     author__login,
                                                     commit__author__email as email,
                                                     commit__author__date,
                                                     multiIf(endsWith(lower(email), 'google.com'), 'google',
                                                             endsWith(lower(email), 'huawei.com'), 'huawei',
                                                             endsWith(lower(email), 'intel.com'), 'intel',
                                                             endsWith(lower(email), 'fb.com'), 'meta',
                                                             endsWith(lower(email), 'meta.com'), 'intel',
                                                             endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'enthought.com'), 'enthought',
                                                             endsWith(lower(email), 'continuum.io'), 'anaconda',
                                                             endsWith(lower(email), 'arm.com'), 'arm',
                                                             endsWith(lower(email), 'ibm.com'), 'ibm',
                                                             endsWith(lower(email), 'twosigma.com'), 'twosigma',
                                                         -- 特殊情况 drtodd13@comcast.net
                                                             (endsWith(lower(email), 'jeff@reback.net') or
                                                              endsWith(lower(email), 'stan@mtrr.org')),
                                                             'anaconda',
                                                             (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                                              endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                                             (endsWith(lower(email), 'sebastian@sipsolutions.net')),
                                                             'nvidia',
                                                             '')           as company
                                              from github_commits
                                              where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                                  or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                                  or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                                and length(`parents.sha`) = 1
                                                and company = 'anaconda')
                                        )
                               group by sha, search_key__owner, search_key__repo, author__login, commit__author__date
                               union all
                               select sha,
                                      search_key__owner,
                                      if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                      author__login,
                                      commit__author__date
                               from (
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          -- todo
                                          and (commit__author__email like '%google%')
                                          and length(`parents.sha`) = 1

                                        union all
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and author__login global in (select author__login
                                                                       from (
                                                                                -- github_commit 通过邮箱找nvidia
                                                                                select author__login
                                                                                from (select author__login
                                                                                      from github_commits
                                                                                      where author__login != ''
                                                                                        and author__login not like '%[bot]%'
                                                                                        and lower(commit__author__email) like '%google%'
                                                                                      group by author__login

                                                                                      union all

                                                                                      select committer__login
                                                                                      from github_commits
                                                                                      where committer__login != ''
                                                                                        and committer__login not like '%[bot]%'
                                                                                        and lower(commit__committer__email) like '%google%'
                                                                                      group by committer__login)
                                                                                group by author__login

                                                                                union all
                                                                                -- 在github profile 里找
                                                                                select github_login
                                                                                from (select a.*, b.final_company_inferred_from_company
                                                                                      from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                                               left join (select company, final_company_inferred_from_company
                                                                                                          from github_profile
                                                                                                          where company != ''
                                                                                                            and final_company_inferred_from_company != ''
                                                                                                          group by company, final_company_inferred_from_company) as b
                                                                                                         on a.company = b.company
                                                                                      where lower(final_company_inferred_from_company) = 'google'
                                                                                         or lower(company) like '%google%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                                                select github_login
                                                                                from (select commits.author_email            as email,
                                                                                             commits.author_github_login     as github_login,
                                                                                             `commits.author_github_company` as company
                                                                                      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                                      where lower(email) like '%google%'
                                                                                         or lower(company) like '%google%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 也算从profile出发
                                                                                select github_login
                                                                                from nvidia_contributor_profile_v3
                                                                                where lower(company) like '%google%'
                                                                                group by github_login
                                                                                )
                                                                       where author__login != ''
                                                                       group by author__login)
                                          and length(`parents.sha`) = 1
                                          and lower(commit__author__email) not like '%intel%'
                                          and lower(commit__author__email) not like '%anaconda%'
                                          and lower(commit__author__email) not like '%meta%'
                                          and lower(commit__author__email) not like '%facebook%'
                                          and lower(commit__author__email) not like '%fb.com%'
                                          and lower(commit__author__email) not like '%microsoft%'
                                          and lower(commit__author__email) not like '%nvidia%'
                                        union all
                                        select search_key__owner,
                                               if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                               sha,
                                               author__login,
                                               email,
                                               commit__author__date
                                        from (select search_key__owner,
                                                     search_key__repo,
                                                     sha,
                                                     author__login,
                                                     commit__author__email as email,
                                                     commit__author__date,
                                                     multiIf(endsWith(lower(email), 'google.com'), 'google',
                                                             endsWith(lower(email), 'huawei.com'), 'huawei',
                                                             endsWith(lower(email), 'intel.com'), 'intel',
                                                             endsWith(lower(email), 'fb.com'), 'meta',
                                                             endsWith(lower(email), 'meta.com'), 'intel',
                                                             endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'enthought.com'), 'enthought',
                                                             endsWith(lower(email), 'continuum.io'), 'anaconda',
                                                             endsWith(lower(email), 'arm.com'), 'arm',
                                                             endsWith(lower(email), 'ibm.com'), 'ibm',
                                                             endsWith(lower(email), 'twosigma.com'), 'twosigma',
                                                         -- 特殊情况 drtodd13@comcast.net
                                                             (endsWith(lower(email), 'jeff@reback.net') or
                                                              endsWith(lower(email), 'stan@mtrr.org')),
                                                             'anaconda',
                                                             (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                                              endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                                             (endsWith(lower(email), 'sebastian@sipsolutions.net')),
                                                             'nvidia',
                                                             '')           as company
                                              from github_commits
                                              where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                                  or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                                  or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                                and length(`parents.sha`) = 1
                                                and company = 'google')
                                        )
                               group by sha, search_key__owner, search_key__repo, author__login, commit__author__date
                               union all
                               select sha,
                                      search_key__owner,
                                      if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                      author__login,
                                      commit__author__date
                               from (
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          -- todo
                                          and (commit__author__email like '%intel%')
                                          and length(parents.sha) = 1
                                        union all
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and author__login global in (select author__login
                                                                       from (
                                                                                -- github_commit 通过邮箱找nvidia
                                                                                select author__login
                                                                                from (select author__login
                                                                                      from github_commits
                                                                                      where author__login != ''
                                                                                        and author__login not like '%[bot]%'
                                                                                        and lower(commit__author__email) like '%intel%'
                                                                                      group by author__login

                                                                                      union all

                                                                                      select committer__login
                                                                                      from github_commits
                                                                                      where committer__login != ''
                                                                                        and committer__login not like '%[bot]%'
                                                                                        and lower(commit__committer__email) like '%intel%'
                                                                                      group by committer__login)
                                                                                group by author__login

                                                                                union all
                                                                                -- 在github profile 里找
                                                                                select github_login
                                                                                from (select a.*, b.final_company_inferred_from_company
                                                                                      from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                                               left join (select company, final_company_inferred_from_company
                                                                                                          from github_profile
                                                                                                          where company != ''
                                                                                                            and final_company_inferred_from_company != ''
                                                                                                          group by company, final_company_inferred_from_company) as b
                                                                                                         on a.company = b.company
                                                                                      where lower(final_company_inferred_from_company) = 'intel'
                                                                                         or lower(company) like '%intel%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                                                select github_login
                                                                                from (select commits.author_email            as email,
                                                                                             commits.author_github_login     as github_login,
                                                                                             `commits.author_github_company` as company
                                                                                      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                                      where lower(email) like '%intel%'
                                                                                         or lower(company) like '%intel%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 也算从profile出发
                                                                                select github_login
                                                                                from nvidia_contributor_profile_v3
                                                                                where lower(company) like '%intel%'
                                                                                group by github_login
                                                                                )
                                                                       where author__login != ''
                                                                       group by author__login)
                                          and length(`parents.sha`) = 1
                                          and lower(commit__author__email) not like '%google%'
                                          and lower(commit__author__email) not like '%anaconda%'
                                          and lower(commit__author__email) not like '%meta%'
                                          and lower(commit__author__email) not like '%facebook%'
                                          and lower(commit__author__email) not like '%fb.com%'
                                          and lower(commit__author__email) not like '%microsoft%'
                                          and lower(commit__author__email) not like '%nvidia%'
                                        union all
                                        select search_key__owner,
                                               if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                               sha,
                                               author__login,
                                               email,
                                               commit__author__date
                                        from (select search_key__owner,
                                                     search_key__repo,
                                                     sha,
                                                     author__login,
                                                     commit__author__email as email,
                                                     commit__author__date,
                                                     multiIf(endsWith(lower(email), 'google.com'), 'google',
                                                             endsWith(lower(email), 'huawei.com'), 'huawei',
                                                             endsWith(lower(email), 'intel.com'), 'intel',
                                                             endsWith(lower(email), 'fb.com'), 'meta',
                                                             endsWith(lower(email), 'meta.com'), 'intel',
                                                             endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'enthought.com'), 'enthought',
                                                             endsWith(lower(email), 'continuum.io'), 'anaconda',
                                                             endsWith(lower(email), 'arm.com'), 'arm',
                                                             endsWith(lower(email), 'ibm.com'), 'ibm',
                                                             endsWith(lower(email), 'twosigma.com'), 'twosigma',
                                                         -- 特殊情况 drtodd13@comcast.net
                                                             (endsWith(lower(email), 'jeff@reback.net') or
                                                              endsWith(lower(email), 'stan@mtrr.org')),
                                                             'anaconda',
                                                             (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                                              endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                                             (endsWith(lower(email), 'sebastian@sipsolutions.net')),
                                                             'nvidia',
                                                             '')           as company
                                              from github_commits
                                              where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                                  or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                                  or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                                and length(`parents.sha`) = 1
                                                and company = 'intel')
                                        )
                               group by sha, search_key__owner, search_key__repo, author__login, commit__author__date
                               union all
                               select sha,
                                      search_key__owner,
                                      if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                      author__login,
                                      commit__author__date
                               from (
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          -- todo
                                          and (lower(commit__author__email) like '%meta%' or
                                               lower(commit__author__email) like '%fb.com%')
                                          and length(parents.sha) = 1
                                        union all
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and author__login global in (select author__login
                                                                       from (
                                                                                -- github_commit 通过邮箱找nvidia
                                                                                select author__login
                                                                                from (select author__login
                                                                                      from github_commits
                                                                                      where author__login != ''
                                                                                        and author__login not like '%[bot]%'
                                                                                        and (lower(commit__author__email) like
                                                                                             '%meta%' or
                                                                                             lower(commit__author__email) like
                                                                                             '%fb.com%')
                                                                                      group by author__login

                                                                                      union all

                                                                                      select committer__login
                                                                                      from github_commits
                                                                                      where committer__login != ''
                                                                                        and committer__login not like '%[bot]%'
                                                                                        and (lower(commit__committer__email) like
                                                                                             '%meta%' or
                                                                                             lower(commit__committer__email) like
                                                                                             '%fb.com%')
                                                                                      group by committer__login)
                                                                                group by author__login

                                                                                union all
                                                                                -- 在github profile 里找
                                                                                select github_login
                                                                                from (select a.*, b.final_company_inferred_from_company
                                                                                      from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                                               left join (select company, final_company_inferred_from_company
                                                                                                          from github_profile
                                                                                                          where company != ''
                                                                                                            and final_company_inferred_from_company != ''
                                                                                                          group by company, final_company_inferred_from_company) as b
                                                                                                         on a.company = b.company
                                                                                      where lower(final_company_inferred_from_company) = 'mate'
                                                                                         or lower(company) like '%meta%'
                                                                                         or lower(final_company_inferred_from_company) = 'facebook'
                                                                                         or lower(company) like '%facebook%'
                                                                                         or lower(company) = 'fb')
                                                                                group by github_login

                                                                                union all
                                                                                -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                                                select github_login
                                                                                from (select commits.author_email            as email,
                                                                                             commits.author_github_login     as github_login,
                                                                                             `commits.author_github_company` as company
                                                                                      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                                      where lower(email) like '%meta%'
                                                                                         or lower(email) like '%fb.com%'
                                                                                         or lower(company) like '%facebook%'
                                                                                         or lower(company) like '%meta%'
                                                                                         or lower(company) = 'fb')
                                                                                group by github_login

                                                                                union all
                                                                                -- 也算从profile出发
                                                                                select github_login
                                                                                from nvidia_contributor_profile_v3
                                                                                where (lower(company) like
                                                                                       '%facebook%' or
                                                                                       lower(company) like '%meta%' or
                                                                                       lower(company) = 'fb')
                                                                                group by github_login
                                                                                )
                                                                       where author__login != ''
                                                                       group by author__login)
                                          and length(`parents.sha`) = 1
                                          and lower(commit__author__email) not like '%google%'
                                          and lower(commit__author__email) not like '%anaconda%'
                                          and lower(commit__author__email) not like '%intel%'
--               and lower(commit__author__email) not like '%facebook%'
--               and lower(commit__author__email) not like '%fb.com%'
                                          and lower(commit__author__email) not like '%microsoft%'
                                          and lower(commit__author__email) not like '%nvidia%'
                                        union all
                                        select search_key__owner,
                                               if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                               sha,
                                               author__login,
                                               email,
                                               commit__author__date
                                        from (select search_key__owner,
                                                     search_key__repo,
                                                     sha,
                                                     author__login,
                                                     commit__author__email as email,
                                                     commit__author__date,
                                                     multiIf(endsWith(lower(email), 'google.com'), 'google',
                                                             endsWith(lower(email), 'huawei.com'), 'huawei',
                                                             endsWith(lower(email), 'intel.com'), 'intel',
                                                             endsWith(lower(email), 'fb.com'), 'meta',
                                                             endsWith(lower(email), 'meta.com'), 'intel',
                                                             endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'enthought.com'), 'enthought',
                                                             endsWith(lower(email), 'continuum.io'), 'anaconda',
                                                             endsWith(lower(email), 'arm.com'), 'arm',
                                                             endsWith(lower(email), 'ibm.com'), 'ibm',
                                                             endsWith(lower(email), 'twosigma.com'), 'twosigma',
                                                         -- 特殊情况 drtodd13@comcast.net
                                                             (endsWith(lower(email), 'jeff@reback.net') or
                                                              endsWith(lower(email), 'stan@mtrr.org')),
                                                             'anaconda',
                                                             (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                                              endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                                             (endsWith(lower(email), 'sebastian@sipsolutions.net')),
                                                             'nvidia',
                                                             '')           as company
                                              from github_commits
                                              where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                                  or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                                  or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                                and length(`parents.sha`) = 1
                                                and company = 'meta')
                                        )
                               group by sha, search_key__owner, search_key__repo, author__login, commit__author__date
                               union all
                               select sha,
                                      search_key__owner,
                                      if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                      author__login,
                                      commit__author__date
                               from (
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          -- todo
                                          and (commit__author__email like '%microsoft%')
                                          and length(parents.sha) = 1
                                        union all
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and author__login global in (select author__login
                                                                       from (
                                                                                -- github_commit 通过邮箱找nvidia
                                                                                select author__login
                                                                                from (select author__login
                                                                                      from github_commits
                                                                                      where author__login != ''
                                                                                        and author__login not like '%[bot]%'
                                                                                        and lower(commit__author__email) like '%microsoft%'
                                                                                      group by author__login

                                                                                      union all

                                                                                      select committer__login
                                                                                      from github_commits
                                                                                      where committer__login != ''
                                                                                        and committer__login not like '%[bot]%'
                                                                                        and lower(commit__committer__email) like '%microsoft%'
                                                                                      group by committer__login)
                                                                                group by author__login

                                                                                union all
                                                                                -- 在github profile 里找
                                                                                select github_login
                                                                                from (select a.*, b.final_company_inferred_from_company
                                                                                      from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                                               left join (select company, final_company_inferred_from_company
                                                                                                          from github_profile
                                                                                                          where company != ''
                                                                                                            and final_company_inferred_from_company != ''
                                                                                                          group by company, final_company_inferred_from_company) as b
                                                                                                         on a.company = b.company
                                                                                      where lower(final_company_inferred_from_company) = 'microsoft'
                                                                                         or lower(company) like '%microsoft%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                                                select github_login
                                                                                from (select commits.author_email            as email,
                                                                                             commits.author_github_login     as github_login,
                                                                                             `commits.author_github_company` as company
                                                                                      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                                      where lower(email) like '%microsoft%'
                                                                                         or lower(company) like '%microsoft%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 也算从profile出发
                                                                                select github_login
                                                                                from nvidia_contributor_profile_v3
                                                                                where lower(company) like '%microsoft%'
                                                                                group by github_login
                                                                                )
                                                                       where author__login != ''
                                                                       group by author__login)
                                          and length(`parents.sha`) = 1
                                          and lower(commit__author__email) not like '%google%'
                                          and lower(commit__author__email) not like '%anaconda%'
                                          and lower(commit__author__email) not like '%meta%'
                                          and lower(commit__author__email) not like '%facebook%'
                                          and lower(commit__author__email) not like '%fb.com%'
                                          and lower(commit__author__email) not like '%intel%'
                                          and lower(commit__author__email) not like '%nvidia%'
                                        union all
                                        select search_key__owner,
                                               if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                               sha,
                                               author__login,
                                               email,
                                               commit__author__date
                                        from (select search_key__owner,
                                                     search_key__repo,
                                                     sha,
                                                     author__login,
                                                     commit__author__email as email,
                                                     commit__author__date,
                                                     multiIf(endsWith(lower(email), 'google.com'), 'google',
                                                             endsWith(lower(email), 'huawei.com'), 'huawei',
                                                             endsWith(lower(email), 'intel.com'), 'intel',
                                                             endsWith(lower(email), 'fb.com'), 'meta',
                                                             endsWith(lower(email), 'meta.com'), 'intel',
                                                             endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'enthought.com'), 'enthought',
                                                             endsWith(lower(email), 'continuum.io'), 'anaconda',
                                                             endsWith(lower(email), 'arm.com'), 'arm',
                                                             endsWith(lower(email), 'ibm.com'), 'ibm',
                                                             endsWith(lower(email), 'twosigma.com'), 'twosigma',
                                                         -- 特殊情况 drtodd13@comcast.net
                                                             (endsWith(lower(email), 'jeff@reback.net') or
                                                              endsWith(lower(email), 'stan@mtrr.org')),
                                                             'anaconda',
                                                             (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                                              endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                                             (endsWith(lower(email), 'sebastian@sipsolutions.net')),
                                                             'nvidia',
                                                             '')           as company
                                              from github_commits
                                              where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                                  or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                                  or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                                and length(`parents.sha`) = 1
                                                and company = 'microsoft')
                                        )
                               group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
                         group by sha)
group by search_key__owner, search_key__repo


union all

--amazon
select search_key__owner,
       search_key__repo,
              'amazon' as company,

--        month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
            -- todo
              and (commit__author__email like '%amazon%')
            and length(parents.sha) = 1
          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and lower(commit__author__email) like '%amazon%'
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and lower(commit__committer__email) like '%amazon%'
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'amazon'
                                                             or lower(company) like '%amazon%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%amazon%'
                                                             or lower(company) like '%amazon%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where lower(company) like '%amazon%'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%google%'
              and lower(commit__author__email) not like '%anaconda%'
              and lower(commit__author__email) not like '%meta%'
              and lower(commit__author__email) not like '%facebook%'
              and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%intel%'
              and lower(commit__author__email) not like '%nvidia%'
            and lower(commit__author__email) not like '%microsoft%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                             endsWith(lower(email), 'amazon.com'), 'amazon',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'amazon')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo












-- 指定厂商 查看厂商在指定项目中 每月的commit数和人数
-- nvidia
select search_key__owner,
       search_key__repo,
       'nvidia' as company,
       month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and (commit__author__email like '%nvidia%' or commit__author__email like '%rapidsai%' )
            and length(parents.sha) ==1
          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and (lower(commit__author__email) like '%nvidia%' or lower(commit__author__email) like '%rapidsai%')
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and (lower(commit__committer__email) like '%nvidia%' or lower(commit__committer__email) like '%rapidsai%')
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'nvidia'
                                                             or lower(company) like '%nvidia%'
                                                             or lower(company) like '%rapidsai%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%nvidia%' or lower(email) like '%rapidsai%'
                                                             or lower(company) like '%nvidia%'
                                                             or lower(company) like '%rapidsai%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where lower(company) like '%nvidia%'
                                                       or lower(company) like '%rapidsai'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%intel%'
              and lower(commit__author__email) not like '%google%'
              and lower(commit__author__email) not like '%meta%'
              and lower(commit__author__email) not like '%facebook%'
              and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%microsoft%'
              and lower(commit__author__email) not like '%anaconda%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'nvidia')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo
       , toYYYYMM(commit__author__date) as month

union all


-- anaconda
select search_key__owner,
       search_key__repo,
              'anaconda' as company,

       month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
            -- todo
              and lower(commit__author__email) like '%anaconda%'
              and length(`parents.sha`) = 1
          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and lower(commit__author__email) like '%anaconda%'
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and lower(commit__committer__email) like '%anaconda%'
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'anaconda'
                                                             or lower(company) like '%anaconda%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%anaconda%'
                                                             or lower(company) like '%anaconda%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where company like '%anaconda%'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%intel%'
              and lower(commit__author__email) not like '%google%'
              and lower(commit__author__email) not like '%meta%'
              and lower(commit__author__email) not like '%facebook%'
              and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%microsoft%'
              and lower(commit__author__email) not like '%nvidia%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'anaconda')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo
       , toYYYYMM(commit__author__date) as month



union all





-- google

select search_key__owner,
       search_key__repo,
              'google' as company,

       month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
            -- todo
              and (commit__author__email like '%google%')
                          and length(`parents.sha`) = 1

          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and lower(commit__author__email) like '%google%'
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and lower(commit__committer__email) like '%google%'
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'google'
                                                             or lower(company) like '%google%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%google%'
                                                             or lower(company) like '%google%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where lower(company) like '%google%'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%intel%'
              and lower(commit__author__email) not like '%anaconda%'
              and lower(commit__author__email) not like '%meta%'
              and lower(commit__author__email) not like '%facebook%'
              and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%microsoft%'
              and lower(commit__author__email) not like '%nvidia%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'google')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo
       , toYYYYMM(commit__author__date) as month


union all
-- intel
select search_key__owner,
       search_key__repo,
              'intel' as company,

       month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
            -- todo
              and (commit__author__email like '%intel%')
            and length(parents.sha) =1
          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and lower(commit__author__email) like '%intel%'
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and lower(commit__committer__email) like '%intel%'
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'intel'
                                                             or lower(company) like '%intel%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%intel%'
                                                             or lower(company) like '%intel%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where lower(company) like '%intel%'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%google%'
              and lower(commit__author__email) not like '%anaconda%'
              and lower(commit__author__email) not like '%meta%'
              and lower(commit__author__email) not like '%facebook%'
              and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%microsoft%'
              and lower(commit__author__email) not like '%nvidia%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'intel')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo
       , toYYYYMM(commit__author__date) as month


union all
-- facebook



select search_key__owner,
       search_key__repo,
              'meta' as company,

       month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
            -- todo
              and (lower(commit__author__email) like '%meta%' or lower(commit__author__email) like '%fb.com%')
              and length(parents.sha) = 1
          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and (lower(commit__author__email) like '%meta%' or lower(commit__author__email) like '%fb.com%')
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and (lower(commit__committer__email) like '%meta%' or lower(commit__committer__email) like '%fb.com%')
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'mate'
                                                             or lower(company) like '%meta%' or lower(final_company_inferred_from_company) = 'facebook'
                                                             or lower(company) like '%facebook%'  or lower(company) = 'fb')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%meta%' or lower(email) like '%fb.com%'
                                                             or lower(company) like '%facebook%' or lower(company) like '%meta%'  or lower(company) = 'fb')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where (lower(company) like '%facebook%' or lower(company) like '%meta%' or lower(company) = 'fb')
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%google%'
              and lower(commit__author__email) not like '%anaconda%'
              and lower(commit__author__email) not like '%intel%'
--               and lower(commit__author__email) not like '%facebook%'
--               and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%microsoft%'
              and lower(commit__author__email) not like '%nvidia%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'meta')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo
       , toYYYYMM(commit__author__date) as month


union all

-- microsoft
select search_key__owner,
       search_key__repo,
              'microsoft' as company,

       month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select sha, search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, author__login, commit__author__date
      from (
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
            -- todo
              and (commit__author__email like '%microsoft%')
            and length(parents.sha) = 1
          union all
          select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
            from github_commits
            where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                or (search_key__owner = 'dask' and search_key__repo = 'dask')
                or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                or (search_key__owner = 'numba' and search_key__repo = 'numba')
                or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
              and author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and lower(commit__author__email) like '%microsoft%'
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and lower(commit__committer__email) like '%microsoft%'
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where lower(final_company_inferred_from_company) = 'microsoft'
                                                             or lower(company) like '%microsoft%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%microsoft%'
                                                             or lower(company) like '%microsoft%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where lower(company) like '%microsoft%'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)
              and length(`parents.sha`) = 1
              and lower(commit__author__email) not like '%google%'
              and lower(commit__author__email) not like '%anaconda%'
              and lower(commit__author__email) not like '%meta%'
              and lower(commit__author__email) not like '%facebook%'
              and lower(commit__author__email) not like '%fb.com%'
              and lower(commit__author__email) not like '%intel%'
              and lower(commit__author__email) not like '%nvidia%'
            union all
            select search_key__owner, if(search_key__repo = 'distributed','dask',search_key__repo) as search_key__repo, sha, author__login, email, commit__author__date
            from (select search_key__owner,
                         search_key__repo,
                         sha,
                         author__login,
                         commit__author__email as email,
                         commit__author__date,
                         multiIf(endsWith(lower(email), 'google.com'), 'google',
                                 endsWith(lower(email), 'huawei.com'), 'huawei',
                                 endsWith(lower(email), 'intel.com'), 'intel',
                                 endsWith(lower(email), 'fb.com'), 'meta',
                                 endsWith(lower(email), 'meta.com'), 'intel',
                                 endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                 endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                 endsWith(lower(email), 'enthought.com'), 'enthought',
                                 endsWith(lower(email), 'continuum.io'), 'anaconda',
                                 endsWith(lower(email), 'arm.com'), 'arm',
                                 endsWith(lower(email), 'ibm.com'), 'ibm',
                                 endsWith(lower(email), 'twosigma.com'), 'twosigma',
                             -- 特殊情况 drtodd13@comcast.net
                                 (endsWith(lower(email), 'jeff@reback.net') or endsWith(lower(email), 'stan@mtrr.org')),
                                 'anaconda',
                                 (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                  endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                 (endsWith(lower(email), 'sebastian@sipsolutions.net')), 'nvidia',
                                 '')           as company
                  from github_commits
                  where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                      or (search_key__owner = 'dask' and search_key__repo = 'dask')
                      or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                      or (search_key__owner = 'numba' and search_key__repo = 'numba')
                      or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                    and length(`parents.sha`) = 1
                    and company = 'microsoft')
               )
      group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
group by search_key__owner, search_key__repo
       , toYYYYMM(commit__author__date) as month



union all

-- other
select search_key__owner,
       search_key__repo,
       'other'                       as company,

       month,
       count(distinct author__login) as author_count,
       count()                       as commit_count
from (select search_key__owner, search_key__repo, sha, author__login, commit__author__email, commit__author__date
      from github_commits
      where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
          or (search_key__owner = 'dask' and search_key__repo = 'dask')
          or (search_key__owner = 'dask' and search_key__repo = 'distributed')
          or (search_key__owner = 'numba' and search_key__repo = 'numba')
          or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
--               and (commit__author__email like '%nvidia%' or commit__author__email like '%rapidsai%' )
        and length(parents.sha) == 1)
where sha global not in (select sha
                         from (select sha,
                                      search_key__owner,
                                      if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                      author__login,
                                      commit__author__date
                               from (
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and (commit__author__email like '%nvidia%' or
                                               commit__author__email like '%rapidsai%')
                                          and length(parents.sha) == 1
                                        union all
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and author__login global in (select author__login
                                                                       from (
                                                                                -- github_commit 通过邮箱找nvidia
                                                                                select author__login
                                                                                from (select author__login
                                                                                      from github_commits
                                                                                      where author__login != ''
                                                                                        and author__login not like '%[bot]%'
                                                                                        and (lower(commit__author__email) like
                                                                                             '%nvidia%' or
                                                                                             lower(commit__author__email) like
                                                                                             '%rapidsai%')
                                                                                      group by author__login

                                                                                      union all

                                                                                      select committer__login
                                                                                      from github_commits
                                                                                      where committer__login != ''
                                                                                        and committer__login not like '%[bot]%'
                                                                                        and (lower(commit__committer__email) like
                                                                                             '%nvidia%' or
                                                                                             lower(commit__committer__email) like
                                                                                             '%rapidsai%')
                                                                                      group by committer__login)
                                                                                group by author__login

                                                                                union all
                                                                                -- 在github profile 里找
                                                                                select github_login
                                                                                from (select a.*, b.final_company_inferred_from_company
                                                                                      from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                                               left join (select company, final_company_inferred_from_company
                                                                                                          from github_profile
                                                                                                          where company != ''
                                                                                                            and final_company_inferred_from_company != ''
                                                                                                          group by company, final_company_inferred_from_company) as b
                                                                                                         on a.company = b.company
                                                                                      where lower(final_company_inferred_from_company) = 'nvidia'
                                                                                         or lower(company) like '%nvidia%'
                                                                                         or lower(company) like '%rapidsai%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                                                select github_login
                                                                                from (select commits.author_email            as email,
                                                                                             commits.author_github_login     as github_login,
                                                                                             `commits.author_github_company` as company
                                                                                      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                                      where lower(email) like '%nvidia%'
                                                                                         or lower(email) like '%rapidsai%'
                                                                                         or lower(company) like '%nvidia%'
                                                                                         or lower(company) like '%rapidsai%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 也算从profile出发
                                                                                select github_login
                                                                                from nvidia_contributor_profile_v3
                                                                                where lower(company) like '%nvidia%'
                                                                                   or lower(company) like '%rapidsai'
                                                                                group by github_login
                                                                                )
                                                                       where author__login != ''
                                                                       group by author__login)
                                          and length(`parents.sha`) = 1
                                          and lower(commit__author__email) not like '%intel%'
                                          and lower(commit__author__email) not like '%google%'
                                          and lower(commit__author__email) not like '%meta%'
                                          and lower(commit__author__email) not like '%facebook%'
                                          and lower(commit__author__email) not like '%fb.com%'
                                          and lower(commit__author__email) not like '%microsoft%'
                                          and lower(commit__author__email) not like '%anaconda%'
                                        union all
                                        select search_key__owner,
                                               if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                               sha,
                                               author__login,
                                               email,
                                               commit__author__date
                                        from (select search_key__owner,
                                                     search_key__repo,
                                                     sha,
                                                     author__login,
                                                     commit__author__email as email,
                                                     commit__author__date,
                                                     multiIf(endsWith(lower(email), 'google.com'), 'google',
                                                             endsWith(lower(email), 'huawei.com'), 'huawei',
                                                             endsWith(lower(email), 'intel.com'), 'intel',
                                                             endsWith(lower(email), 'fb.com'), 'meta',
                                                             endsWith(lower(email), 'meta.com'), 'intel',
                                                             endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'enthought.com'), 'enthought',
                                                             endsWith(lower(email), 'continuum.io'), 'anaconda',
                                                             endsWith(lower(email), 'arm.com'), 'arm',
                                                             endsWith(lower(email), 'ibm.com'), 'ibm',
                                                             endsWith(lower(email), 'twosigma.com'), 'twosigma',
                                                         -- 特殊情况 drtodd13@comcast.net
                                                             (endsWith(lower(email), 'jeff@reback.net') or
                                                              endsWith(lower(email), 'stan@mtrr.org')),
                                                             'anaconda',
                                                             (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                                              endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                                             (endsWith(lower(email), 'sebastian@sipsolutions.net')),
                                                             'nvidia',
                                                             '')           as company
                                              from github_commits
                                              where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                                  or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                                  or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                                and length(`parents.sha`) = 1
                                                and company = 'nvidia')
                                        )
                               group by sha, search_key__owner, search_key__repo, author__login, commit__author__date
                               union all
                               select sha,
                                      search_key__owner,
                                      if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                      author__login,
                                      commit__author__date
                               from (
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          -- todo
                                          and lower(commit__author__email) like '%anaconda%'
                                          and length(`parents.sha`) = 1
                                        union all
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and author__login global in (select author__login
                                                                       from (
                                                                                -- github_commit 通过邮箱找nvidia
                                                                                select author__login
                                                                                from (select author__login
                                                                                      from github_commits
                                                                                      where author__login != ''
                                                                                        and author__login not like '%[bot]%'
                                                                                        and lower(commit__author__email) like '%anaconda%'
                                                                                      group by author__login

                                                                                      union all

                                                                                      select committer__login
                                                                                      from github_commits
                                                                                      where committer__login != ''
                                                                                        and committer__login not like '%[bot]%'
                                                                                        and lower(commit__committer__email) like '%anaconda%'
                                                                                      group by committer__login)
                                                                                group by author__login

                                                                                union all
                                                                                -- 在github profile 里找
                                                                                select github_login
                                                                                from (select a.*, b.final_company_inferred_from_company
                                                                                      from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                                               left join (select company, final_company_inferred_from_company
                                                                                                          from github_profile
                                                                                                          where company != ''
                                                                                                            and final_company_inferred_from_company != ''
                                                                                                          group by company, final_company_inferred_from_company) as b
                                                                                                         on a.company = b.company
                                                                                      where lower(final_company_inferred_from_company) = 'anaconda'
                                                                                         or lower(company) like '%anaconda%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                                                select github_login
                                                                                from (select commits.author_email            as email,
                                                                                             commits.author_github_login     as github_login,
                                                                                             `commits.author_github_company` as company
                                                                                      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                                      where lower(email) like '%anaconda%'
                                                                                         or lower(company) like '%anaconda%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 也算从profile出发
                                                                                select github_login
                                                                                from nvidia_contributor_profile_v3
                                                                                where company like '%anaconda%'
                                                                                group by github_login
                                                                                )
                                                                       where author__login != ''
                                                                       group by author__login)
                                          and length(`parents.sha`) = 1
                                          and lower(commit__author__email) not like '%intel%'
                                          and lower(commit__author__email) not like '%google%'
                                          and lower(commit__author__email) not like '%meta%'
                                          and lower(commit__author__email) not like '%facebook%'
                                          and lower(commit__author__email) not like '%fb.com%'
                                          and lower(commit__author__email) not like '%microsoft%'
                                          and lower(commit__author__email) not like '%nvidia%'
                                        union all
                                        select search_key__owner,
                                               if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                               sha,
                                               author__login,
                                               email,
                                               commit__author__date
                                        from (select search_key__owner,
                                                     search_key__repo,
                                                     sha,
                                                     author__login,
                                                     commit__author__email as email,
                                                     commit__author__date,
                                                     multiIf(endsWith(lower(email), 'google.com'), 'google',
                                                             endsWith(lower(email), 'huawei.com'), 'huawei',
                                                             endsWith(lower(email), 'intel.com'), 'intel',
                                                             endsWith(lower(email), 'fb.com'), 'meta',
                                                             endsWith(lower(email), 'meta.com'), 'intel',
                                                             endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'enthought.com'), 'enthought',
                                                             endsWith(lower(email), 'continuum.io'), 'anaconda',
                                                             endsWith(lower(email), 'arm.com'), 'arm',
                                                             endsWith(lower(email), 'ibm.com'), 'ibm',
                                                             endsWith(lower(email), 'twosigma.com'), 'twosigma',
                                                         -- 特殊情况 drtodd13@comcast.net
                                                             (endsWith(lower(email), 'jeff@reback.net') or
                                                              endsWith(lower(email), 'stan@mtrr.org')),
                                                             'anaconda',
                                                             (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                                              endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                                             (endsWith(lower(email), 'sebastian@sipsolutions.net')),
                                                             'nvidia',
                                                             '')           as company
                                              from github_commits
                                              where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                                  or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                                  or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                                and length(`parents.sha`) = 1
                                                and company = 'anaconda')
                                        )
                               group by sha, search_key__owner, search_key__repo, author__login, commit__author__date
                               union all
                               select sha,
                                      search_key__owner,
                                      if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                      author__login,
                                      commit__author__date
                               from (
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          -- todo
                                          and (commit__author__email like '%google%')
                                          and length(`parents.sha`) = 1

                                        union all
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and author__login global in (select author__login
                                                                       from (
                                                                                -- github_commit 通过邮箱找nvidia
                                                                                select author__login
                                                                                from (select author__login
                                                                                      from github_commits
                                                                                      where author__login != ''
                                                                                        and author__login not like '%[bot]%'
                                                                                        and lower(commit__author__email) like '%google%'
                                                                                      group by author__login

                                                                                      union all

                                                                                      select committer__login
                                                                                      from github_commits
                                                                                      where committer__login != ''
                                                                                        and committer__login not like '%[bot]%'
                                                                                        and lower(commit__committer__email) like '%google%'
                                                                                      group by committer__login)
                                                                                group by author__login

                                                                                union all
                                                                                -- 在github profile 里找
                                                                                select github_login
                                                                                from (select a.*, b.final_company_inferred_from_company
                                                                                      from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                                               left join (select company, final_company_inferred_from_company
                                                                                                          from github_profile
                                                                                                          where company != ''
                                                                                                            and final_company_inferred_from_company != ''
                                                                                                          group by company, final_company_inferred_from_company) as b
                                                                                                         on a.company = b.company
                                                                                      where lower(final_company_inferred_from_company) = 'google'
                                                                                         or lower(company) like '%google%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                                                select github_login
                                                                                from (select commits.author_email            as email,
                                                                                             commits.author_github_login     as github_login,
                                                                                             `commits.author_github_company` as company
                                                                                      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                                      where lower(email) like '%google%'
                                                                                         or lower(company) like '%google%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 也算从profile出发
                                                                                select github_login
                                                                                from nvidia_contributor_profile_v3
                                                                                where lower(company) like '%google%'
                                                                                group by github_login
                                                                                )
                                                                       where author__login != ''
                                                                       group by author__login)
                                          and length(`parents.sha`) = 1
                                          and lower(commit__author__email) not like '%intel%'
                                          and lower(commit__author__email) not like '%anaconda%'
                                          and lower(commit__author__email) not like '%meta%'
                                          and lower(commit__author__email) not like '%facebook%'
                                          and lower(commit__author__email) not like '%fb.com%'
                                          and lower(commit__author__email) not like '%microsoft%'
                                          and lower(commit__author__email) not like '%nvidia%'
                                        union all
                                        select search_key__owner,
                                               if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                               sha,
                                               author__login,
                                               email,
                                               commit__author__date
                                        from (select search_key__owner,
                                                     search_key__repo,
                                                     sha,
                                                     author__login,
                                                     commit__author__email as email,
                                                     commit__author__date,
                                                     multiIf(endsWith(lower(email), 'google.com'), 'google',
                                                             endsWith(lower(email), 'huawei.com'), 'huawei',
                                                             endsWith(lower(email), 'intel.com'), 'intel',
                                                             endsWith(lower(email), 'fb.com'), 'meta',
                                                             endsWith(lower(email), 'meta.com'), 'intel',
                                                             endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'enthought.com'), 'enthought',
                                                             endsWith(lower(email), 'continuum.io'), 'anaconda',
                                                             endsWith(lower(email), 'arm.com'), 'arm',
                                                             endsWith(lower(email), 'ibm.com'), 'ibm',
                                                             endsWith(lower(email), 'twosigma.com'), 'twosigma',
                                                         -- 特殊情况 drtodd13@comcast.net
                                                             (endsWith(lower(email), 'jeff@reback.net') or
                                                              endsWith(lower(email), 'stan@mtrr.org')),
                                                             'anaconda',
                                                             (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                                              endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                                             (endsWith(lower(email), 'sebastian@sipsolutions.net')),
                                                             'nvidia',
                                                             '')           as company
                                              from github_commits
                                              where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                                  or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                                  or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                                and length(`parents.sha`) = 1
                                                and company = 'google')
                                        )
                               group by sha, search_key__owner, search_key__repo, author__login, commit__author__date
                               union all
                               select sha,
                                      search_key__owner,
                                      if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                      author__login,
                                      commit__author__date
                               from (
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          -- todo
                                          and (commit__author__email like '%intel%')
                                          and length(parents.sha) = 1
                                        union all
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and author__login global in (select author__login
                                                                       from (
                                                                                -- github_commit 通过邮箱找nvidia
                                                                                select author__login
                                                                                from (select author__login
                                                                                      from github_commits
                                                                                      where author__login != ''
                                                                                        and author__login not like '%[bot]%'
                                                                                        and lower(commit__author__email) like '%intel%'
                                                                                      group by author__login

                                                                                      union all

                                                                                      select committer__login
                                                                                      from github_commits
                                                                                      where committer__login != ''
                                                                                        and committer__login not like '%[bot]%'
                                                                                        and lower(commit__committer__email) like '%intel%'
                                                                                      group by committer__login)
                                                                                group by author__login

                                                                                union all
                                                                                -- 在github profile 里找
                                                                                select github_login
                                                                                from (select a.*, b.final_company_inferred_from_company
                                                                                      from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                                               left join (select company, final_company_inferred_from_company
                                                                                                          from github_profile
                                                                                                          where company != ''
                                                                                                            and final_company_inferred_from_company != ''
                                                                                                          group by company, final_company_inferred_from_company) as b
                                                                                                         on a.company = b.company
                                                                                      where lower(final_company_inferred_from_company) = 'intel'
                                                                                         or lower(company) like '%intel%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                                                select github_login
                                                                                from (select commits.author_email            as email,
                                                                                             commits.author_github_login     as github_login,
                                                                                             `commits.author_github_company` as company
                                                                                      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                                      where lower(email) like '%intel%'
                                                                                         or lower(company) like '%intel%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 也算从profile出发
                                                                                select github_login
                                                                                from nvidia_contributor_profile_v3
                                                                                where lower(company) like '%intel%'
                                                                                group by github_login
                                                                                )
                                                                       where author__login != ''
                                                                       group by author__login)
                                          and length(`parents.sha`) = 1
                                          and lower(commit__author__email) not like '%google%'
                                          and lower(commit__author__email) not like '%anaconda%'
                                          and lower(commit__author__email) not like '%meta%'
                                          and lower(commit__author__email) not like '%facebook%'
                                          and lower(commit__author__email) not like '%fb.com%'
                                          and lower(commit__author__email) not like '%microsoft%'
                                          and lower(commit__author__email) not like '%nvidia%'
                                        union all
                                        select search_key__owner,
                                               if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                               sha,
                                               author__login,
                                               email,
                                               commit__author__date
                                        from (select search_key__owner,
                                                     search_key__repo,
                                                     sha,
                                                     author__login,
                                                     commit__author__email as email,
                                                     commit__author__date,
                                                     multiIf(endsWith(lower(email), 'google.com'), 'google',
                                                             endsWith(lower(email), 'huawei.com'), 'huawei',
                                                             endsWith(lower(email), 'intel.com'), 'intel',
                                                             endsWith(lower(email), 'fb.com'), 'meta',
                                                             endsWith(lower(email), 'meta.com'), 'intel',
                                                             endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'enthought.com'), 'enthought',
                                                             endsWith(lower(email), 'continuum.io'), 'anaconda',
                                                             endsWith(lower(email), 'arm.com'), 'arm',
                                                             endsWith(lower(email), 'ibm.com'), 'ibm',
                                                             endsWith(lower(email), 'twosigma.com'), 'twosigma',
                                                         -- 特殊情况 drtodd13@comcast.net
                                                             (endsWith(lower(email), 'jeff@reback.net') or
                                                              endsWith(lower(email), 'stan@mtrr.org')),
                                                             'anaconda',
                                                             (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                                              endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                                             (endsWith(lower(email), 'sebastian@sipsolutions.net')),
                                                             'nvidia',
                                                             '')           as company
                                              from github_commits
                                              where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                                  or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                                  or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                                and length(`parents.sha`) = 1
                                                and company = 'intel')
                                        )
                               group by sha, search_key__owner, search_key__repo, author__login, commit__author__date
                               union all
                               select sha,
                                      search_key__owner,
                                      if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                      author__login,
                                      commit__author__date
                               from (
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          -- todo
                                          and (lower(commit__author__email) like '%meta%' or
                                               lower(commit__author__email) like '%fb.com%')
                                          and length(parents.sha) = 1
                                        union all
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and author__login global in (select author__login
                                                                       from (
                                                                                -- github_commit 通过邮箱找nvidia
                                                                                select author__login
                                                                                from (select author__login
                                                                                      from github_commits
                                                                                      where author__login != ''
                                                                                        and author__login not like '%[bot]%'
                                                                                        and (lower(commit__author__email) like
                                                                                             '%meta%' or
                                                                                             lower(commit__author__email) like
                                                                                             '%fb.com%')
                                                                                      group by author__login

                                                                                      union all

                                                                                      select committer__login
                                                                                      from github_commits
                                                                                      where committer__login != ''
                                                                                        and committer__login not like '%[bot]%'
                                                                                        and (lower(commit__committer__email) like
                                                                                             '%meta%' or
                                                                                             lower(commit__committer__email) like
                                                                                             '%fb.com%')
                                                                                      group by committer__login)
                                                                                group by author__login

                                                                                union all
                                                                                -- 在github profile 里找
                                                                                select github_login
                                                                                from (select a.*, b.final_company_inferred_from_company
                                                                                      from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                                               left join (select company, final_company_inferred_from_company
                                                                                                          from github_profile
                                                                                                          where company != ''
                                                                                                            and final_company_inferred_from_company != ''
                                                                                                          group by company, final_company_inferred_from_company) as b
                                                                                                         on a.company = b.company
                                                                                      where lower(final_company_inferred_from_company) = 'mate'
                                                                                         or lower(company) like '%meta%'
                                                                                         or lower(final_company_inferred_from_company) = 'facebook'
                                                                                         or lower(company) like '%facebook%'
                                                                                         or lower(company) = 'fb')
                                                                                group by github_login

                                                                                union all
                                                                                -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                                                select github_login
                                                                                from (select commits.author_email            as email,
                                                                                             commits.author_github_login     as github_login,
                                                                                             `commits.author_github_company` as company
                                                                                      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                                      where lower(email) like '%meta%'
                                                                                         or lower(email) like '%fb.com%'
                                                                                         or lower(company) like '%facebook%'
                                                                                         or lower(company) like '%meta%'
                                                                                         or lower(company) = 'fb')
                                                                                group by github_login

                                                                                union all
                                                                                -- 也算从profile出发
                                                                                select github_login
                                                                                from nvidia_contributor_profile_v3
                                                                                where (lower(company) like
                                                                                       '%facebook%' or
                                                                                       lower(company) like '%meta%' or
                                                                                       lower(company) = 'fb')
                                                                                group by github_login
                                                                                )
                                                                       where author__login != ''
                                                                       group by author__login)
                                          and length(`parents.sha`) = 1
                                          and lower(commit__author__email) not like '%google%'
                                          and lower(commit__author__email) not like '%anaconda%'
                                          and lower(commit__author__email) not like '%intel%'
--               and lower(commit__author__email) not like '%facebook%'
--               and lower(commit__author__email) not like '%fb.com%'
                                          and lower(commit__author__email) not like '%microsoft%'
                                          and lower(commit__author__email) not like '%nvidia%'
                                        union all
                                        select search_key__owner,
                                               if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                               sha,
                                               author__login,
                                               email,
                                               commit__author__date
                                        from (select search_key__owner,
                                                     search_key__repo,
                                                     sha,
                                                     author__login,
                                                     commit__author__email as email,
                                                     commit__author__date,
                                                     multiIf(endsWith(lower(email), 'google.com'), 'google',
                                                             endsWith(lower(email), 'huawei.com'), 'huawei',
                                                             endsWith(lower(email), 'intel.com'), 'intel',
                                                             endsWith(lower(email), 'fb.com'), 'meta',
                                                             endsWith(lower(email), 'meta.com'), 'intel',
                                                             endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'enthought.com'), 'enthought',
                                                             endsWith(lower(email), 'continuum.io'), 'anaconda',
                                                             endsWith(lower(email), 'arm.com'), 'arm',
                                                             endsWith(lower(email), 'ibm.com'), 'ibm',
                                                             endsWith(lower(email), 'twosigma.com'), 'twosigma',
                                                         -- 特殊情况 drtodd13@comcast.net
                                                             (endsWith(lower(email), 'jeff@reback.net') or
                                                              endsWith(lower(email), 'stan@mtrr.org')),
                                                             'anaconda',
                                                             (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                                              endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                                             (endsWith(lower(email), 'sebastian@sipsolutions.net')),
                                                             'nvidia',
                                                             '')           as company
                                              from github_commits
                                              where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                                  or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                                  or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                                and length(`parents.sha`) = 1
                                                and company = 'meta')
                                        )
                               group by sha, search_key__owner, search_key__repo, author__login, commit__author__date
                               union all
                               select sha,
                                      search_key__owner,
                                      if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                      author__login,
                                      commit__author__date
                               from (
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          -- todo
                                          and (commit__author__email like '%microsoft%')
                                          and length(parents.sha) = 1
                                        union all
                                        select search_key__owner,
                                               search_key__repo,
                                               sha,
                                               author__login,
                                               commit__author__email,
                                               commit__author__date
                                        from github_commits
                                        where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                            or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                            or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                            or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                            or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                          and author__login global in (select author__login
                                                                       from (
                                                                                -- github_commit 通过邮箱找nvidia
                                                                                select author__login
                                                                                from (select author__login
                                                                                      from github_commits
                                                                                      where author__login != ''
                                                                                        and author__login not like '%[bot]%'
                                                                                        and lower(commit__author__email) like '%microsoft%'
                                                                                      group by author__login

                                                                                      union all

                                                                                      select committer__login
                                                                                      from github_commits
                                                                                      where committer__login != ''
                                                                                        and committer__login not like '%[bot]%'
                                                                                        and lower(commit__committer__email) like '%microsoft%'
                                                                                      group by committer__login)
                                                                                group by author__login

                                                                                union all
                                                                                -- 在github profile 里找
                                                                                select github_login
                                                                                from (select a.*, b.final_company_inferred_from_company
                                                                                      from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                                               left join (select company, final_company_inferred_from_company
                                                                                                          from github_profile
                                                                                                          where company != ''
                                                                                                            and final_company_inferred_from_company != ''
                                                                                                          group by company, final_company_inferred_from_company) as b
                                                                                                         on a.company = b.company
                                                                                      where lower(final_company_inferred_from_company) = 'microsoft'
                                                                                         or lower(company) like '%microsoft%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                                                select github_login
                                                                                from (select commits.author_email            as email,
                                                                                             commits.author_github_login     as github_login,
                                                                                             `commits.author_github_company` as company
                                                                                      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                                      where lower(email) like '%microsoft%'
                                                                                         or lower(company) like '%microsoft%')
                                                                                group by github_login

                                                                                union all
                                                                                -- 也算从profile出发
                                                                                select github_login
                                                                                from nvidia_contributor_profile_v3
                                                                                where lower(company) like '%microsoft%'
                                                                                group by github_login
                                                                                )
                                                                       where author__login != ''
                                                                       group by author__login)
                                          and length(`parents.sha`) = 1
                                          and lower(commit__author__email) not like '%google%'
                                          and lower(commit__author__email) not like '%anaconda%'
                                          and lower(commit__author__email) not like '%meta%'
                                          and lower(commit__author__email) not like '%facebook%'
                                          and lower(commit__author__email) not like '%fb.com%'
                                          and lower(commit__author__email) not like '%intel%'
                                          and lower(commit__author__email) not like '%nvidia%'
                                        union all
                                        select search_key__owner,
                                               if(search_key__repo = 'distributed', 'dask', search_key__repo) as search_key__repo,
                                               sha,
                                               author__login,
                                               email,
                                               commit__author__date
                                        from (select search_key__owner,
                                                     search_key__repo,
                                                     sha,
                                                     author__login,
                                                     commit__author__email as email,
                                                     commit__author__date,
                                                     multiIf(endsWith(lower(email), 'google.com'), 'google',
                                                             endsWith(lower(email), 'huawei.com'), 'huawei',
                                                             endsWith(lower(email), 'intel.com'), 'intel',
                                                             endsWith(lower(email), 'fb.com'), 'meta',
                                                             endsWith(lower(email), 'meta.com'), 'intel',
                                                             endsWith(lower(email), 'microsoft.com'), 'microsoft',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'anaconda.com'), 'anaconda',
                                                             endsWith(lower(email), 'nvidia.com'), 'nvidia',
                                                             endsWith(lower(email), 'enthought.com'), 'enthought',
                                                             endsWith(lower(email), 'continuum.io'), 'anaconda',
                                                             endsWith(lower(email), 'arm.com'), 'arm',
                                                             endsWith(lower(email), 'ibm.com'), 'ibm',
                                                             endsWith(lower(email), 'twosigma.com'), 'twosigma',
                                                         -- 特殊情况 drtodd13@comcast.net
                                                             (endsWith(lower(email), 'jeff@reback.net') or
                                                              endsWith(lower(email), 'stan@mtrr.org')),
                                                             'anaconda',
                                                             (endsWith(lower(email), 'johannes@sipsolutions.net') or
                                                              endsWith(lower(email), 'drtodd13@comcast.net')), 'intel',
                                                             (endsWith(lower(email), 'sebastian@sipsolutions.net')),
                                                             'nvidia',
                                                             '')           as company
                                              from github_commits
                                              where ((search_key__owner = 'numpy' and search_key__repo = 'numpy')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'dask')
                                                  or (search_key__owner = 'dask' and search_key__repo = 'distributed')
                                                  or (search_key__owner = 'numba' and search_key__repo = 'numba')
                                                  or (search_key__owner = 'pandas-dev' and search_key__repo = 'pandas'))
                                                and length(`parents.sha`) = 1
                                                and company = 'microsoft')
                                        )
                               group by sha, search_key__owner, search_key__repo, author__login, commit__author__date)
                         group by sha)
group by search_key__owner, search_key__repo
       , toYYYYMM(commit__author__date) as month



-- 指定某个人 查看表nvidia_contributor_pr_v3 查询这个人使用每个邮箱的起始时间和最新时间
select email,min(date) as start,max(date) as end
from (select
          owner,repo,
          commits.author_email            as email,
             commits.author_github_login     as github_login,
             `commits.author_github_company` as company
            ,commits.author_date as date
      from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`,commits.author_date
--       where
--           lower(email) like '%nvidia%'
--          or lower(company) like '%nvidia%'
         )
where github_login = 'teoliphant' group by email order by start








-- 查看两种公司开发者圈子是否有交叉（共同的开发者） （使用找厂商四步走）

select *
from (select author__login
      from (
               -- github_commit 通过邮箱找nvidia
               select author__login
               from (select author__login
                     from github_commits
                     where author__login != ''
                       and author__login not like '%[bot]%'
                       and lower(commit__author__email) like '%nvidia%'
                     group by author__login

                     union all

                     select committer__login
                     from github_commits
                     where committer__login != ''
                       and committer__login not like '%[bot]%'
                       and lower(commit__committer__email) like '%nvidia%'
                     group by committer__login)
               group by author__login

               union all
               -- 在github profile 里找
               select github_login
               from (select a.*, b.final_company_inferred_from_company
                     from (select github_login, company from nvidia_contributor_profile_v3) as a global
                              left join (select company, final_company_inferred_from_company
                                         from github_profile
                                         where company != ''
                                           and final_company_inferred_from_company != ''
                                         group by company, final_company_inferred_from_company) as b
                                        on a.company = b.company
                     where final_company_inferred_from_company = 'nvidia'
                        or lower(company) like '%nvidia%'
                        or lower(company) like '%rapidsai%')
               group by github_login

               union all
               -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
               select github_login
               from (select commits.author_email            as email,
                            commits.author_github_login     as github_login,
                            `commits.author_github_company` as company
                     from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                     where lower(email) like '%nvidia%'
                        or lower(company) like '%nvidia%'
                        or lower(company) like '%rapidsai%')
               group by github_login

               union all
               -- 也算从profile出发
               select github_login
               from nvidia_contributor_profile_v3
               where company like '%nvidia%'
                  or company like '%rapidsai'
               group by github_login
               )
      where author__login != ''
      group by author__login) as a global
         join (select author__login
               from (
                        -- github_commit 通过邮箱找nvidia
                        select author__login
                        from (select author__login
                              from github_commits
                              where author__login != ''
                                and author__login not like '%[bot]%'
                                and lower(commit__author__email) like '%intel%'
                              group by author__login

                              union all

                              select committer__login
                              from github_commits
                              where committer__login != ''
                                and committer__login not like '%[bot]%'
                                and lower(commit__committer__email) like '%intel%'
                              group by committer__login)
                        group by author__login

                        union all
                        -- 在github profile 里找
                        select github_login
                        from (select a.*, b.final_company_inferred_from_company
                              from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                       left join (select company, final_company_inferred_from_company
                                                  from github_profile
                                                  where company != ''
                                                    and final_company_inferred_from_company != ''
                                                  group by company, final_company_inferred_from_company) as b
                                                 on a.company = b.company
                              where final_company_inferred_from_company = 'intel'
                                 or lower(company) like '%intel%'
                                 )
                        group by github_login

                        union all
                        -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                        select github_login
                        from (select commits.author_email            as email,
                                     commits.author_github_login     as github_login,
                                     `commits.author_github_company` as company
                              from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                              where lower(email) like '%intel%'
                                 or lower(company) like '%intel%'
                                 )
                        group by github_login

                        union all
                        -- 也算从profile出发
                        select github_login
                        from nvidia_contributor_profile_v3
                        where company like '%intel%'
                        group by github_login
                        )
               where author__login != ''
               group by author__login) as b on a.author__login = b.author__login






-- 指定开发者使用表nvidia_contributor_pr_v3 查看这个开发者在哪一天参与贡献并使用什么邮箱
select * from (select *
      from (select email, github_login, day
            from (select commits.author_email                                            as email,
                         commits.author_github_login                                     as github_login,
                         commits.author_date                                             as date,
                         toInt64(concat(splitByChar('-', date)[1], splitByChar('-', date)[2],
                                        splitByChar('T', splitByChar('-', date)[3])[1])) as day
                  from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, commits.author_date
                  where github_login != '')
            --           lower(email) like '%nvidia%'
--          or lower(company) like '%nvidia%'

            union all

            select commit__author__email,

                   author__login,
                   toYYYYMMDD(commit__author__date) as day
            from github_commits
            where author__login != '')
      order by github_login, day) where github_login ='TheNeuralBit'























-- 指定项目 指定公司（找公司四步走） 查看 这个公司在该项目中某目录的添加行数
with 'pandas-dev' as owner, 'pandas' as repo
select search_key__owner, search_key__repo, dir, sum(insertions) as total_insertions
from (select search_key__owner,
             search_key__repo,
             message,
             `files.file_name`  as file_name,
             `files.insertions` as insertions,
             --  if中 file_name 切片长度大于几 后边就拼接几
                   if(length(splitByChar('/', file_name)) > 3,
                concat(splitByChar('/', file_name)[1], '/', splitByChar('/', file_name)[2], '/',splitByChar('/', file_name)[3]
--                     , '/',splitByChar('/', file_name)[4]
                    )
                       , '') as dir
      from (select a.*, b.author__login
            from (select *
                  from gits
                  where search_key__owner = owner
                    and search_key__repo = repo
                    and length(parents) = 1
--                     and toYYYYMMDD(authored_date) > 20220518
                     ) as a global
                     join (select author__login, commit__author__email
                           from github_commits
                           where author__login != '' and  lower(commit__author__email) not like '%intel%'
  and lower(commit__author__email) not like '%google%'
  and lower(commit__author__email) not like '%meta%'
  and lower(commit__author__email) not like '%facebook%'
  and lower(commit__author__email) not like '%fb.com%'
  and lower(commit__author__email) not like '%microsoft%'
                           group by author__login, commit__author__email) as b
                          on a.author_email = b.commit__author__email
            where author__login global in (select author__login
                                           from (
                                                    -- github_commit 通过邮箱找nvidia
                                                    select author__login
                                                    from (select author__login
                                                          from github_commits
                                                          where author__login != ''
                                                            and author__login not like '%[bot]%'
                                                            and lower(commit__author__email) like '%nvidia%'
                                                          group by author__login

                                                          union all

                                                          select committer__login
                                                          from github_commits
                                                          where committer__login != ''
                                                            and committer__login not like '%[bot]%'
                                                            and lower(commit__committer__email) like '%nvidia%'
                                                          group by committer__login)
                                                    group by author__login

                                                    union all
                                                    -- 在github profile 里找
                                                    select github_login
                                                    from (select a.*, b.final_company_inferred_from_company
                                                          from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                   left join (select company, final_company_inferred_from_company
                                                                              from github_profile
                                                                              where company != ''
                                                                                and final_company_inferred_from_company != ''
                                                                              group by company, final_company_inferred_from_company) as b
                                                                             on a.company = b.company
                                                          where final_company_inferred_from_company = 'nvidia'
                                                             or lower(company) like '%nvidia%'
                                                             or lower(company) like '%rapidsai%')
                                                    group by github_login

                                                    union all
                                                    -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                    select github_login
                                                    from (select commits.author_email            as email,
                                                                 commits.author_github_login     as github_login,
                                                                 `commits.author_github_company` as company
                                                          from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                          where lower(email) like '%nvidia%'
                                                             or lower(company) like '%nvidia%'
                                                             or lower(company) like '%rapidsai%')
                                                    group by github_login

                                                    union all
                                                    -- 也算从profile出发
                                                    select github_login
                                                    from nvidia_contributor_profile_v3
                                                    where company like '%nvidia%'
                                                       or company like '%rapidsai'
                                                    group by github_login
                                                    )
                                           where author__login != ''
                                           group by author__login)) array join `files.insertions`, `files.file_name`
         )
where file_name not like '%test%'
  and file_name not like '%doc%' and dir !=''
group by search_key__owner, search_key__repo, dir
order by total_insertions desc
limit 50





-- 指定开发者列表和目录找与目录相关的message信息


with 'scipy' as owner, 'scipy' as repo, 'scipy/signal/_upfirdn_apply.pyx' as file_name_
select search_key__owner,
       search_key__repo,
       message,
       `files.file_name`  as file_name,
       `files.insertions` as insertions,
       `files.deletions`  as deletions
--        ,
--        `files.insertions`,
--        `files.deletions`
--              if(length(splitByChar('/', file_name)) > 4,
--                 concat(splitByChar('/', file_name)[1], '/', splitByChar('/', file_name)[2], '/',splitByChar('/', file_name)[3], '/',splitByChar('/', file_name)[4]), '') as dir
from (select a.*, b.author__login
      from (select *
            from gits
            where search_key__owner = owner
              and search_key__repo = repo
              and length(parents) = 1) as a global
               join (select author__login, commit__author__email
                     from github_commits
                     where search_key__owner = owner
                       and search_key__repo = repo
                       and author__login != ''
                     group by author__login, commit__author__email) as b
                    on a.author_email = b.commit__author__email
      where author__login global in
                (select author__login
                                     from (
                                              -- github_commit 通过邮箱找nvidia
                                              select author__login
                                              from (select author__login
                                                    from github_commits
                                                    where author__login != ''
                                                      and author__login not like '%[bot]%'
                                                      and (lower(commit__author__email) like '%nvidia%' or
                                                           lower(commit__author__email) like '%rapidsai%')
                                                    group by author__login

                                                    union all

                                                    select committer__login
                                                    from github_commits
                                                    where committer__login != ''
                                                      and committer__login not like '%[bot]%'
                                                      and (lower(commit__committer__email) like '%nvidia%' or
                                                           lower(commit__committer__email) like '%rapidsai%')
                                                    group by committer__login)
                                              group by author__login

                                              union all
                                              -- 在github profile 里找
                                              select github_login
                                              from (select a.*, b.final_company_inferred_from_company
                                                    from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                             left join (select company, final_company_inferred_from_company
                                                                        from github_profile
                                                                        where company != ''
                                                                          and final_company_inferred_from_company != ''
                                                                        group by company, final_company_inferred_from_company) as b
                                                                       on a.company = b.company
                                                    where lower(final_company_inferred_from_company) = 'nvidia'
                                                       or lower(company) like '%nvidia%'
                                                       or lower(company) like '%rapidsai%')
                                              group by github_login

                                              union all
                                              -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                              select github_login
                                              from (select commits.author_email            as email,
                                                           commits.author_github_login     as github_login,
                                                           `commits.author_github_company` as company
                                                    from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                    where lower(email) like '%nvidia%'
                                                       or lower(email) like '%rapidsai%'
                                                       or lower(company) like '%nvidia%'
                                                       or lower(company) like '%rapidsai%')
                                              group by github_login

                                              union all
                                              -- 也算从profile出发
                                              select github_login
                                              from nvidia_contributor_profile_v3
                                              where lower(company) like '%nvidia%'
                                                 or lower(company) like '%rapidsai'
                                              group by github_login
                                              )
                                     where author__login != ''
                                     group by author__login)) array join `files.insertions`, `files.file_name`, `files.deletions`
where file_name = file_name_
order by insertions desc
;




-- 通过nvidia_contributor_pr_v3 的title 查找 是否项目中 英伟达的贡献是 与gpu相关的
select *
from nvidia_contributor_pr_v3
where owner = 'dask'
  and repo = 'dask'
  and github_login global in (select author__login
                                     from (
                                              -- github_commit 通过邮箱找nvidia
                                              select author__login
                                              from (select author__login
                                                    from github_commits
                                                    where author__login != ''
                                                      and author__login not like '%[bot]%'
                                                      and (lower(commit__author__email) like '%nvidia%' or
                                                           lower(commit__author__email) like '%rapidsai%')
                                                    group by author__login

                                                    union all

                                                    select committer__login
                                                    from github_commits
                                                    where committer__login != ''
                                                      and committer__login not like '%[bot]%'
                                                      and (lower(commit__committer__email) like '%nvidia%' or
                                                           lower(commit__committer__email) like '%rapidsai%')
                                                    group by committer__login)
                                              group by author__login

                                              union all
                                              -- 在github profile 里找
                                              select github_login
                                              from (select a.*, b.final_company_inferred_from_company
                                                    from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                             left join (select company, final_company_inferred_from_company
                                                                        from github_profile
                                                                        where company != ''
                                                                          and final_company_inferred_from_company != ''
                                                                        group by company, final_company_inferred_from_company) as b
                                                                       on a.company = b.company
                                                    where lower(final_company_inferred_from_company) = 'nvidia'
                                                       or lower(company) like '%nvidia%'
                                                       or lower(company) like '%rapidsai%')
                                              group by github_login

                                              union all
                                              -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                              select github_login
                                              from (select commits.author_email            as email,
                                                           commits.author_github_login     as github_login,
                                                           `commits.author_github_company` as company
                                                    from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                    where lower(email) like '%nvidia%'
                                                       or lower(email) like '%rapidsai%'
                                                       or lower(company) like '%nvidia%'
                                                       or lower(company) like '%rapidsai%')
                                              group by github_login

                                              union all
                                              -- 也算从profile出发
                                              select github_login
                                              from nvidia_contributor_profile_v3
                                              where lower(company) like '%nvidia%'
                                                 or lower(company) like '%rapidsai'
                                              group by github_login
                                              )
                                     where author__login != ''
                                     group by author__login)
  and lower(title) like '%gpu%'
  and merged = 1
;





-- 按照partition 优化表（主要去重表）
optimize table github_commits_local on cluster replicated partition 'NVIDIA-Omniverse'








-- 分项目 nvidia 每个项目的人数和贡献量和pr量 topn 使用了nvidia_contributor_pr_v3表看pr总体
select concat('https://github.com/', owner, '/', repo) as url,
       owner,
       repo,
--        year_month,
       count(distinct commit_login)                    as author_count,
--        arrayDistinct(groupArray(commit_login)),
       count()                                         as commit_count,
       count(distinct id) as pr_count
from (select owner,
             repo,
             id,
             github_login,
             commits.author_github_login                    as commit_login,
             commits.oid                                    as sha,
             commits.author_email                           as email,
             commits.author_date                            as date,
             splitByChar('-', date)[1]                      as year,
             splitByChar('-', date)[2]                      as month,
             splitByChar('T', splitByChar('-', date)[3])[1] as day
      from nvidia_contributor_pr_v3 array join commits.author_github_login, commits.oid, commits.author_email, commits.author_date
      where (commit_login global in (select author__login
                                     from (
                                              -- github_commit 通过邮箱找nvidia
                                              select author__login
                                              from (select author__login
                                                    from github_commits
                                                    where author__login != ''
                                                      and author__login not like '%[bot]%'
                                                      and (lower(commit__author__email) like '%nvidia%' or
                                                           lower(commit__author__email) like '%rapidsai%')
                                                    group by author__login

                                                    union all

                                                    select committer__login
                                                    from github_commits
                                                    where committer__login != ''
                                                      and committer__login not like '%[bot]%'
                                                      and (lower(commit__committer__email) like '%nvidia%' or
                                                           lower(commit__committer__email) like '%rapidsai%')
                                                    group by committer__login)
                                              group by author__login

                                              union all
                                              -- 在github profile 里找
                                              select github_login
                                              from (select a.*, b.final_company_inferred_from_company
                                                    from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                             left join (select company, final_company_inferred_from_company
                                                                        from github_profile
                                                                        where company != ''
                                                                          and final_company_inferred_from_company != ''
                                                                        group by company, final_company_inferred_from_company) as b
                                                                       on a.company = b.company
                                                    where lower(final_company_inferred_from_company) = 'nvidia'
                                                       or lower(company) like '%nvidia%'
                                                       or lower(company) like '%rapidsai%')
                                              group by github_login

                                              union all
                                              -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                              select github_login
                                              from (select commits.author_email            as email,
                                                           commits.author_github_login     as github_login,
                                                           `commits.author_github_company` as company
                                                    from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                    where lower(email) like '%nvidia%'
                                                       or lower(email) like '%rapidsai%'
                                                       or lower(company) like '%nvidia%'
                                                       or lower(company) like '%rapidsai%')
                                              group by github_login

                                              union all
                                              -- 也算从profile出发
                                              select github_login
                                              from nvidia_contributor_profile_v3
                                              where lower(company) like '%nvidia%'
                                                 or lower(company) like '%rapidsai'
                                              group by github_login
                                              )
                                     where author__login != ''
                                     group by author__login)
          or (lower(email) like '%nvidia%' or lower(email) like '%rapidsai%'))
        and (lower(email) not like '%intel%'
          and lower(email) not like '%google%'
          and lower(email) not like '%meta%'
          and lower(email) not like '%facebook%'
          and lower(email) not like '%fb.com%'
          and lower(email) not like '%microsoft%'
          and lower(email) not like '%anaconda%'
          and lower(email) not like '%amazon%'
          and lower(email) not like '%ibm.com%'
          and lower(email) not like '%arm.com%'
          and lower(email) not like '%apple.com%'
          and isFork = 0
          ) )
group by owner, repo
--        , toInt32(concat(year, month)) as year_month
having author_count>1 and commit_count >=10
order by
--     year_month,
    commit_count desc,
    author_count desc




-- 通过github pr event 找出的topn项目 筛选指标有，pr发起者数量，pr发起者人均pr数量（可指定项目）
select *,
       RANK() OVER (ORDER BY month_count desc ,
           total_pr_count desc ,user_count_per_month desc) AS rank
--     count()
from (select concat('https://github.com/', search_key__owner, '/', search_key__repo) as url,
             search_key__owner,
             search_key__repo,
             count()                                                                 as month_count,
             sum(pr_count)                                                           as total_pr_count,
             sum(dev_count)                                                          as total_dev_count,
             round(total_pr_count / total_dev_count, 1)                              as pr_count_per_user,
             round(total_dev_count / month_count, 1)                                 as user_count_per_month
      from (select search_key__owner,
                   search_key__repo,
                   month,
                   count()                      as pr_count,
                   count(distinct actor__login) as dev_count
            from (select search_key__owner,
                         search_key__repo,
--                          if(payload__pull_request__merged = 'True',1,0) as payload__pull_request__merged,
                         created_at,
                         payload__pull_request__state,
                         payload__pull_request__head__user__login,
                         actor__login
                  from cleaned_mini_pull_request_event
                  union all
                  select search_key__owner,
                         search_key__repo,
--                          payload__pull_request__merged,
                         created_at,
                         payload__pull_request__state,
                         payload__pull_request__head__user__login,
                         actor__login
                  from pull_request_event
                  where toYear(created_at) global in (2022, 2021)
                     )
            where
--                 toYear(created_at) < 2023
--               and
                payload__pull_request__state = 'open'
              and payload__pull_request__head__user__login not like '%[bot]%'
              and payload__pull_request__head__user__login != 'dependabot'
              and payload__pull_request__head__user__login not like '%-bot%'
            --               and (
--                   (search_key__owner = 'numba' and search_key__repo = 'numba')
            --or
--                    (search_key__owner = 'rust-lang' and search_key__repo = 'rust') or
--                    (search_key__owner = 'tensorflow' and search_key__repo = 'tensorflow') or
--                    (search_key__owner = 'cupy' and search_key__repo = 'cupy') or
--                    (search_key__owner = 'rapidsai' and search_key__repo = 'cudf') or
--                    (search_key__owner = 'NixOS' and search_key__repo = 'nixpkgs')
--                    (search_key__owner = 'NixOS' and search_key__repo = 'nixpkgs')
--                 )
            group by search_key__owner, search_key__repo, toYYYYMM(created_at) as month)
      group by search_key__owner, search_key__repo
      having pr_count_per_user < 100
--           month_count >=3 and month_count<12
         and month_count != total_dev_count
         and user_count_per_month >= 10
         and total_dev_count < 10000
         and pr_count_per_user > 2
      order by month_count desc
             , user_count_per_month desc, total_pr_count desc
      limit 1500)
order by month_count desc,
         total_pr_count desc, user_count_per_month desc




-- github pr event 每个项目每个月投入人数和pr数（可指定项目）


select search_key__owner,
       search_key__repo,
       month,
       count()                      as pr_count,
       count(distinct actor__login) as dev_count
from (select search_key__owner,
             search_key__repo,
--                          if(payload__pull_request__merged = 'True',1,0) as payload__pull_request__merged,
             created_at,
             payload__pull_request__state,
             payload__pull_request__head__user__login,
             actor__login
      from cleaned_mini_pull_request_event
      union all
      select search_key__owner,
             search_key__repo,
--                          payload__pull_request__merged,
             created_at,
             payload__pull_request__state,
             payload__pull_request__head__user__login,
             actor__login
      from pull_request_event
      where toYear(created_at) global in (2022, 2021)
         )
where
--                 toYear(created_at) < 2023
--               and
    payload__pull_request__state = 'open'
  and payload__pull_request__head__user__login not like '%[bot]%'
  and payload__pull_request__head__user__login != 'dependabot'
  and payload__pull_request__head__user__login not like '%-bot%'
--               and (
--                   (search_key__owner = 'numba' and search_key__repo = 'numba')
--or
--                    (search_key__owner = 'rust-lang' and search_key__repo = 'rust') or
--                    (search_key__owner = 'tensorflow' and search_key__repo = 'tensorflow') or
--                    (search_key__owner = 'cupy' and search_key__repo = 'cupy') or
--                    (search_key__owner = 'rapidsai' and search_key__repo = 'cudf') or
--                    (search_key__owner = 'NixOS' and search_key__repo = 'nixpkgs')
--                    (search_key__owner = 'NixOS' and search_key__repo = 'nixpkgs')
--                 )
group by search_key__owner, search_key__repo, toYYYYMM(created_at) as month



-- 获取github 上大厂邮箱的owner repo sha 并且每个邮箱只取一条sha 作为调api获取大厂开发者github 信息的前置输入
select *
from (select search_key__owner, search_key__repo, sha, email
      from (select search_key__owner,
                   search_key__repo,
                   `payload__commits.sha`           as sha,
                   `payload__commits.author__email` as email,
                   `payload__commits.url`           as url,
                   `payload__commits.distinct`      as distinct,
                   created_at
            from cleaned_mini_push_event_v2 array join `payload__commits.sha`, `payload__commits.author__email`, `payload__commits.url`, `payload__commits.distinct`
            where
                          (
                           lower(email) like '%fb.com%' or
                           lower(email) like '%meta.com%' or
                           lower(email) like '%google.com%' or
                           lower(email) like '%intel.com%' or
                           lower(email) like '%facebook.com%' or
                           lower(email) like '%nvidia.com%'
                              )
                          and search_key__gh_archive_year = '2024'
                      )
            group by search_key__owner, search_key__repo, sha, email
            limit 1 by sha)
      limit 1 by email



-- 多个项目的共同开发者
select *
from (select search_key__owner, search_key__repo, author__login, count() as commit_count
      from github_commits
      where (
              (search_key__owner = 'pandas-dev'
                  and search_key__repo = 'pandas') or (search_key__owner = 'numba'
              and search_key__repo = 'numba') or (search_key__owner = 'numpy'
              and search_key__repo = 'numpy') or (search_key__owner = 'llvm'
              and search_key__repo = 'llvm-project') or
          (search_key__owner = 'cupy'
              and search_key__repo = 'cupy')
          )


        and length(parents.sha) = 1
        and author__login global in (
          -- github_commit 通过邮箱找nvidia
          select author__login
          from (select author__login
                from github_commits
                where author__login != ''
                  and author__login not like '%[bot]%'
                  and lower(commit__author__email) like '%nvidia%'
                group by author__login

                union all

                select committer__login
                from github_commits
                where committer__login != ''
                  and committer__login not like '%[bot]%'
                  and lower(commit__committer__email) like '%nvidia%'
                group by committer__login)
          group by author__login

          union all
          -- 在github profile 里找
          select github_login
          from (select a.*, b.final_company_inferred_from_company
                from (select github_login, company from nvidia_contributor_profile_v3) as a global
                         left join (select company, final_company_inferred_from_company
                                    from github_profile
                                    where company != ''
                                      and final_company_inferred_from_company != ''
                                    group by company, final_company_inferred_from_company) as b
                                   on a.company = b.company
                where (final_company_inferred_from_company = 'nvidia'
                    or lower(company) like '%nvidia%'
                    or lower(company) like '%rapidsai%')
                  and github_login != '')

          group by github_login

          union all
          -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
          select github_login
          from (select commits.author_email            as email,
                       commits.author_github_login     as github_login,
                       `commits.author_github_company` as company
                from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                where (lower(email) like '%nvidia%'
                    or lower(company) like '%nvidia%'
                    or lower(company) like '%rapidsai%')
                  and github_login != '')
          group by github_login

          union all
          -- 也算从profile出发
          select github_login
          from nvidia_contributor_profile_v3
          where company like '%nvidia%'
             or company like '%rapidsai'
          group by github_login)
        and (lower(commit__author__email) not like '%intel%'
          and lower(commit__author__email) not like '%google%'
          and lower(commit__author__email) not like '%meta%'
          and lower(commit__author__email) not like '%facebook%'
          and lower(commit__author__email) not like '%fb.com%'
          and lower(commit__author__email) not like '%microsoft%'
          and lower(commit__author__email) not like '%anaconda%')
      group by search_key__owner, search_key__repo, author__login) as a global
         join (select search_key__owner, search_key__repo, author__login, count() as commit_count
               from github_commits
               where (
                       (search_key__owner = 'pandas-dev'
                           and search_key__repo = 'pandas') or (search_key__owner = 'numba'
                       and search_key__repo = 'numba') or (search_key__owner = 'numpy'
                       and search_key__repo = 'numpy') or (search_key__owner = 'llvm'
                       and search_key__repo = 'llvm-project') or (search_key__owner = 'cupy'
                       and search_key__repo = 'cupy')
                   )
                 and length(parents.sha) = 1
                 and author__login global in (
                   -- github_commit 通过邮箱找nvidia
                   select author__login
                   from (select author__login
                         from github_commits
                         where author__login != ''
                           and author__login not like '%[bot]%'
                           and lower(commit__author__email) like '%nvidia%'
                         group by author__login

                         union all

                         select committer__login
                         from github_commits
                         where committer__login != ''
                           and committer__login not like '%[bot]%'
                           and lower(commit__committer__email) like '%nvidia%'
                         group by committer__login)
                   group by author__login

                   union all
                   -- 在github profile 里找
                   select github_login
                   from (select a.*, b.final_company_inferred_from_company
                         from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                  left join (select company, final_company_inferred_from_company
                                             from github_profile
                                             where company != ''
                                               and final_company_inferred_from_company != ''
                                             group by company, final_company_inferred_from_company) as b
                                            on a.company = b.company
                         where (final_company_inferred_from_company = 'nvidia'
                             or lower(company) like '%nvidia%'
                             or lower(company) like '%rapidsai%')
                           and github_login != '')

                   group by github_login

                   union all
                   -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                   select github_login
                   from (select commits.author_email            as email,
                                commits.author_github_login     as github_login,
                                `commits.author_github_company` as company
                         from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                         where (lower(email) like '%nvidia%'
                             or lower(company) like '%nvidia%'
                             or lower(company) like '%rapidsai%')
                           and github_login != '')
                   group by github_login

                   union all
                   -- 也算从profile出发
                   select github_login
                   from nvidia_contributor_profile_v3
                   where company like '%nvidia%'
                      or company like '%rapidsai'
                   group by github_login)
                 and (lower(commit__author__email) not like '%intel%'
                   and lower(commit__author__email) not like '%google%'
                   and lower(commit__author__email) not like '%meta%'
                   and lower(commit__author__email) not like '%facebook%'
                   and lower(commit__author__email) not like '%fb.com%'
                   and lower(commit__author__email) not like '%microsoft%'
                   and lower(commit__author__email) not like '%anaconda%')
               group by search_key__owner, search_key__repo, author__login
               order by commit_count desc) as b on a.author__login = b.author__login
where a.search_key__owner != b.search_key__owner
  and a.search_key__repo != b.search_key__repo
order by search_key__owner



-- 指定项目和公司通过gits表查看每个月 commit 提交者人数 提交次数 代码行数
select search_key__owner,
       search_key__repo,
       month,
       count(distinct author__login) as author_count,
       count()                       as total_commit_count,
       sum(total__lines)             as alter_code_lines,
       sum(total__deletions) as deletions_lines,
       sum(total__insertions) as insertions_lines
from (select *
      from gits
      where
--           (
--               (search_key__owner = 'NVIDIA' and search_key__repo = 'TensorRT')
--               or
--               (search_key__owner = 'NVIDIA' and search_key__repo = 'TensorRT-LLM') or
--               (search_key__owner = 'pytorch' and search_key__repo = 'TensorRT') or
--               (search_key__owner = 'tensorflow' and search_key__repo = 'tensorrt')
--               or
--               (search_key__owner = 'onnx' and search_key__repo = 'onnx-tensorrt')
--           )
          (

(search_key__owner = 'NVIDIA' and search_key__repo = 'gpu-operator') or
(search_key__owner = 'NVIDIA' and search_key__repo = 'k8s-device-plugin') or
(search_key__owner = 'NVIDIA' and search_key__repo = 'libnvidia-container') or
(search_key__owner = 'NVIDIA' and search_key__repo = 'ais-k8s') or
(search_key__owner = 'NVIDIA' and search_key__repo = 'knavigator') or
(search_key__owner = 'NVIDIA' and search_key__repo = 'k8s-dra-driver') or
(search_key__owner = 'NVIDIA' and search_key__repo = 'vgpu-device-manager') or
(search_key__owner = 'NVIDIA' and search_key__repo = 'kubectl-nv') or
(search_key__owner = 'NVIDIA' and search_key__repo = 'k8s-driver-manager') or
(search_key__owner = 'Mellanox' and search_key__repo = 'network-operator') or
    (search_key__owner = 'NVIDIA' and search_key__repo = 'cloud-native-stack')
    )
        and length(parents) = 1) as a global
         join (
    select author__login, commit__author__email
    from github_commits
    where author__id != 0
      and author__login global in (select author__login
                                   from (
                                            -- github_commit 通过邮箱找nvidia
                                            select author__login
                                            from (select author__login
                                                  from github_commits
                                                  where author__login != ''
                                                    and author__login not like '%[bot]%'
                                                    and lower(commit__author__email) like '%nvidia%'
                                                  group by author__login

                                                  union all

                                                  select committer__login
                                                  from github_commits
                                                  where committer__login != ''
                                                    and committer__login not like '%[bot]%'
                                                    and lower(commit__committer__email) like '%nvidia%'
                                                  group by committer__login)
                                            group by author__login

                                            union all
                                            -- 在github profile 里找
                                            select github_login
                                            from (select a.*, b.final_company_inferred_from_company
                                                  from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                           left join (select company, final_company_inferred_from_company
                                                                      from github_profile
                                                                      where company != ''
                                                                        and final_company_inferred_from_company != ''
                                                                      group by company, final_company_inferred_from_company) as b
                                                                     on a.company = b.company
                                                  where (final_company_inferred_from_company = 'nvidia'
                                                      or lower(company) like '%nvidia%'
                                                      or lower(company) like '%rapidsai%')
                                                    and github_login != '')

                                            group by github_login

                                            union all
                                            -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                            select github_login
                                            from (select commits.author_email            as email,
                                                         commits.author_github_login     as github_login,
                                                         `commits.author_github_company` as company
                                                  from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                  where (lower(email) like '%nvidia%'
                                                      or lower(company) like '%nvidia%'
                                                      or lower(company) like '%rapidsai%')
                                                    and github_login != '')
                                            group by github_login

                                            union all
                                            -- 也算从profile出发
                                            select github_login
                                            from nvidia_contributor_profile_v3
                                            where company like '%nvidia%'
                                               or company like '%rapidsai'
                                            group by github_login)
                                   where author__login != ''
                                   group by author__login)
    group by author__login, commit__author__email
    ) as b on a.author_email = b.commit__author__email

group by search_key__owner, search_key__repo, toYYYYMM(authored_date) as month
order by month


-- 查看项目最新时间和count数
with ((search_key__owner = 'apache' and search_key__repo = 'spark') or
       (search_key__owner = 'NVIDIA' and search_key__repo = 'spark-rapids') or
       (search_key__owner = 'nuclio' and search_key__repo = 'nuclio')) as owner_repo
select 'gits' as type, search_key__owner, search_key__repo, max(authored_date), count()
from gits
where owner_repo

group by search_key__owner, search_key__repo
union all
select 'github_commits' as type, search_key__owner, search_key__repo, max(commit__author__date), count()
from github_commits
where owner_repo

group by search_key__owner, search_key__repo




-- 指定owner 查看库里有什么repo
select concat('https://github.com/', search_key__owner, '/', search_key__repo) url,
       search_key__owner,
       search_key__repo
from gits
where search_key__owner = 'llvm'
group by search_key__owner, search_key__repo



-- 指定项目查看 哪些邮箱后缀占比高
select email_domain, count() as commit_count
from gits
where search_key__owner = 'llvm'
  and search_key__repo = 'llvm-project'
  and email_domain != 'gmail.com'
  and email_domain != 'outlook.com'
  and email_domain != 'googlemail.com'
  and email_domain != 'users.noreply.github.com'
group by splitByChar('@', author_email)[2] as email_domain
order by commit_count desc















-- 指定项目、公司 查看不同公司在不同目录中的贡献占比
select *
from (-- nvidia
         select search_key__owner,
                search_key__repo,
                dir,
                'nvidia'                                                  as company,
                length(splitByChar('/', dir)) - 1                         as dir_level,
                a.commit_count                                            as company_commit_count,
                b.commit_count                                            as total_commit_count,
                round(company_commit_count / total_commit_count * 100, 2) as percentage
         from (select search_key__owner, search_key__repo, dir, count() as commit_count
               from (select search_key__owner, search_key__repo, dir, hexsha
                     from dir_label_new_test_v2
                     where search_key__owner = 'llvm'
                       and search_key__repo = 'llvm-project'
                       and (dir not like '%test/%' and dir not like '%docs/%' and dir not like '.%')
                       and author__logins global in (select author__login
                                                     from (
                                                              -- github_commit 通过邮箱找nvidia
                                                              select author__login
                                                              from (select author__login
                                                                    from github_commits
                                                                    where author__login != ''
                                                                      and author__login not like '%[bot]%'
                                                                      and lower(commit__author__email) like '%nvidia%'
                                                                    group by author__login

                                                                    union all

                                                                    select committer__login
                                                                    from github_commits
                                                                    where committer__login != ''
                                                                      and committer__login not like '%[bot]%'
                                                                      and lower(commit__committer__email) like '%nvidia%'
                                                                    group by committer__login)
                                                              group by author__login

                                                              union all
                                                              -- 在github profile 里找
                                                              select github_login
                                                              from (select a.*, b.final_company_inferred_from_company
                                                                    from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                             left join (select company, final_company_inferred_from_company
                                                                                        from github_profile
                                                                                        where company != ''
                                                                                          and final_company_inferred_from_company != ''
                                                                                        group by company, final_company_inferred_from_company) as b
                                                                                       on a.company = b.company
                                                                    where (final_company_inferred_from_company =
                                                                           'nvidia'
                                                                        or lower(company) like '%nvidia%'
                                                                        or lower(company) like
                                                                           '%rapidsai%')
                                                                      and github_login != '')

                                                              group by github_login

                                                              union all
                                                              -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                              select github_login
                                                              from (select commits.author_email            as email,
                                                                           commits.author_github_login     as github_login,
                                                                           `commits.author_github_company` as company
                                                                    from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                    where (lower(email) like '%nvidia%'
                                                                        or lower(company) like '%nvidia%'
                                                                        or lower(company) like
                                                                           '%rapidsai%')
                                                                      and github_login != '')
                                                              group by github_login

                                                              union all
                                                              -- 也算从profile出发
                                                              select github_login
                                                              from nvidia_contributor_profile_v3
                                                              where company like '%nvidia%'
                                                                 or company like '%rapidsai'
                                                              group by github_login)
                                                     where author__login != ''
                                                     group by author__login)
                       and (
                                 lower(author_email) not like '%pytorch%'
                             and
                                 lower(author_email) not like '%intel%'
                             and lower(author_email) not like '%google%'
                             and lower(author_email) not like '%tensorflow%'
                             and lower(author_email) not like '%meta%'
                             and lower(author_email) not like '%facebook%'
                             and lower(author_email) not like '%amd.com%'
                             and lower(author_email) not like '%apple.com%'
                             and lower(author_email) not like '%fb.com%'
                             and lower(author_email) not like '%microsoft%'
                             and lower(author_email) not like '%anaconda%')
--               and dir like '%cuda%'
                     group by search_key__owner, search_key__repo, dir, hexsha)
               group by search_key__owner, search_key__repo, dir
               order by commit_count desc) as a global
                  join (select search_key__owner, search_key__repo, dir, count() as commit_count
                        from (select search_key__owner, search_key__repo, dir, hexsha
                              from dir_label_new_test_v2
                              where search_key__owner = 'llvm'
                                and search_key__repo = 'llvm-project'
--                        and dir like '%cuda%'
                                and (dir not like '%test/%' and dir not like '%docs/%' and dir not like '.%')

                              group by search_key__owner, search_key__repo, dir, hexsha)
                        group by search_key__owner, search_key__repo, dir
                        order by commit_count desc) as b
                       on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and
                          a.dir = b.dir


         union all
-- meta
         select search_key__owner,
                search_key__repo,
                dir,
                'meta'                                                 as company,
                length(splitByChar('/', dir)) - 1                      as dir_level,
                a.commit_count                                         as meta_commit_count,
                b.commit_count                                         as total_commit_count,
                round(meta_commit_count / total_commit_count * 100, 2) as percentage
         from (select search_key__owner, search_key__repo, dir, count() as commit_count
               from (select search_key__owner, search_key__repo, dir, hexsha
                     from dir_label_new_test_v2
                     where search_key__owner = 'llvm'
                       and search_key__repo = 'llvm-project'
                       and (dir not like '%test/%' and dir not like '%docs/%' and dir not like '.%')

                       and author__logins global in (select author__login
                                                     from (
                                                              -- github_commit 通过邮箱找nvidia
                                                              select author__login
                                                              from (select author__login
                                                                    from github_commits
                                                                    where author__login != ''
                                                                      and author__login not like '%[bot]%'
                                                                      and (lower(commit__author__email) like
                                                                           '%meta.com%' or
                                                                           lower(commit__author__email) like '%fb.com%')
                                                                    group by author__login
                                                                    union all
                                                                    select committer__login
                                                                    from github_commits
                                                                    where committer__login != ''
                                                                      and committer__login not like '%[bot]%'
                                                                      and (lower(commit__committer__email) like
                                                                           '%meta.com%' or
                                                                           lower(commit__committer__email) like
                                                                           '%fb.com%')
                                                                    group by committer__login)
                                                              group by author__login
                                                              union all
                                                              -- 在github profile 里找
                                                              select github_login
                                                              from (select a.*, b.final_company_inferred_from_company
                                                                    from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                             left join (select company, final_company_inferred_from_company
                                                                                        from github_profile
                                                                                        where company != ''
                                                                                          and final_company_inferred_from_company != ''
                                                                                        group by company, final_company_inferred_from_company) as b
                                                                                       on a.company = b.company
                                                                    where (final_company_inferred_from_company =
                                                                           'meta'
                                                                        or lower(company) like '%meta%'
                                                                        or lower(company) like
                                                                           '%facebook%')
                                                                      and github_login != '')

                                                              group by github_login

                                                              union all
                                                              -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                              select github_login
                                                              from (select commits.author_email            as email,
                                                                           commits.author_github_login     as github_login,
                                                                           `commits.author_github_company` as company
                                                                    from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                    where (lower(email) like '%fb.com%'
                                                                        or lower(company) like '%meta.com%')
                                                                      and github_login != '')
                                                              group by github_login

                                                              union all
                                                              -- 也算从profile出发
                                                              select github_login
                                                              from nvidia_contributor_profile_v3
                                                              where lower(company) like '%meta%'
                                                                 or lower(company) like '%facebook'
                                                              group by github_login)
                                                     where author__login != ''
                                                     group by author__login)
                       and (
                                 lower(author_email) not like '%pytorch%'
                             and
                                 lower(author_email) not like '%intel%'
                             and lower(author_email) not like '%google%'
                             and lower(author_email) not like '%tensorflow%'
                             and lower(author_email) not like '%nvidia%'
                             and lower(author_email) not like '%amd.com%'
                             and lower(author_email) not like '%apple.com%'

                             --                                             and lower(author_email) not like '%facebook%'
--                                             and lower(author_email) not like '%fb.com%'
                             and lower(author_email) not like '%microsoft%'
                             and lower(author_email) not like '%anaconda%')
--               and dir like '%cuda%'
                     group by search_key__owner, search_key__repo, dir, hexsha)
               group by search_key__owner, search_key__repo, dir
               order by commit_count desc) as a global
                  join (select search_key__owner, search_key__repo, dir, count() as commit_count
                        from (select search_key__owner, search_key__repo, dir, hexsha
                              from dir_label_new_test_v2
                              where search_key__owner = 'llvm'
                                and search_key__repo = 'llvm-project'
--                        and dir like '%cuda%'
                                and (dir not like '%test/%' and dir not like '%docs/%' and dir not like '.%')
                              group by search_key__owner, search_key__repo, dir, hexsha)
                        group by search_key__owner, search_key__repo, dir
                        order by commit_count desc) as b
                       on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and
                          a.dir = b.dir


         union all


-- amd
         select search_key__owner,
                search_key__repo,
                dir,
                'amd'                                                  as company,
                length(splitByChar('/', dir)) - 1                      as dir_level,
                a.commit_count                                         as meta_commit_count,
                b.commit_count                                         as total_commit_count,
                round(meta_commit_count / total_commit_count * 100, 2) as percentage
         from (select search_key__owner, search_key__repo, dir, count() as commit_count
               from (select search_key__owner, search_key__repo, dir, hexsha
                     from dir_label_new_test_v2
                     where search_key__owner = 'llvm'
                       and search_key__repo = 'llvm-project'
                       and author__logins global in (select author__login
                                                     from (
                                                              -- github_commit 通过邮箱找nvidia
                                                              select author__login
                                                              from (select author__login
                                                                    from github_commits
                                                                    where author__login != ''
                                                                      and author__login not like '%[bot]%'
                                                                      and lower(commit__author__email) like '%amd.com%'
                                                                    group by author__login

                                                                    union all

                                                                    select committer__login
                                                                    from github_commits
                                                                    where committer__login != ''
                                                                      and committer__login not like '%[bot]%'
                                                                      and lower(commit__committer__email) like '%amd.com%'
                                                                    group by committer__login)
                                                              group by author__login

                                                              union all
                                                              -- 在github profile 里找
                                                              select github_login
                                                              from (select a.*, b.final_company_inferred_from_company
                                                                    from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                             left join (select company, final_company_inferred_from_company
                                                                                        from github_profile
                                                                                        where company != ''
                                                                                          and final_company_inferred_from_company != ''
                                                                                        group by company, final_company_inferred_from_company) as b
                                                                                       on a.company = b.company
                                                                    where (final_company_inferred_from_company =
                                                                           'amd'
                                                                        or lower(company) like '%amd%')
                                                                      and github_login != '')

                                                              group by github_login

                                                              union all
                                                              -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                              select github_login
                                                              from (select commits.author_email            as email,
                                                                           commits.author_github_login     as github_login,
                                                                           `commits.author_github_company` as company
                                                                    from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                    where (lower(email) like '%amd.com%'
                                                                        or lower(company) like '%amd%')
                                                                      and github_login != '')
                                                              group by github_login

                                                              union all
                                                              -- 也算从profile出发
                                                              select github_login
                                                              from nvidia_contributor_profile_v3
                                                              where company like '%amd%'
                                                              group by github_login)
                                                     where author__login != ''
                                                     group by author__login)
                       and (
                                 lower(author_email) not like '%pytorch%'
                             and lower(author_email) not like '%apple.com%'
                             and
                                 lower(author_email) not like '%intel%'
                             and lower(author_email) not like '%google%'
                             and lower(author_email) not like '%tensorflow%'
                             and lower(author_email) not like '%nvidia%'
                             and lower(author_email) not like '%facebook%'
                             and lower(author_email) not like '%fb.com%'
                             and lower(author_email) not like '%meta.com%'
                             and lower(author_email) not like '%microsoft%'
                             and lower(author_email) not like '%anaconda%')
--               and dir like '%cuda%'
                       and (dir not like '%test/%' and dir not like '%docs/%' and dir not like '.%')
                     group by search_key__owner, search_key__repo, dir, hexsha)
               group by search_key__owner, search_key__repo, dir
               order by commit_count desc) as a global
                  join (select search_key__owner, search_key__repo, dir, count() as commit_count
                        from (select search_key__owner, search_key__repo, dir, hexsha
                              from dir_label_new_test_v2
                              where search_key__owner = 'llvm'
                                and search_key__repo = 'llvm-project'
                                and (dir not like '%test/%' and dir not like '%docs/%' and dir not like '.%')

--                        and dir like '%cuda%'
                              group by search_key__owner, search_key__repo, dir, hexsha)
                        group by search_key__owner, search_key__repo, dir
                        order by commit_count desc) as b
                       on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and
                          a.dir = b.dir

         union all

-- apple
         select search_key__owner,
                search_key__repo,
                dir,
                'quansight'                                            as company,
                length(splitByChar('/', dir)) - 1                      as dir_level,
                a.commit_count                                         as meta_commit_count,
                b.commit_count                                         as total_commit_count,
                round(meta_commit_count / total_commit_count * 100, 2) as percentage
         from (select search_key__owner, search_key__repo, dir, count() as commit_count
               from (select search_key__owner, search_key__repo, dir, hexsha
                     from dir_label_new_test_v2
                     where search_key__owner = 'llvm'
                       and search_key__repo = 'llvm-project'
                       and author__logins global in (select author__login
                                                     from (
                                                              -- github_commit 通过邮箱找nvidia
                                                              select author__login
                                                              from (select author__login
                                                                    from github_commits
                                                                    where author__login != ''
                                                                      and author__login not like '%[bot]%'
                                                                      and lower(commit__author__email) like '%apple%'
                                                                    group by author__login

                                                                    union all

                                                                    select committer__login
                                                                    from github_commits
                                                                    where committer__login != ''
                                                                      and committer__login not like '%[bot]%'
                                                                      and lower(commit__committer__email) like '%apple%'
                                                                    group by committer__login)
                                                              group by author__login

                                                              union all
                                                              -- 在github profile 里找
                                                              select github_login
                                                              from (select a.*, b.final_company_inferred_from_company
                                                                    from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                             left join (select company, final_company_inferred_from_company
                                                                                        from github_profile
                                                                                        where company != ''
                                                                                          and final_company_inferred_from_company != ''
                                                                                        group by company, final_company_inferred_from_company) as b
                                                                                       on a.company = b.company
                                                                    where (final_company_inferred_from_company =
                                                                           'apple'
                                                                        or lower(company) like '%apple%')
                                                                      and github_login != '')

                                                              group by github_login

                                                              union all
                                                              -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                              select github_login
                                                              from (select commits.author_email            as email,
                                                                           commits.author_github_login     as github_login,
                                                                           `commits.author_github_company` as company
                                                                    from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                    where (lower(email) like '%apple%'
                                                                        or lower(company) like '%apple%')
                                                                      and github_login != '')
                                                              group by github_login

                                                              union all
                                                              -- 也算从profile出发
                                                              select github_login
                                                              from nvidia_contributor_profile_v3
                                                              where company like '%apple%'
                                                              group by github_login)
                                                     where author__login != ''
                                                     group by author__login)
                       and (
                                 lower(author_email) not like '%pytorch%'
                             and
                                 lower(author_email) not like '%intel%'
                             and lower(author_email) not like '%google%'
                             and lower(author_email) not like '%tensorflow%'
                             and lower(author_email) not like '%nvidia%'
                             and lower(author_email) not like '%facebook%'
                             and lower(author_email) not like '%meta%'
                             and lower(author_email) not like '%fb.com%'
                             and lower(author_email) not like '%amd.com%'
                             and lower(author_email) not like '%microsoft%'
                             and lower(author_email) not like '%anaconda%')
--               and dir like '%cuda%'
                       and (dir not like '%test/%' and dir not like '%docs/%' and dir not like '.%')

                     group by search_key__owner, search_key__repo, dir, hexsha)
               group by search_key__owner, search_key__repo, dir
               order by commit_count desc) as a global
                  join (select search_key__owner, search_key__repo, dir, count() as commit_count
                        from (select search_key__owner, search_key__repo, dir, hexsha
                              from dir_label_new_test_v2
                              where search_key__owner = 'llvm'
                                and search_key__repo = 'llvm-project'
--                        and dir like '%cuda%'
                                and (dir not like '%test/%' and dir not like '%docs/%' and dir not like '.%')

                              group by search_key__owner, search_key__repo, dir, hexsha)
                        group by search_key__owner, search_key__repo, dir
                        order by commit_count desc) as b
                       on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and
                          a.dir = b.dir


         union all
--microsoft
         select search_key__owner,
                search_key__repo,
                dir,
                'microsoft'                                            as company,
                length(splitByChar('/', dir)) - 1                      as dir_level,
                a.commit_count                                         as meta_commit_count,
                b.commit_count                                         as total_commit_count,
                round(meta_commit_count / total_commit_count * 100, 2) as percentage
         from (select search_key__owner, search_key__repo, dir, count() as commit_count
               from (select search_key__owner, search_key__repo, dir, hexsha
                     from dir_label_new_test_v2
                     where search_key__owner = 'llvm'
                       and search_key__repo = 'llvm-project'
                       and author__logins global in (select author__login
                                                     from (
                                                              -- github_commit 通过邮箱找nvidia
                                                              select author__login
                                                              from (select author__login
                                                                    from github_commits
                                                                    where author__login != ''
                                                                      and author__login not like '%[bot]%'
                                                                      and lower(commit__author__email) like '%microsoft%'
                                                                    group by author__login

                                                                    union all

                                                                    select committer__login
                                                                    from github_commits
                                                                    where committer__login != ''
                                                                      and committer__login not like '%[bot]%'
                                                                      and lower(commit__committer__email) like '%microsoft%'
                                                                    group by committer__login)
                                                              group by author__login

                                                              union all
                                                              -- 在github profile 里找
                                                              select github_login
                                                              from (select a.*, b.final_company_inferred_from_company
                                                                    from (select github_login, company from nvidia_contributor_profile_v3) as a global
                                                                             left join (select company, final_company_inferred_from_company
                                                                                        from github_profile
                                                                                        where company != ''
                                                                                          and final_company_inferred_from_company != ''
                                                                                        group by company, final_company_inferred_from_company) as b
                                                                                       on a.company = b.company
                                                                    where (final_company_inferred_from_company =
                                                                           'microsoft'
                                                                        or lower(company) like '%microsoft%')
                                                                      and github_login != '')

                                                              group by github_login

                                                              union all
                                                              -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为nvidia
                                                              select github_login
                                                              from (select commits.author_email            as email,
                                                                           commits.author_github_login     as github_login,
                                                                           `commits.author_github_company` as company
                                                                    from nvidia_contributor_pr_v3 array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                                                                    where (lower(email) like '%microsoft%'
                                                                        or lower(company) like '%microsoft%')
                                                                      and github_login != '')
                                                              group by github_login

                                                              union all
                                                              -- 也算从profile出发
                                                              select github_login
                                                              from nvidia_contributor_profile_v3
                                                              where company like '%microsoft%'
                                                              group by github_login)
                                                     where author__login != ''
                                                     group by author__login)
                       and (
                                 lower(author_email) not like '%pytorch%'
                             and lower(author_email) not like '%apple.com%'
                             and
                                 lower(author_email) not like '%intel%'
                             and lower(author_email) not like '%google%'
                             and lower(author_email) not like '%tensorflow%'
                             and lower(author_email) not like '%nvidia%'
                             and lower(author_email) not like '%facebook%'
                             and lower(author_email) not like '%fb.com%'
                             and lower(author_email) not like '%amd.com%'
                             and lower(author_email) not like '%meta.com%'
                             and lower(author_email) not like '%anaconda%')
--               and dir like '%cuda%'
                       and (dir not like '%test/%' and dir not like '%docs/%' and dir not like '.%')

                     group by search_key__owner, search_key__repo, dir, hexsha)
               group by search_key__owner, search_key__repo, dir
               order by commit_count desc) as a global
                  join (select search_key__owner, search_key__repo, dir, count() as commit_count
                        from (select search_key__owner, search_key__repo, dir, hexsha
                              from dir_label_new_test_v2
                              where search_key__owner = 'llvm'
                                and search_key__repo = 'llvm-project'
--                        and dir like '%cuda%'
                                and (dir not like '%test/%' and dir not like '%docs/%' and dir not like '.%')

                              group by search_key__owner, search_key__repo, dir, hexsha)
                        group by search_key__owner, search_key__repo, dir
                        order by commit_count desc) as b
                       on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and
                          a.dir = b.dir)



;

-- 指定项目 每个项目每月总投入人数和总贡献量
select search_key__owner,
       search_key__repo,
       toYYYYMM(authored_date) as month,
       count(distinct author__login) as author_count,
       count()                 as commit_count
from (select *
      from gits
      where ((search_key__owner = 'llvm' and search_key__repo = 'llvm-project') or
             (search_key__owner = 'kubernetes' and search_key__repo = 'kubernetes') or
             (search_key__owner = 'torvalds' and search_key__repo = 'linux') or
             (search_key__owner = 'rust-lang' and search_key__repo = 'rust') or
             (search_key__owner = 'ray-project' and search_key__repo = 'ray') or
             (search_key__owner = 'apache' and search_key__repo = 'arrow') or
             (search_key__owner = 'huggingface' and search_key__repo = 'transformers') or
             (search_key__owner = 'pytorch' and search_key__repo = 'pytorch') or
             (search_key__owner = 'Lightning-AI' and search_key__repo = 'pytorch-lightning') or
             (search_key__owner = 'Lightning-AI' and search_key__repo = 'litgpt') or
             (search_key__owner = 'Lightning-AI' and search_key__repo = 'torchmetrics') or
             (search_key__owner = 'Lightning-AI' and search_key__repo = 'lightning-thunder'))
        and length(parents) = 1) as a global
         join (
    select author__login, commit__author__email
    from github_commits
    where author__id != 0
    group by author__login, commit__author__email
    ) as b on a.author_email = b.commit__author__email
group by search_key__owner, search_key__repo, month
;

;
