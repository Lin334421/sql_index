insert into table robot_login_email
select robots_login as robot_login_email, toUnixTimestamp(now()) as insert_at
from (select robots_login
      from (select robots_login
            from (with ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]'] as robots_login
                  select robots_login)
                     array join robots_login
            group by robots_login
            union all
            select author__login
            from github_commits
            where
--     search_key__owner = 'rust-lang'

(author__login like '%[bot]'
    or author__login like '%-robot%'
    or author__login like '%dependabot-%'
    or author__login like '%-bot'
--            or author__login  like '%bot%'
    )
            group by author__login
            union all
            select committer__login
            from github_commits
            where
--     search_key__owner = 'rust-lang'

(committer__login like '%[bot]'
    or committer__login like '%-robot%'
    or committer__login like '%dependabot-%'
    or committer__login like '%-bot'
--            or author__login  like '%bot%'
    )
            group by committer__login)
      where robots_login != ''
      group by robots_login

      union all
      select commit__committer__email
      from (select commit__committer__email
            from github_commits
            where committer__login global in (select robots_login
                                              from (select robots_login
                                                    from (with ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                                                        'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]'] as robots_login
                                                          select robots_login)
                                                             array join robots_login
                                                    group by robots_login
                                                    union all
                                                    select author__login
                                                    from github_commits
                                                    where
--     search_key__owner = 'rust-lang'
--   and search_key__repo = 'rust'
--   and
(author__login like '%[bot]'
    or author__login like '%-robot%'
    or author__login like '%dependabot-%'
    or author__login like '%-bot'
--            or author__login  like '%bot%'
    )
                                                    group by author__login
                                                    union all
                                                    select committer__login
                                                    from github_commits
                                                    where
--     search_key__owner = 'rust-lang'
--   and search_key__repo = 'rust'
--   and
(committer__login like '%[bot]'
    or committer__login like '%-robot%'
    or committer__login like '%dependabot-%'
    or committer__login like '%-bot'
--            or author__login  like '%bot%'
    )
                                                    group by committer__login)
                                              group by robots_login)
              and commit__committer__email != ''
            group by commit__committer__email

            union all
            select commit__author__email
            from github_commits
            where author__login global in (select robots_login
                                           from (select robots_login
                                                 from (with ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                                                     'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]'] as robots_login
                                                       select robots_login)
                                                          array join robots_login
                                                 group by robots_login
                                                 union all
                                                 select author__login
                                                 from github_commits
                                                 where
--     search_key__owner = 'rust-lang'
--   and search_key__repo = 'rust'
--   and
(author__login like '%[bot]'
    or author__login like '%-robot%'
    or author__login like '%dependabot-%'
    or author__login like '%-bot'
--            or author__login  like '%bot%'
    )
                                                 group by author__login
                                                 union all
                                                 select committer__login
                                                 from github_commits
                                                 where
--     search_key__owner = 'rust-lang'
--   and search_key__repo = 'rust'
--   and
(committer__login like '%[bot]'
    or committer__login like '%-robot%'
    or committer__login like '%dependabot-%'
    or committer__login like '%-bot'
--   or author__login  like '%bot%'
    )
                                                 group by committer__login)
                                           group by robots_login)
              and commit__author__email != ''
            group by commit__author__email)
      group by commit__committer__email)



-- 机器人login和email 表用以去除
create table default.robot_login_email_local on cluster replicated
(
    robot_login_email String,
    insert_at         Int64

)
    engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/robot_login_email', '{replica}', insert_at)
        ORDER BY (robot_login_email)
        SETTINGS index_granularity = 8192;

;


create table default.robot_login_email on cluster replicated as robot_login_email_local
    engine = Distributed
(
    'replicated',
    'default',
    'robot_login_email_local',
    halfMD5
(
    robot_login_email
));