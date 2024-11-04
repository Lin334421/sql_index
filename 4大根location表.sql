
-- k8s
-- 插入code_owners_location
insert into table code_owners_location
select search_key__owner                                       as owner,
       search_key__repo                                        as repo,
       'github_login'                                          as data_type,
       toUnixTimestamp(now())                                  as insert_at,
       github_login                                            as login_or_email,
       member_type,
       month,
       if(github_login = 'davidtwco', '欧洲', b.inferred_area) as inferred_area
from (select search_key__owner,
             search_key__repo,
             toYYYYMM(authored_date)               as month,
             github_login,
             JSONExtractString(misc, 'owner_type') as member_type
      from code_owners
      where search_key__repo = 'kubernetes'
      group by search_key__owner, search_key__repo, month, github_login, member_type) as a global
         join (select github_login,
                      multiIf(inferred_area global in
                              ['North West','UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                          , '欧洲', inferred_area global in
                                    ['Canada', 'Mexico', 'United States','USA'],
                              '北美',
                              inferred_area global in ['China', 'Hong Kong','Taiwan'],
                              '中国',
                              inferred_area global in ['India'],
                              '印度',
                              inferred_area global in ['Japan', 'South Korea'], '日韩',
                              inferred_area global in ['New Zealand', 'Australia'], '澳洲',
                              inferred_area) as inferred_area
               from (select if(b.login != '', b.login, a.author__login)   as github_login,
                            if(b.region != '', b.region, a.inferred_area) as inferred_area

                     from (select author__login, inferred_area, main_tz_area, location
                           from (select author__id, author__login
                                 from (select author__id, author__login
                                       from github_commits
                                       where author__login != ''
                                       group by author__id, author__login
                                       union all
                                       select committer__id, committer__login
                                       from github_commits
                                       where committer__login != ''
                                       group by committer__id, committer__login)
                                 where author__login global in (select github_login
                                                                from code_owners
                                                                where search_key__repo = 'kubernetes'
                                                                group by github_login)
                                 group by author__id, author__login) as a global
                                    join github_id_main_tz_map as b on a.author__id = b.github_id) as a global
                              full join (select login,
                                                inferred_from_location__country,
                                                multiIf(inferred_from_location__country global in
                                                        ['North West','UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                                    , '欧洲',
                                                        inferred_from_location__country global in
                                                        ['New Zealand', 'Australia'], '澳洲',
                                                        inferred_from_location__country global in
                                                        ['Canada', 'Mexico', 'United States','USA'],
                                                        '北美',
                                                        inferred_from_location__country global in
                                                        ['Japan', 'South Korea'], '日韩',
                                                        inferred_from_location__country global in
                                                        ['China', 'Hong Kong','Taiwan'],
                                                        '中国',
                                                        inferred_from_location__country global in ['India'],
                                                        '印度',
                                                        '其他') as region
                                         from github_profile
                                         where login global in (select github_login
                                                                from code_owners
                                                                where search_key__repo = 'kubernetes'
                                                                group by github_login)
                                           and inferred_from_location__country != '') as b
                                        on a.author__login = b.login)
               group by github_login, inferred_area
    ) as b on a.github_login = b.github_login
where login_or_email global not in
      ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
          'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
and (login_or_email not like '%-bot%' and login_or_email not like '%[bot]%')
and login_or_email not like '%sig-%' and login_or_email not like '%release-%'
and login_or_email not like '%feature-%'
and login_or_email not like '%api-%'
and login_or_email not like '%dep-%'

and login_or_email not like '%build-%'
and login_or_email not like '%-approvers'
and login_or_email not like '%-maintainers'
and login_or_email not like '%-reviewers'

union all

-- 插入没有推断出来的人为其他

select search_key__owner                                       as owner,
       search_key__repo                                        as repo,
       'github_login'                                          as data_type,
       toUnixTimestamp(now())                                  as insert_at,
       github_login                                            as login_or_email,
       member_type,
       month,
       '其他' as inferred_area
from (select search_key__owner,
             search_key__repo,
             toYYYYMM(authored_date)               as month,
             github_login,
             JSONExtractString(misc, 'owner_type') as member_type
      from code_owners
      where search_key__repo = 'kubernetes'
      group by search_key__owner, search_key__repo, month, github_login, member_type)
where login_or_email != ''
and github_login global not in (select login_or_email from (select search_key__owner                                       as owner,
       search_key__repo                                        as repo,
       'github_login'                                          as data_type,
       toUnixTimestamp(now())                                  as insert_at,
       github_login                                            as login_or_email,
       member_type,
       month,
       if(github_login = 'davidtwco', '欧洲', b.inferred_area) as inferred_area
from (select search_key__owner,
             search_key__repo,
             toYYYYMM(authored_date)               as month,
             github_login,
             JSONExtractString(misc, 'owner_type') as member_type
      from code_owners
      where search_key__repo = 'kubernetes'
      group by search_key__owner, search_key__repo, month, github_login, member_type) as a global
         join (select github_login,
                      multiIf(inferred_area global in
                              ['North West','UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                          , '欧洲', inferred_area global in
                                    ['Canada', 'Mexico', 'United States','USA'],
                              '北美',
                              inferred_area global in ['China', 'Hong Kong','Taiwan'],
                              '中国',
                              inferred_area global in ['India'],
                              '印度',
                              inferred_area global in ['Japan', 'South Korea'], '日韩',
                              inferred_area global in ['New Zealand', 'Australia'], '澳洲',
                              inferred_area) as inferred_area
               from (select if(b.login != '', b.login, a.author__login)   as github_login,
                            if(b.region != '', b.region, a.inferred_area) as inferred_area

                     from (select author__login, inferred_area, main_tz_area, location
                           from (select author__id, author__login
                                 from (select author__id, author__login
                                       from github_commits
                                       where author__login != ''
                                       group by author__id, author__login
                                       union all
                                       select committer__id, committer__login
                                       from github_commits
                                       where committer__login != ''
                                       group by committer__id, committer__login)
                                 where author__login global in (select github_login
                                                                from code_owners
                                                                where search_key__repo = 'kubernetes'
                                                                group by github_login)
                                 group by author__id, author__login) as a global
                                    join github_id_main_tz_map as b on a.author__id = b.github_id) as a global
                              full join (select login,
                                                inferred_from_location__country,
                                                multiIf(inferred_from_location__country global in
                                                        ['North West','UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                                    , '欧洲',
                                                        inferred_from_location__country global in
                                                        ['New Zealand', 'Australia'], '澳洲',
                                                        inferred_from_location__country global in
                                                        ['Canada', 'Mexico', 'United States','USA'],
                                                        '北美',
                                                        inferred_from_location__country global in
                                                        ['Japan', 'South Korea'], '日韩',
                                                        inferred_from_location__country global in
                                                        ['China', 'Hong Kong','Taiwan'],
                                                        '中国',
                                                        inferred_from_location__country global in ['India'],
                                                        '印度',
                                                        '其他') as region
                                         from github_profile
                                         where login global in (select github_login
                                                                from code_owners
                                                                where search_key__repo = 'kubernetes'
                                                                group by github_login)
                                           and inferred_from_location__country != '') as b
                                        on a.author__login = b.login)
               group by github_login, inferred_area
    ) as b on a.github_login = b.github_login
where login_or_email global not in
      ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
          'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
and (login_or_email not like '%-bot%' and login_or_email not like '%[bot]%')) group by login_or_email)

and login_or_email global not in
      ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
          'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
and (login_or_email not like '%-bot%' and login_or_email not like '%[bot]%')
and login_or_email not like '%sig-%' and login_or_email not like '%release-%'
and login_or_email not like '%feature-%'
and login_or_email not like '%api-%'
and login_or_email not like '%dep-%'

and login_or_email not like '%build-%'
and login_or_email not like '%-approvers'
and login_or_email not like '%-maintainers'
and login_or_email not like '%-reviewers'







-- 插入 rust member数据 到code_owners_location
insert into table code_owners_location
select *
from (select 'rust-lang'                                             as owner,
             'rust'                                                  as repo,
             'github_login'                                          as data_type,
             toUnixTimestamp(now())                                  as insert_at,
             people__members                                         as login_or_email,
             'member'                                                as member_type,
             month,
             if(github_login = 'davidtwco', '欧洲', b.inferred_area) as inferred_area
      from (select people__members, month
            from (select name, `subteam-of` as root_team, month, people__members
                  from rust_teams_history
                           array join people__members)
            where people__members != ''
            group by people__members, month) as a global
               join (select github_login,
                            multiIf(inferred_area global in
                                    ['North West','UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                , '欧洲', inferred_area global in
                                          ['Canada', 'Mexico', 'United States','USA'],
                                    '北美',
                                    inferred_area global in ['China', 'Hong Kong','Taiwan'],
                                    '中国',
                                    inferred_area global in ['India'],
                                    '印度',
                                    inferred_area global in ['Japan', 'South Korea'], '日韩',
                                    inferred_area global in ['New Zealand', 'Australia'], '澳洲',
                                    inferred_area) as inferred_area
                     from (select if(b.login != '', b.login, a.author__login)   as github_login,
                                  if(b.region != '', b.region, a.inferred_area) as inferred_area

                           from (select author__login, inferred_area, main_tz_area, location
                                 from (select author__id, author__login
                                       from (select author__id, author__login
                                             from github_commits
                                             where author__login != ''
                                             group by author__id, author__login
                                             union all
                                             select committer__id, committer__login
                                             from github_commits
                                             where committer__login != ''
                                             group by committer__id, committer__login)
                                       where author__login global in (select people__members
                                                                      from (select name, `subteam-of` as root_team, month, people__members
                                                                            from rust_teams_history
                                                                                     array join people__members)
                                                                      where people__members != ''
                                                                      group by people__members)
                                       group by author__id, author__login) as a global
                                          join github_id_main_tz_map as b on a.author__id = b.github_id) as a global
                                    full join (select login,
                                                      inferred_from_location__country,
                                                      multiIf(inferred_from_location__country global in
                                                              ['North West','UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                                          , '欧洲',
                                                              inferred_from_location__country global in
                                                              ['New Zealand', 'Australia'], '澳洲',
                                                              inferred_from_location__country global in
                                                              ['Canada', 'Mexico', 'United States','USA'],
                                                              '北美',
                                                              inferred_from_location__country global in
                                                              ['Japan', 'South Korea'], '日韩',
                                                              inferred_from_location__country global in
                                                              ['China', 'Hong Kong','Taiwan'],
                                                              '中国',
                                                              inferred_from_location__country global in ['India'],
                                                              '印度',
                                                              '其他') as region
                                               from github_profile
                                               where login global in (select people__members
                                                                      from (select name, `subteam-of` as root_team, month, people__members
                                                                            from rust_teams_history
                                                                                     array join people__members)
                                                                      where people__members != ''
                                                                      group by people__members)
                                                 and inferred_from_location__country != '') as b
                                              on a.author__login = b.login)
                     group by github_login, inferred_area
          ) as b on a.people__members = b.github_login)
where login_or_email global not in
      ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
          'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
and (login_or_email not like '%-bot%' and login_or_email not like '%[bot]%')
union all
-- 插入 rust member中没有推断出区域的成员 为其他
-- insert into table code_owners_location

select 'rust-lang'                                             as owner,
       'rust'                                                  as repo,
       'github_login'                                          as data_type,
       toUnixTimestamp(now())                                  as insert_at,
       github_login                                            as login_or_email,
       'leader'                                                as member_type,
       month,
       '其他' as inferred_area from (select people__members as github_login, month
            from (select month, people__members
                  from rust_teams_history
                           array join people__members)
            where people__members != ''
            group by people__members, month)
where github_login global not in (select login_or_email from (select *
from (select 'rust-lang'                                             as owner,
             'rust'                                                  as repo,
             'github_login'                                          as data_type,
             toUnixTimestamp(now())                                  as insert_at,
             people__members                                         as login_or_email,
             'member'                                                as member_type,
             month,
             if(github_login = 'davidtwco', '欧洲', b.inferred_area) as inferred_area
      from (select people__members, month
            from (select name, `subteam-of` as root_team, month, people__members
                  from rust_teams_history
                           array join people__members)
            where people__members != ''
            group by people__members, month) as a global
               join (select github_login,
                            multiIf(inferred_area global in
                                    ['North West','UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                , '欧洲', inferred_area global in
                                          ['Canada', 'Mexico', 'United States','USA'],
                                    '北美',
                                    inferred_area global in ['China', 'Hong Kong','Taiwan'],
                                    '中国',
                                    inferred_area global in ['India'],
                                    '印度',
                                    inferred_area global in ['Japan', 'South Korea'], '日韩',
                                    inferred_area global in ['New Zealand', 'Australia'], '澳洲',
                                    inferred_area) as inferred_area
                     from (select if(b.login != '', b.login, a.author__login)   as github_login,
                                  if(b.region != '', b.region, a.inferred_area) as inferred_area

                           from (select author__login, inferred_area, main_tz_area, location
                                 from (select author__id, author__login
                                       from (select author__id, author__login
                                             from github_commits
                                             where author__login != ''
                                             group by author__id, author__login
                                             union all
                                             select committer__id, committer__login
                                             from github_commits
                                             where committer__login != ''
                                             group by committer__id, committer__login)
                                       where author__login global in (select people__members
                                                                      from (select name, `subteam-of` as root_team, month, people__members
                                                                            from rust_teams_history
                                                                                     array join people__members)
                                                                      where people__members != ''
                                                                      group by people__members)
                                       group by author__id, author__login) as a global
                                          join github_id_main_tz_map as b on a.author__id = b.github_id) as a global
                                    full join (select login,
                                                      inferred_from_location__country,
                                                      multiIf(inferred_from_location__country global in
                                                              ['North West','UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                                          , '欧洲',
                                                              inferred_from_location__country global in
                                                              ['New Zealand', 'Australia'], '澳洲',
                                                              inferred_from_location__country global in
                                                              ['Canada', 'Mexico', 'United States','USA'],
                                                              '北美',
                                                              inferred_from_location__country global in
                                                              ['Japan', 'South Korea'], '日韩',
                                                              inferred_from_location__country global in
                                                              ['China', 'Hong Kong','Taiwan'],
                                                              '中国',
                                                              inferred_from_location__country global in ['India'],
                                                              '印度',
                                                              '其他') as region
                                               from github_profile
                                               where login global in (select people__members
                                                                      from (select name, `subteam-of` as root_team, month, people__members
                                                                            from rust_teams_history
                                                                                     array join people__members)
                                                                      where people__members != ''
                                                                      group by people__members)
                                                 and inferred_from_location__country != '') as b
                                              on a.author__login = b.login)
                     group by github_login, inferred_area
          ) as b on a.people__members = b.github_login)
where login_or_email global not in
      ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
          'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']) group by login_or_email)
and login_or_email global not in
      ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot', 'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
      and (login_or_email not like '%-bot%' and login_or_email not like '%[bot]%')











-- 插入 rust lead 数据到 code_owners_location
insert into table code_owners_location

select *
from (select 'rust-lang'                                             as owner,
             'rust'                                                  as repo,
             'github_login'                                          as data_type,
             toUnixTimestamp(now())                                  as insert_at,
             people__leads                                           as login_or_email,
             'leader'                                                as member_type,
             month,
             if(github_login = 'davidtwco', '欧洲', b.inferred_area) as inferred_area
      from (select people__leads, month
            from (select month, people__leads
                  from rust_teams_history
                           array join people__leads)
            where people__leads != ''
            group by people__leads, month) as a global
               join (select github_login,
                            multiIf(inferred_area global in
                                    ['North West','UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                , '欧洲', inferred_area global in
                                          ['Canada', 'Mexico', 'United States','USA'],
                                    '北美',
                                    inferred_area global in ['China', 'Hong Kong','Taiwan'],
                                    '中国',
                                    inferred_area global in ['India'],
                                    '印度',
                                    inferred_area global in ['Japan', 'South Korea'], '日韩',
                                    inferred_area global in ['New Zealand', 'Australia'], '澳洲',
                                    inferred_area) as inferred_area
                     from (select if(b.login != '', b.login, a.author__login)   as github_login,
                                  if(b.region != '', b.region, a.inferred_area) as inferred_area

                           from (select author__login, inferred_area, main_tz_area, location
                                 from (select author__id, author__login
                                       from (select author__id, author__login
                                             from github_commits
                                             where author__login != ''
                                             group by author__id, author__login
                                             union all
                                             select committer__id, committer__login
                                             from github_commits
                                             where committer__login != ''
                                             group by committer__id, committer__login)
                                       where author__login global in (

                                           --logins
                                           select people__leads
                                           from (select month, people__leads
                                                 from rust_teams_history
                                                          array join people__leads)
                                           where people__leads != ''
                                           group by people__leads)
                                       group by author__id, author__login) as a global
                                          join github_id_main_tz_map as b on a.author__id = b.github_id) as a global
                                    full join (select login,
                                                      inferred_from_location__country,
                                                      multiIf(inferred_from_location__country global in
                                                              ['North West','UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                                          , '欧洲',
                                                              inferred_from_location__country global in
                                                              ['New Zealand', 'Australia'], '澳洲',
                                                              inferred_from_location__country global in
                                                              ['Canada', 'Mexico', 'United States','USA'],
                                                              '北美',
                                                              inferred_from_location__country global in
                                                              ['Japan', 'South Korea'], '日韩',
                                                              inferred_from_location__country global in
                                                              ['China', 'Hong Kong','Taiwan'],
                                                              '中国',
                                                              inferred_from_location__country global in ['India'],
                                                              '印度',
                                                              '其他') as region
                                               from github_profile
                                               where login global in (

                                                   --logins
                                                   select people__leads
                                                   from (select month, people__leads
                                                         from rust_teams_history
                                                                  array join people__leads)
                                                   where people__leads != ''
                                                   group by people__leads)
                                                 and inferred_from_location__country != '') as b
                                              on a.author__login = b.login)
                     group by github_login, inferred_area
          ) as b on a.people__leads = b.github_login)
where login_or_email global not in
      ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot', 'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
      and (login_or_email not like '%-bot%' and login_or_email not like '%[bot]%')

union all
-- 插入 rust leed中没有推断出区域的成员 为其他
-- insert into table code_owners_location
select 'rust-lang'                                             as owner,
       'rust'                                                  as repo,
       'github_login'                                          as data_type,
       toUnixTimestamp(now())                                  as insert_at,
       github_login                                            as login_or_email,
       'leader'                                                as member_type,
       month,
       '其他' as inferred_area
from (select month, people__leads as github_login
      from rust_teams_history
               array join people__leads where github_login != '' group by github_login, month
)
where github_login != '' and github_login global not in (select login_or_email from (select 'rust-lang'                                             as owner,
             'rust'                                                  as repo,
             'github_login'                                          as data_type,
             toUnixTimestamp(now())                                  as insert_at,
             people__leads                                           as login_or_email,
             'leader'                                                as member_type,
             month,
             if(github_login = 'davidtwco', '欧洲', b.inferred_area) as inferred_area
      from (select people__leads, month
            from (select month, people__leads
                  from rust_teams_history
                           array join people__leads)
            where people__leads != ''
            group by people__leads, month) as a global
               join (select github_login,
                            multiIf(inferred_area global in
                                    ['North West','UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                , '欧洲', inferred_area global in
                                          ['Canada', 'Mexico', 'United States','USA'],
                                    '北美',
                                    inferred_area global in ['China', 'Hong Kong','Taiwan'],
                                    '中国',
                                    inferred_area global in ['India'],
                                    '印度',
                                    inferred_area global in ['Japan', 'South Korea'], '日韩',
                                    inferred_area global in ['New Zealand', 'Australia'], '澳洲',
                                    inferred_area) as inferred_area
                     from (select if(b.login != '', b.login, a.author__login)   as github_login,
                                  if(b.region != '', b.region, a.inferred_area) as inferred_area

                           from (select author__login, inferred_area, main_tz_area, location
                                 from (select author__id, author__login
                                       from (select author__id, author__login
                                             from github_commits
                                             where author__login != ''
                                             group by author__id, author__login
                                             union all
                                             select committer__id, committer__login
                                             from github_commits
                                             where committer__login != ''
                                             group by committer__id, committer__login)
                                       where author__login global in (

                                           --logins
                                           select people__leads
                                           from (select month, people__leads
                                                 from rust_teams_history
                                                          array join people__leads)
                                           where people__leads != ''
                                           group by people__leads)
                                       group by author__id, author__login) as a global
                                          join github_id_main_tz_map as b on a.author__id = b.github_id) as a global
                                    full join (select login,
                                                      inferred_from_location__country,
                                                      multiIf(inferred_from_location__country global in
                                                              ['North West','UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                                          , '欧洲',
                                                              inferred_from_location__country global in
                                                              ['New Zealand', 'Australia'], '澳洲',
                                                              inferred_from_location__country global in
                                                              ['Canada', 'Mexico', 'United States','USA'],
                                                              '北美',
                                                              inferred_from_location__country global in
                                                              ['Japan', 'South Korea'], '日韩',
                                                              inferred_from_location__country global in
                                                              ['China', 'Hong Kong','Taiwan'],
                                                              '中国',
                                                              inferred_from_location__country global in ['India'],
                                                              '印度',
                                                              '其他') as region
                                               from github_profile
                                               where login global in (

                                                   --logins
                                                   select people__leads
                                                   from (select month, people__leads
                                                         from rust_teams_history
                                                                  array join people__leads)
                                                   where people__leads != ''
                                                   group by people__leads)
                                                 and inferred_from_location__country != '') as b
                                              on a.author__login = b.login)
                     group by github_login, inferred_area
          ) as b on a.people__leads = b.github_login) group by login_or_email)
and login_or_email global not in
      ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot', 'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
      and (login_or_email not like '%-bot%' and login_or_email not like '%[bot]%')














-- 插入code_owners_location
insert into table code_owners_location
select owner,
       repo,
       'email'                as data_type,
       toUnixTimestamp(now()) as insert_at,
       email                  as login_or_email,
       'member'               as member_type,
       year_month             as month,
       inferred_area

from (
         select owner, repo, owner_id as email, year_month
         from code_owner_history_by_year_month
         where owner = 'llvm'
           and repo = 'llvm-project'
         group by owner, repo, owner_id, year_month) as a global
         join (select if(a.email != '', a.email, b.email)                as email,
                      if(a.inferred_area != '', a.inferred_area, b.area) as inferred_area
               from (select if(b.email != '', b.email, a.email)           as email,
                            if(b.region != '', b.region, a.inferred_area) as inferred_area
                     from (select a.*,
                                  multiIf(inferred_area global in
                                          ['UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                      , '欧洲', inferred_area global in
                                                ['Canada', 'Mexico', 'United States','USA'],
                                          '北美',
                                          inferred_area global in ['China', 'Hong Kong','Taiwan'],
                                          '中国',
                                          inferred_area global in ['India'],
                                          '印度',
                                          inferred_area global in ['Japan', 'South Korea'], '日韩',
                                          inferred_area global in ['New Zealand', 'Australia'], '澳洲',
                                          inferred_area) as inferred_area
                           from (select a.*, b.github_id
                                 from (
                                          -- email
                                          select owner_id as email
                                          from code_owner_history_by_year_month
                                          where owner = 'llvm'
                                            and repo = 'llvm-project'
                                          group by owner_id) as a global
                                          join (select author__id as github_id, commit__author__email as email
                                                from (select author__id, commit__author__email
                                                      from github_commits
                                                      where author__id != 0
                                                      group by author__id, commit__author__email
                                                      union all
                                                      select committer__id, commit__committer__email
                                                      from github_commits
                                                      where committer__id != 0
                                                      group by committer__id, commit__committer__email)
                                                group by github_id, email) as b on a.email = b.email) as a global
                                    left join github_id_main_tz_map as b on a.github_id = b.github_id) as a global
                              full join (select email, region
                                         from (select login,
                                                      id,
                                                      inferred_from_location__country,
                                                      multiIf(inferred_from_location__country global in
                                                              ['UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                                          , '欧洲',
                                                              inferred_from_location__country global in
                                                              ['New Zealand', 'Australia'],
                                                              '澳洲',
                                                              inferred_from_location__country global in
                                                              ['Canada', 'Mexico', 'United States','USA'],
                                                              '北美',
                                                              inferred_from_location__country global in
                                                              ['Japan', 'South Korea'],
                                                              '日韩',
                                                              inferred_from_location__country global in
                                                              ['China', 'Hong Kong','Taiwan'],
                                                              '中国',
                                                              inferred_from_location__country global in ['India'],
                                                              '印度',
                                                              '其他') as region
                                               from github_profile
                                               where inferred_from_location__country != '') as a global
                                                  join (select a.*, b.github_id
                                                        from (
                                                                 -- email
                                                                 select owner_id as email
                                                                 from code_owner_history_by_year_month
                                                                 where owner = 'llvm'
                                                                   and repo = 'llvm-project'
                                                                 group by owner_id) as a global
                                                                 left join (select author__id as github_id, commit__author__email as email
                                                                            from (select author__id, commit__author__email
                                                                                  from github_commits
                                                                                  where author__id != 0
                                                                                  group by author__id, commit__author__email
                                                                                  union all
                                                                                  select committer__id, commit__committer__email
                                                                                  from github_commits
                                                                                  where committer__id != 0
                                                                                  group by committer__id, commit__committer__email)
                                                                            group by github_id, email) as b
                                                                           on a.email = b.email) as b
                                                       on a.id = b.github_id) as b
                                        on a.email = b.email) as a global
                        full join (select email,
                                          groupArray([region, toString(commit_count)]) as region_count_map
                                           ,
                                          if(length(region_count_map) > 1 and region_count_map[1][1] = '0时区',
                                             region_count_map[2][1],
                                             region_count_map[1][1])                   as area
                                   from (select email, region, sum(commit_count) as commit_count
                                         from (select email,
                                                      tz,
                                                      multiIf(tz in (8), '中国',
                                                              tz in
                                                              (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12),
                                                              '北美',
                                                              tz in (1, 2, 3, 4), '欧洲', tz in (5), '印度',
                                                              tz in (10), '澳洲',
                                                              tz in (9), '日韩',
                                                              tz in (0), '0时区', '其他') as region,
                                                      sum(commit_count)                   as commit_count
                                               from (select email, tz, count() as commit_count
                                                     from (select argMax(author_email, search_key__updated_at) as email,
                                                                  argMax(author_tz, search_key__updated_at)    as tz
                                                           from gits
                                                           where author_email global in ( -- email
                                                               select owner_id as email
                                                               from code_owner_history_by_year_month
                                                               where owner = 'llvm'
                                                                 and repo = 'llvm-project'
                                                               group by owner_id)
                                                           group by hexsha)
                                                     group by email, tz

                                                     union all

                                                     select email, tz, count() as commit_count
                                                     from (select argMax(committer_email, search_key__updated_at) as email,
                                                                  argMax(committer_tz, search_key__updated_at)    as tz
                                                           from gits
                                                           where committer_email global in ( -- email
                                                               select owner_id as email
                                                               from code_owner_history_by_year_month
                                                               where owner = 'llvm'
                                                                 and repo = 'llvm-project'
                                                               group by owner_id)
                                                           group by hexsha)
                                                     group by email, tz)
                                               group by email, tz
                                               order by email, commit_count desc)
                                         group by email, region
                                         order by email, commit_count desc)
                                   group by email) as b on a.email = b.email


               group by email, inferred_area) as b on a.email = b.email
where login_or_email global not in (select commit__author__email
from (select commit__author__email
      from github_commits
      where author__login global in
            ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
      group by commit__author__email
      union all
      select commit__committer__email
      from github_commits
      where committer__login global in
            ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
      group by commit__committer__email)
group by commit__author__email)

union all

select owner,
       repo,
       'email'                as data_type,
       toUnixTimestamp(now()) as insert_at,
       email                  as login_or_email,
       'member'               as member_type,
       year_month             as month,
       '其他'                 as inferred_area
from (select owner, repo, owner_id as email, year_month
      from code_owner_history_by_year_month
      where owner = 'llvm'
        and repo = 'llvm-project'
      group by owner, repo, owner_id, year_month)
where login_or_email global not in (select login_or_email
                                    from (select owner,
                                                 repo,
                                                 'email'                as data_type,
                                                 toUnixTimestamp(now()) as insert_at,
                                                 email                  as login_or_email,
                                                 'member'               as member_type,
                                                 year_month             as month,
                                                 inferred_area

                                          from (
                                                   select owner, repo, owner_id as email, year_month
                                                   from code_owner_history_by_year_month
                                                   where owner = 'llvm'
                                                     and repo = 'llvm-project'
                                                   group by owner, repo, owner_id, year_month) as a global
                                                   join (select if(a.email != '', a.email, b.email)                as email,
                                                                if(a.inferred_area != '', a.inferred_area, b.area) as inferred_area
                                                         from (select if(b.email != '', b.email, a.email)           as email,
                                                                      if(b.region != '', b.region, a.inferred_area) as inferred_area
                                                               from (select a.*,
                                                                            multiIf(inferred_area global in
                                                                                    ['UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                                                                , '欧洲', inferred_area global in
                                                                                          ['Canada', 'Mexico', 'United States','USA'],
                                                                                    '北美',
                                                                                    inferred_area global in
                                                                                    ['China', 'Hong Kong','Taiwan'],
                                                                                    '中国',
                                                                                    inferred_area global in ['India'],
                                                                                    '印度',
                                                                                    inferred_area global in
                                                                                    ['Japan', 'South Korea'], '日韩',
                                                                                    inferred_area global in
                                                                                    ['New Zealand', 'Australia'],
                                                                                    '澳洲',
                                                                                    inferred_area) as inferred_area
                                                                     from (select a.*, b.github_id
                                                                           from (
                                                                                    -- email
                                                                                    select owner_id as email
                                                                                    from code_owner_history_by_year_month
                                                                                    where owner = 'llvm'
                                                                                      and repo = 'llvm-project'
                                                                                    group by owner_id) as a global
                                                                                    join (select author__id as github_id, commit__author__email as email
                                                                                          from (select author__id, commit__author__email
                                                                                                from github_commits
                                                                                                where author__id != 0
                                                                                                group by author__id, commit__author__email
                                                                                                union all
                                                                                                select committer__id, commit__committer__email
                                                                                                from github_commits
                                                                                                where committer__id != 0
                                                                                                group by committer__id, commit__committer__email)
                                                                                          group by github_id, email) as b
                                                                                         on a.email = b.email) as a global
                                                                              left join github_id_main_tz_map as b on a.github_id = b.github_id) as a global
                                                                        full join (select email, region
                                                                                   from (select login,
                                                                                                id,
                                                                                                inferred_from_location__country,
                                                                                                multiIf(
                                                                                                            inferred_from_location__country global in
                                                                                                            ['UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                                                                                    , '欧洲',
                                                                                                            inferred_from_location__country global in
                                                                                                            ['New Zealand', 'Australia'],
                                                                                                            '澳洲',
                                                                                                            inferred_from_location__country global in
                                                                                                            ['Canada', 'Mexico', 'United States','USA'],
                                                                                                            '北美',
                                                                                                            inferred_from_location__country global in
                                                                                                            ['Japan', 'South Korea'],
                                                                                                            '日韩',
                                                                                                            inferred_from_location__country global in
                                                                                                            ['China', 'Hong Kong','Taiwan'],
                                                                                                            '中国',
                                                                                                            inferred_from_location__country global in
                                                                                                            ['India'],
                                                                                                            '印度',
                                                                                                            '其他') as region
                                                                                         from github_profile
                                                                                         where inferred_from_location__country != '') as a global
                                                                                            join (select a.*, b.github_id
                                                                                                  from (
                                                                                                           -- email
                                                                                                           select owner_id as email
                                                                                                           from code_owner_history_by_year_month
                                                                                                           where owner = 'llvm'
                                                                                                             and repo = 'llvm-project'
                                                                                                           group by owner_id) as a global
                                                                                                           left join (select author__id as github_id, commit__author__email as email
                                                                                                                      from (select author__id, commit__author__email
                                                                                                                            from github_commits
                                                                                                                            where author__id != 0
                                                                                                                            group by author__id, commit__author__email
                                                                                                                            union all
                                                                                                                            select committer__id, commit__committer__email
                                                                                                                            from github_commits
                                                                                                                            where committer__id != 0
                                                                                                                            group by committer__id, commit__committer__email)
                                                                                                                      group by github_id, email) as b
                                                                                                                     on a.email = b.email) as b
                                                                                                 on a.id = b.github_id) as b
                                                                                  on a.email = b.email) as a global
                                                                  full join (select email,
                                                                                    groupArray([region, toString(commit_count)]) as region_count_map
                                                                                     ,
                                                                                    if(length(region_count_map) > 1 and
                                                                                       region_count_map[1][1] = '0时区',
                                                                                       region_count_map[2][1],
                                                                                       region_count_map[1][1])                   as area
                                                                             from (select email, region, sum(commit_count) as commit_count
                                                                                   from (select email,
                                                                                                tz,
                                                                                                multiIf(tz in (8),
                                                                                                        '中国',
                                                                                                        tz in
                                                                                                        (-1, -2, -3, -4,
                                                                                                         -5, -6, -7, -8,
                                                                                                         -9, -10, -11,
                                                                                                         -12),
                                                                                                        '北美',
                                                                                                        tz in
                                                                                                        (1, 2, 3, 4),
                                                                                                        '欧洲',
                                                                                                        tz in (5),
                                                                                                        '印度',
                                                                                                        tz in (10),
                                                                                                        '澳洲',
                                                                                                        tz in (9),
                                                                                                        '日韩',
                                                                                                        tz in (0),
                                                                                                        '0时区',
                                                                                                        '其他')   as region,
                                                                                                sum(commit_count) as commit_count
                                                                                         from (select email, tz, count() as commit_count
                                                                                               from (select argMax(author_email, search_key__updated_at) as email,
                                                                                                            argMax(author_tz, search_key__updated_at)    as tz
                                                                                                     from gits
                                                                                                     where author_email global in
                                                                                                           ( -- email
                                                                                                               select owner_id as email
                                                                                                               from code_owner_history_by_year_month
                                                                                                               where owner = 'llvm'
                                                                                                                 and repo = 'llvm-project'
                                                                                                               group by owner_id)
                                                                                                     group by hexsha)
                                                                                               group by email, tz

                                                                                               union all

                                                                                               select email, tz, count() as commit_count
                                                                                               from (select argMax(committer_email, search_key__updated_at) as email,
                                                                                                            argMax(committer_tz, search_key__updated_at)    as tz
                                                                                                     from gits
                                                                                                     where committer_email global in
                                                                                                           ( -- email
                                                                                                               select owner_id as email
                                                                                                               from code_owner_history_by_year_month
                                                                                                               where owner = 'llvm'
                                                                                                                 and repo = 'llvm-project'
                                                                                                               group by owner_id)
                                                                                                     group by hexsha)
                                                                                               group by email, tz)
                                                                                         group by email, tz
                                                                                         order by email, commit_count desc)
                                                                                   group by email, region
                                                                                   order by email, commit_count desc)
                                                                             group by email) as b on a.email = b.email


                                                         group by email, inferred_area) as b on a.email = b.email
                                          where login_or_email global not in (select commit__author__email
                                                                              from (select commit__author__email
                                                                                    from github_commits
                                                                                    where author__login global in
                                                                                          ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                                                                                              'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
                                                                                    group by commit__author__email
                                                                                    union all
                                                                                    select commit__committer__email
                                                                                    from github_commits
                                                                                    where committer__login global in
                                                                                          ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                                                                                              'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
                                                                                    group by commit__committer__email)
                                                                              group by commit__author__email))
                                    group by login_or_email)
  -- 去掉机器人
  and login_or_email global not in (select commit__author__email
                                    from (select commit__author__email
                                          from github_commits
                                          where author__login global in
                                                ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                                                    'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
                                          group by commit__author__email
                                          union all
                                          select commit__committer__email
                                          from github_commits
                                          where committer__login global in
                                                ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                                                    'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
                                          group by commit__committer__email)
                                    group by commit__author__email)









-- 插入code_owners_location
insert into table code_owners_location
select owner,
       repo,
       'email'                as data_type,
       toUnixTimestamp(now()) as insert_at,
       email                  as login_or_email,
       member_type,
       month,
       inferred_area

from (
         select search_key__owner                     as owner,
                search_key__repo                      as repo,
                email,
                toYYYYMM(authored_date)               as month,
                JSONExtractString(misc, 'owner_type') as member_type
         from code_owners
         where search_key__repo = 'linux'
         group by owner, repo, month, email, member_type) as a global
         join (select if(a.email != '', a.email, b.email)                as email,
                      if(a.inferred_area != '', a.inferred_area, b.area) as inferred_area
               from (select if(b.email != '', b.email, a.email)           as email,
                            if(b.region != '', b.region, a.inferred_area) as inferred_area
                     from (select a.*,
                                  multiIf(inferred_area global in
                                          ['UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                      , '欧洲', inferred_area global in
                                                ['Canada', 'Mexico', 'United States','USA'],
                                          '北美',
                                          inferred_area global in ['China', 'Hong Kong','Taiwan'],
                                          '中国',
                                          inferred_area global in ['India'],
                                          '印度',
                                          inferred_area global in ['Japan', 'South Korea'], '日韩',
                                          inferred_area global in ['New Zealand', 'Australia'], '澳洲',
                                          inferred_area) as inferred_area
                           from (select a.*, b.github_id
                                 from (select email
                                       from code_owners
                                       where search_key__repo = 'linux'
                                       group by email) as a global
                                          join (select author__id as github_id, commit__author__email as email
                                                from (select author__id, commit__author__email
                                                      from github_commits
                                                      where author__id != 0
                                                      group by author__id, commit__author__email
                                                      union all
                                                      select committer__id, commit__committer__email
                                                      from github_commits
                                                      where committer__id != 0
                                                      group by committer__id, commit__committer__email)
                                                group by github_id, email) as b on a.email = b.email) as a global
                                    left join github_id_main_tz_map as b on a.github_id = b.github_id) as a global
                              full join (select email, region
                                         from (select login,
                                                      id,
                                                      inferred_from_location__country,
                                                      multiIf(inferred_from_location__country global in
                                                              ['UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                                          , '欧洲',
                                                              inferred_from_location__country global in
                                                              ['New Zealand', 'Australia'],
                                                              '澳洲',
                                                              inferred_from_location__country global in
                                                              ['Canada', 'Mexico', 'United States','USA'],
                                                              '北美',
                                                              inferred_from_location__country global in
                                                              ['Japan', 'South Korea'],
                                                              '日韩',
                                                              inferred_from_location__country global in
                                                              ['China', 'Hong Kong','Taiwan'],
                                                              '中国',
                                                              inferred_from_location__country global in ['India'],
                                                              '印度',
                                                              '其他') as region
                                               from github_profile
                                               where inferred_from_location__country != '') as a global
                                                  join (select a.*, b.github_id
                                                        from (
                                                                 select email
                                                                 from code_owners
                                                                 where search_key__repo = 'linux'
                                                                 group by email) as a global
                                                                 left join (select author__id as github_id, commit__author__email as email
                                                                            from (select author__id, commit__author__email
                                                                                  from github_commits
                                                                                  where author__id != 0
                                                                                  group by author__id, commit__author__email
                                                                                  union all
                                                                                  select committer__id, commit__committer__email
                                                                                  from github_commits
                                                                                  where committer__id != 0
                                                                                  group by committer__id, commit__committer__email)
                                                                            group by github_id, email) as b
                                                                           on a.email = b.email) as b
                                                       on a.id = b.github_id) as b
                                        on a.email = b.email) as a global
                        full join (select email,
                                          groupArray([region, toString(commit_count)]) as region_count_map
                                           ,
                                          if(length(region_count_map) > 1 and region_count_map[1][1] = '0时区',
                                             region_count_map[2][1],
                                             region_count_map[1][1])                   as area
                                   from (select email, region, sum(commit_count) as commit_count
                                         from (select email,
                                                      tz,
                                                      multiIf(tz in (8), '中国',
                                                              tz in
                                                              (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12),
                                                              '北美',
                                                              tz in (1, 2, 3, 4), '欧洲', tz in (5), '印度',
                                                              tz in (10), '澳洲',
                                                              tz in (9), '日韩',
                                                              tz in (0), '0时区', '其他') as region,
                                                      sum(commit_count)                   as commit_count
                                               from (select email, tz, count() as commit_count
                                                     from (select argMax(author_email, search_key__updated_at) as email,
                                                                  argMax(author_tz, search_key__updated_at)    as tz
                                                           from gits
                                                           where author_email global in (select email
                                                                                         from code_owners
                                                                                         where search_key__repo = 'linux'
                                                                                         group by email)
                                                           group by hexsha)
                                                     group by email, tz

                                                     union all

                                                     select email, tz, count() as commit_count
                                                     from (select argMax(committer_email, search_key__updated_at) as email,
                                                                  argMax(committer_tz, search_key__updated_at)    as tz
                                                           from gits
                                                           where committer_email global in (select email
                                                                                            from code_owners
                                                                                            where search_key__repo = 'linux'
                                                                                            group by email)
                                                           group by hexsha)
                                                     group by email, tz)
                                               group by email, tz
                                               order by email, commit_count desc)
                                         group by email, region
                                         order by email, commit_count desc)
                                   group by email) as b on a.email = b.email


               group by email, inferred_area) as b on a.email = b.email
where login_or_email global not in (select commit__author__email
from (select commit__author__email
      from github_commits
      where author__login global in
            ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
      group by commit__author__email
      union all
      select commit__committer__email
      from github_commits
      where committer__login global in
            ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
      group by commit__committer__email)
group by commit__author__email)

union all
-- 没推断出地理位置的
select owner,
       repo,
       'email'                as data_type,
       toUnixTimestamp(now()) as insert_at,
       email                  as login_or_email,
       member_type,
       month,
       '其他' as inferred_area from (
         select search_key__owner                     as owner,
                search_key__repo                      as repo,
                email,
                toYYYYMM(authored_date)               as month,
                JSONExtractString(misc, 'owner_type') as member_type
         from code_owners
         where search_key__repo = 'linux'
         group by owner, repo, month, email, member_type)
where login_or_email global not in (select login_or_email from (select owner,
       repo,
       'email'                as data_type,
       toUnixTimestamp(now()) as insert_at,
       email                  as login_or_email,
       member_type,
       month,
       inferred_area

from (
         select search_key__owner                     as owner,
                search_key__repo                      as repo,
                email,
                toYYYYMM(authored_date)               as month,
                JSONExtractString(misc, 'owner_type') as member_type
         from code_owners
         where search_key__repo = 'linux'
         group by owner, repo, month, email, member_type) as a global
         join (select if(a.email != '', a.email, b.email)                as email,
                      if(a.inferred_area != '', a.inferred_area, b.area) as inferred_area
               from (select if(b.email != '', b.email, a.email)           as email,
                            if(b.region != '', b.region, a.inferred_area) as inferred_area
                     from (select a.*,
                                  multiIf(inferred_area global in
                                          ['UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                      , '欧洲', inferred_area global in
                                                ['Canada', 'Mexico', 'United States','USA'],
                                          '北美',
                                          inferred_area global in ['China', 'Hong Kong','Taiwan'],
                                          '中国',
                                          inferred_area global in ['India'],
                                          '印度',
                                          inferred_area global in ['Japan', 'South Korea'], '日韩',
                                          inferred_area global in ['New Zealand', 'Australia'], '澳洲',
                                          inferred_area) as inferred_area
                           from (select a.*, b.github_id
                                 from (select email
                                       from code_owners
                                       where search_key__repo = 'linux'
                                       group by email) as a global
                                          join (select author__id as github_id, commit__author__email as email
                                                from (select author__id, commit__author__email
                                                      from github_commits
                                                      where author__id != 0
                                                      group by author__id, commit__author__email
                                                      union all
                                                      select committer__id, commit__committer__email
                                                      from github_commits
                                                      where committer__id != 0
                                                      group by committer__id, commit__committer__email)
                                                group by github_id, email) as b on a.email = b.email) as a global
                                    left join github_id_main_tz_map as b on a.github_id = b.github_id) as a global
                              full join (select email, region
                                         from (select login,
                                                      id,
                                                      inferred_from_location__country,
                                                      multiIf(inferred_from_location__country global in
                                                              ['UK','Europe','British indian ocean territory','Czechia','Russia', 'Sweden', 'Romania', 'Ukraine', 'Finland', 'Norway', 'Portugal', 'France', 'Poland', 'Italy', 'Spain', 'Estonia', 'Austria', 'Germany', 'Denmark', 'United Kingdom', 'Luxembourg', 'Switzerland', 'Netherlands','Turkey','Montenegro']
                                                          , '欧洲',
                                                              inferred_from_location__country global in
                                                              ['New Zealand', 'Australia'],
                                                              '澳洲',
                                                              inferred_from_location__country global in
                                                              ['Canada', 'Mexico', 'United States','USA'],
                                                              '北美',
                                                              inferred_from_location__country global in
                                                              ['Japan', 'South Korea'],
                                                              '日韩',
                                                              inferred_from_location__country global in
                                                              ['China', 'Hong Kong','Taiwan'],
                                                              '中国',
                                                              inferred_from_location__country global in ['India'],
                                                              '印度',
                                                              '其他') as region
                                               from github_profile
                                               where inferred_from_location__country != '') as a global
                                                  join (select a.*, b.github_id
                                                        from (
                                                                 select email
                                                                 from code_owners
                                                                 where search_key__repo = 'linux'
                                                                 group by email) as a global
                                                                 left join (select author__id as github_id, commit__author__email as email
                                                                            from (select author__id, commit__author__email
                                                                                  from github_commits
                                                                                  where author__id != 0
                                                                                  group by author__id, commit__author__email
                                                                                  union all
                                                                                  select committer__id, commit__committer__email
                                                                                  from github_commits
                                                                                  where committer__id != 0
                                                                                  group by committer__id, commit__committer__email)
                                                                            group by github_id, email) as b
                                                                           on a.email = b.email) as b
                                                       on a.id = b.github_id) as b
                                        on a.email = b.email) as a global
                        full join (select email,
                                          groupArray([region, toString(commit_count)]) as region_count_map
                                           ,
                                          if(length(region_count_map) > 1 and region_count_map[1][1] = '0时区',
                                             region_count_map[2][1],
                                             region_count_map[1][1])                   as area
                                   from (select email, region, sum(commit_count) as commit_count
                                         from (select email,
                                                      tz,
                                                      multiIf(tz in (8), '中国',
                                                              tz in
                                                              (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12),
                                                              '北美',
                                                              tz in (1, 2, 3, 4), '欧洲', tz in (5), '印度',
                                                              tz in (10), '澳洲',
                                                              tz in (9), '日韩',
                                                              tz in (0), '0时区', '其他') as region,
                                                      sum(commit_count)                   as commit_count
                                               from (select email, tz, count() as commit_count
                                                     from (select argMax(author_email, search_key__updated_at) as email,
                                                                  argMax(author_tz, search_key__updated_at)    as tz
                                                           from gits
                                                           where author_email global in (select email
                                                                                         from code_owners
                                                                                         where search_key__repo = 'linux'
                                                                                         group by email)
                                                           group by hexsha)
                                                     group by email, tz

                                                     union all

                                                     select email, tz, count() as commit_count
                                                     from (select argMax(committer_email, search_key__updated_at) as email,
                                                                  argMax(committer_tz, search_key__updated_at)    as tz
                                                           from gits
                                                           where committer_email global in (select email
                                                                                            from code_owners
                                                                                            where search_key__repo = 'linux'
                                                                                            group by email)
                                                           group by hexsha)
                                                     group by email, tz)
                                               group by email, tz
                                               order by email, commit_count desc)
                                         group by email, region
                                         order by email, commit_count desc)
                                   group by email) as b on a.email = b.email


               group by email, inferred_area) as b on a.email = b.email
where login_or_email global not in (select commit__author__email
from (select commit__author__email
      from github_commits
      where author__login global in
            ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
      group by commit__author__email
      union all
      select commit__committer__email
      from github_commits
      where committer__login global in
            ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
      group by commit__committer__email)
group by commit__author__email)) group by login_or_email)
  -- 去掉机器人
  and login_or_email global not in (select commit__author__email
                                    from (select commit__author__email
                                          from github_commits
                                          where author__login global in
                                                ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                                                    'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
                                          group by commit__author__email
                                          union all
                                          select commit__committer__email
                                          from github_commits
                                          where committer__login global in
                                                ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                                                    'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]']
                                          group by commit__committer__email)
                                    group by commit__author__email)