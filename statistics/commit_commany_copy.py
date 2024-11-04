from clickhouse_driver import Client, connect

from statistics.config import clickhouse_server_info


class CKServer:
    def __init__(self, host, port, user, password, database, settings={}, kwargs={}):
        self.client = Client(host=host, port=port, user=user, password=password, database=database, settings=settings,
                             **kwargs)
        self.connect = connect(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.connect.cursor()

    def execute(self, sql: object, params: list) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params)
        return result

    def execute_use_setting(self, sql: object, params: list, settings) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params, settings=settings)
        return result

    def execute_no_params(self, sql: object):
        result = self.client.execute(sql)
        return result

    def fetchall(self, sql):
        result = self.client.execute(sql)
        return result

    def close(self):
        self.client.disconnect()

repos = [
    {
        "owner": "Project-MONAI",
        "repo": "MONAI"
    },

    {
        "owner": "NVIDIA",
        "repo": "NeMo-Guardrails"
    },
    {
        "owner": "onnx",
        "repo": "onnx-tensorrt"
    },
    {
        "owner": "NVIDIA",
        "repo": "TransformerEngine"
    },
    {
        "owner": "pytorch",
        "repo": "TensorRT"
    },
    {
        "owner": "NVIDIA-Omniverse",
        "repo": "PhysX"
    },
    {
        "owner": "PixarAnimationStudios",
        "repo": "USD"
    },
    {
        "owner": "NVIDIA-Omniverse",
        "repo": "USD-proposals"
    },
    {
        "owner": "NVIDIA",
        "repo": "AMGX"
    },
    {
        "owner": "rapidsai",
        "repo": "rmm"
    },
    {
        "owner": "rapidsai",
        "repo": "raft"
    },
    {
        "owner": "rapidsai",
        "repo": "cuxfilter"
    },
    {
        "owner": "rapidsai",
        "repo": "cugraph"
    },
    {
        "owner": "rapidsai",
        "repo": "cudf"
    },
    {
        "owner": "CVCUDA",
        "repo": "CV-CUDA"
    },
    {
        "owner": "NVIDIA",
        "repo": "Megatron-LM"
    }
]





ck_client = CKServer(host=clickhouse_server_info["HOST"],
                     port=clickhouse_server_info["PORT"],
                     user=clickhouse_server_info["USER"],
                     password=clickhouse_server_info["PASSWD"],
                     database=clickhouse_server_info["DATABASE"])
for owner_repo in repos:
    owner = owner_repo['owner']
    repo = owner_repo['repo']
    sql_ = f"""
insert into table commit_company
select *, toUnixTimestamp(now())
from (select search_key__owner,
             search_key__repo,
             author__login,
             commit__message,
             sha,
             commit__author__email,
             commit__author__date,
             company
      from (select *
            from github_commits
            where search_key__owner = '{owner}'
              and search_key__repo = '{repo}'
              and author__login global not in
                  (select robot_login_email from robot_login_email)
              and author__login != ''
              and length(parents.sha) = 1) as a global
               left join (select *
                          from (select login,
                                       company,
                                       min(start_at) as start_at,
                                       max(end_at)   as end_at
                                from (select a.*, b.company
                                      from (
                                               -- 一个login的一个邮箱后缀的起始时间和结束时间
                                               select login,
                                                      splitByChar('@', email)[2] as email_domain,
                                                      min(month)                 as start_at,
                                                      max(month)                 as end_at
                                               from (select commits.author_github_login as login,
                                                            commits.author_email        as email,
                                                            toInt64(concat(
                                                                    splitByChar('-', substring(`commits.author_date`, 1, 10))[1],
                                                                    splitByChar('-', substring(`commits.author_date`, 1, 10))[2]
                                                                --                                 ,
--                                     splitByChar('-', substring(`commits.author_date`, 1, 10))[3]
                                                                    ))                  as month
                                                     from nvidia_contributor_pr_v3 array join commits.author_github_login, commits.author_email, `commits.author_date`
                                                     where login != ''
                                                     union all
                                                     select author__login,
                                                            commit__author__email,
                                                            toYYYYMM(commit__author__date) as month
                                                     from github_commits
                                                     where author__login != ''
                                                       and author__login global not in
                                                           (select robot_login_email from robot_login_email)
                                                        )
                                               group by login, email_domain) as a global
                                               join company_email_map as b on a.email_domain = b.email_domain)
                                group by login, company)) as b on a.author__login = b.login
      where
        -- author_login为空值那就去掉
          toYYYYMM(commit__author__date) >= start_at
        and toYYYYMM(commit__author__date) <= end_at
      union all
      select search_key__owner,
             search_key__repo,
             author__login,
             commit__message,
             sha,
             commit__author__email,
             commit__author__date,
             company
      from (select *
            from github_commits
            where search_key__owner = '{owner}'
              and search_key__repo = '{repo}'
              and author__login global not in
                  (select robot_login_email from robot_login_email)
              and length(parents.sha) = 1
              and author__login = '') as a global
               join (select * from company_email_map) as b
                    on splitByChar('@', a.commit__author__email)[2] = b.email_domain
      union all
      select a.*, if(b.company = 'facebook', 'meta', b.company) as company
      from (select search_key__owner,
                   search_key__repo,
                   author__login,
                   commit__message,
                   sha,
                   commit__author__email,
                   commit__author__date
            from github_commits
            where search_key__owner = '{owner}'
              and search_key__repo = '{repo}'
              and author__login global not in
                  (select robot_login_email from robot_login_email)
              and length(parents.sha) = 1
              and sha global not in (select sha
                                     from (select author__login,
                                                  sha,
                                                  commit__author__email,
                                                  commit__author__date,
                                                  company
                                           from (select *
                                                 from github_commits
                                                 where search_key__owner = '{owner}'
                                                   and search_key__repo = '{repo}'
                                                   and author__login global not in
                                                       (select robot_login_email from robot_login_email)
                                                   and author__login != ''
                                                   and length(parents.sha) = 1) as a global
                                                    left join (select *
                                                               from (select login,
                                                                            company,
                                                                            min(start_at) as start_at,
                                                                            max(end_at)   as end_at
                                                                     from (select a.*, b.company
                                                                           from (
                                                                                    -- 一个login的一个邮箱后缀的起始时间和结束时间
                                                                                    select login,
                                                                                           splitByChar('@', email)[2] as email_domain,
                                                                                           min(month)                 as start_at,
                                                                                           max(month)                 as end_at
                                                                                    from (select commits.author_github_login as login,
                                                                                                 commits.author_email        as email,
                                                                                                 toInt64(concat(
                                                                                                         splitByChar('-', substring(`commits.author_date`, 1, 10))[1],
                                                                                                         splitByChar('-', substring(`commits.author_date`, 1, 10))[2]
                                                                                                     --                                 ,
--                                     splitByChar('-', substring(`commits.author_date`, 1, 10))[3]
                                                                                                         ))                  as month
                                                                                          from nvidia_contributor_pr_v3 array join commits.author_github_login, commits.author_email, `commits.author_date`
                                                                                          where login != ''
                                                                                          union all
                                                                                          select author__login,
                                                                                                 commit__author__email,
                                                                                                 toYYYYMM(commit__author__date) as month
                                                                                          from github_commits
                                                                                          where author__login != ''
                                                                                            and
                                                                                              author__login global not in
                                                                                              (select robot_login_email from robot_login_email)
                                                                                             )
                                                                                    group by login, email_domain) as a global
                                                                                    join company_email_map as b on a.email_domain = b.email_domain)
                                                                     group by login, company)) as b
                                                              on a.author__login = b.login
                                           where
                                             -- author_login为空值那就去掉
                                               toYYYYMM(commit__author__date) >= start_at
                                             and toYYYYMM(commit__author__date) <= end_at
                                           union all
                                           select author__login,
                                                  sha,
                                                  commit__author__email,
                                                  commit__author__date,
                                                  company
                                           from (select *
                                                 from github_commits
                                                 where search_key__owner = '{owner}'
                                                   and search_key__repo = '{repo}'
                                                   and author__login global not in
                                                       (select robot_login_email from robot_login_email)
                                                   and length(parents.sha) = 1
                                                   and author__login = '') as a global
                                                    join (select * from company_email_map) as b
                                                         on splitByChar('@', a.commit__author__email)[2] = b.email_domain)
                                     group by sha)) as a global
               left join (select a.*,
                                 b.company_commit_map                                as company_commit_map_by_all_commit,
                                 b.inferred_company_by_commit_count                  as inferred_company_by_all_commit_count,
                                 multiIf(inferred_company_by_commit_count = '' and profile_company = '' and
                                         inferred_company_by_all_commit_count != '',
                                         inferred_company_by_all_commit_count,
                                         inferred_company_by_commit_count != '', inferred_company_by_commit_count,
                                         inferred_company_by_commit_count = '' and
                                         inferred_company_by_all_commit_count != '',
                                         inferred_company_by_all_commit_count,
                                         inferred_company_by_commit_count = '' and
                                         inferred_company_by_all_commit_count = '' and
                                         profile_company != '', profile_company, '') as company
                          from (select a.*,
                                       final_company_inferred_from_company as profile_company
                                from (select author__login,
                                             groupArray((company, at_company_commit_count)) as company_commit_map
                                              ,
                                             if(length(company_commit_map) != 1 and company_commit_map[1].1 = '',
                                                company_commit_map[2].1,
                                                company_commit_map[1].1)                       inferred_company_by_commit_count
                                      from (select author__login, company, sum(commit_count) as at_company_commit_count
                                            from (select a.*, b.company
                                                  from (select author__login,
                                                               splitByChar('@', commit__author__email)[2] as email_domain,
                                                               count()                                    as commit_count
                                                        from (select author__login, sha, commit__author__email
                                                              from github_commits
                                                              where search_key__owner = '{owner}'
                                                                and search_key__repo = '{repo}'
                                                                and author__login global not in
                                                                    (select robot_login_email from robot_login_email)
                                                                and author__login != ''
                                                              group by author__login, sha, commit__author__email)
                                                        group by author__login, email_domain) as a global
                                                           left join (select * from company_email_map) as b on a.email_domain = b.email_domain)
                                            group by author__login, company
                                            order by author__login, at_company_commit_count desc)
                                      group by author__login) as a global
                                         left join (
                                    select login, final_company_inferred_from_company
                                    from github_profile
                                    where final_company_inferred_from_company != ''
                                    group by login, final_company_inferred_from_company
                                    ) as b on a.author__login = b.login) as a global
                                   left join (select author__login,
                                                     groupArray((company, at_company_commit_count)) as company_commit_map
                                                      ,
                                                     if(length(company_commit_map) != 1 and
                                                        company_commit_map[1].1 = '',
                                                        company_commit_map[2].1,
                                                        company_commit_map[1].1)                       inferred_company_by_commit_count
                                              from (select author__login,
                                                           company,
                                                           sum(commit_count) as at_company_commit_count
                                                    from (select a.*, b.company
                                                          from (select author__login,
                                                                       splitByChar('@', commit__author__email)[2] as email_domain,
                                                                       count()                                    as commit_count
                                                                from (select author__login, sha, commit__author__email
                                                                      from github_commits
                                                                      where author__login global not in
                                                                            (select robot_login_email from robot_login_email)
                                                                        and author__login != ''
                                                                      group by author__login, sha, commit__author__email)
                                                                group by author__login, email_domain) as a global
                                                                   left join (select * from company_email_map) as b on a.email_domain = b.email_domain)
                                                    group by author__login, company
                                                    order by author__login, at_company_commit_count desc)
                                              group by author__login) as b on a.author__login = b.author__login) as b
                         on a.author__login = b.author__login)
"""
    ck_client.execute_no_params(sql_)
    print(f'successful to insert commit_company_map {owner}:::{repo}')
