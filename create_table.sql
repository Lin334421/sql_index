-- major_tech_firms_devp_sha_v1 通过owner repo sha 获取 某个commit 目的是找出author 在github上的信息和commit 关联的pr
create table if not exists default.major_tech_firms_devp_sha_v1_local on cluster replicated
(
    github_login                          String,
    crawl_at                              String,
    crawl_timestamp                       Int64,
    data_insert_at                        Int64,
    sha                                   String,
    owner                                 String,
    repo                                  String,
    authored_at                           String,
    hash_email                            String,
    additions                             Int64,
    deletions                             Int64,
    message                               String,
    parents__totalCount                   Int64,
    author__user__login                   String,
    author__user__name                    String,
    author__user__company                 String,
    author__user__location                String,
    author__user__email                   String,
    author__user__bio                     String,
    author__name                          String,
    author__email                         String,
    author__date                          String,
    committer__user__login                String,
    committer__user__name                 String,
    committer__user__company              String,
    committer__user__location             String,
    committer__user__email                String,
    committer__user__bio                  String,
    committer__name                       String,
    committer__email                      String,
    committer__date                       String,
    `associatedPullRequests.author__login` Array(String),
    `associatedPullRequests.merged`        Array(String),
    `associatedPullRequests.mergedAt`      Array(String),
    `associatedPullRequests.closed`        Array(String),
    `associatedPullRequests.closedAt`      Array(String),
    `associatedPullRequests.createAt`      Array(String),
    `associatedPullRequests.body`          Array(String),
    `associatedPullRequests.title`         Array(String),
    `associatedPullRequests.number`        Array(Int64),
    `associatedPullRequests.url`           Array(String)
)
    engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/major_tech_firms_devp_sha_v1_local', '{replica}',
             data_insert_at)
        ORDER BY (github_login, sha)
        SETTINGS index_granularity = 8192;


create table default.major_tech_firms_devp_sha_v1 on cluster replicated as major_tech_firms_devp_sha_v1_local
    engine = Distributed('replicated', 'default', 'major_tech_firms_devp_sha_v1_local', halfMD5(github_login));



-- github_login_email_experiences
CREATE TABLE IF NOT EXISTS default.github_login_email_experiences_local
    on cluster replicated
(
    github_login String,
    company      String,
    start        Int64,
    end          Int64,
    insert_at    Int64
) engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/cleaned_mini_push_event_local',
           '{replica}', insert_at)
      ORDER BY (github_login, company, start, end)
      SETTINGS index_granularity = 8192;

create table default.github_login_email_experiences on cluster replicated as github_login_email_experiences_local
    engine = Distributed
(
    'replicated',
    'default',
    'github_login_email_experiences_local',
    insert_at
);

-- OPTIMIZE TABLE 模版
OPTIMIZE TABLE gits_local on cluster replicated partition 'NVIDIA'
