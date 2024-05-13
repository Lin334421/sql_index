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