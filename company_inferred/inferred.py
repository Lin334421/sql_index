from statistics.ck_hook import CKServer
from statistics.config import clickhouse_server_info

# 需要项目主导公司映射表

ck = CKServer(host=clickhouse_server_info["HOST"],
              port=clickhouse_server_info["PORT"],
              user=clickhouse_server_info["USER"],
              password=clickhouse_server_info["PASSWD"],
              database=clickhouse_server_info["DATABASE"])
# 主导公司表
# 项目 主导公司
# 如果为头部贡献开发者且没有任何公司痕迹则将该开发者推断为该项目的主导公司
repo_main_company_map = {
    "owner_repo": "company"
}




