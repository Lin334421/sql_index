# -*-coding:utf-8-*-
import csv
import time

from statistics.ck_hook import CKServer
from statistics.config import clickhouse_server_info

bulk_data = []


with open('kernel-maintainers-地域.csv','r') as f:
    cr = csv.reader(f)
    next(cr)
    for row in cr:
        bulk_data.append({
            "email":row[0],
            "name":row[1],
            "inferred_area":row[2]
        })



print(bulk_data)



ck_client = CKServer(host=clickhouse_server_info["HOST"],
                     port=clickhouse_server_info["PORT"],
                     user=clickhouse_server_info["USER"],
                     password=clickhouse_server_info["PASSWD"],
                     database=clickhouse_server_info["DATABASE"])
ck_client.execute("insert into table linux_maintainer_area values",bulk_data)
