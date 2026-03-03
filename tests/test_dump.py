

# path("dump/", DumpDataView.as_view(), name="datadump"),
#     path("start_dump_creation/<int:dump_id>/", StartDumpView.as_view(), name="datadump"),
#     path("restore_dump/<int:restore_dump_id>/", StartDumpRestoreView.as_view(), name="restore_dump"),
#     path("restore_details/", DumpRestoreView.as_view(), name="restore_details")
# ]


import requests

url = "http://localhost:8002"

dump_post = {
    "dump_name": "rohit_dump_test",
    "config": {
        "db_type": "mysql",
        "user": "root",
        "password": "123456789",
        "host": "localhost",
        "port": "3306",
        "db_names": ["nddenttest"],
        "start_time": None,
        "end_time": None
    }
}

# dump_get = requests.get(url + "/dump/")
# print(dump_get.content)

# dump_get = requests.post(
#     url + "/dump/", 
#     json=dump_post, 
#     headers={"Content-Type": "application/json"}
# )
# print(dump_get.content)

# dump_get = requests.get(url + "/start_dump_creation/1/")
# print(dump_get.content)



restore_post = {
    "dump_id": 1,
    "config": {
        "db_type": "mysql",
        "user": "root",
        "password": "123456789",
        "host": "localhost",
        "port": "3306",
        "database": "restore_rohit",
    }
}
# restore = requests.post(
#     url + "/restore_details/", 
#     json=restore_post, 
#     headers={"Content-Type": "application/json"}
# )
# print(restore.content)


dump_get = requests.get(url + "/restore_dump/1/")
print(dump_get.content)
