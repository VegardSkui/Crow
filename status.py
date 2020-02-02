import pymysql.cursors

connection = pymysql.connect(
    host="localhost",
    user="root",
    password="",
    db="crow",
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

try:
    with connection.cursor() as cursor:
        sql = "SELECT `table_schema`, ROUND(SUM(`data_length` + `index_length`) / 1024 / 1024, 1) AS `size` FROM `information_schema`.`tables` WHERE `table_schema` = 'crow'"
        cursor.execute(sql)
        db_size = cursor.fetchone()["size"]

        sql = "SELECT COUNT(host) AS `count` FROM `queue`"
        cursor.execute(sql)
        queue_count = cursor.fetchone()["count"]

        sql = "SELECT COUNT(DISTINCT `protocol`, `host`, `port`) AS `count` FROM `queue`"
        cursor.execute(sql)
        queue_unique_host_count = cursor.fetchone()["count"]

        sql = "SELECT COUNT(host) AS `count` FROM `resources`"
        cursor.execute(sql)
        resource_count = cursor.fetchone()["count"]

        sql = "SELECT COUNT(DISTINCT `protocol`, `host`, `port`) AS `count` FROM `resources`"
        cursor.execute(sql)
        resource_unique_host_count = cursor.fetchone()["count"]

        sql = "SELECT COUNT(host) AS `count` FROM `misses`"
        cursor.execute(sql)
        miss_count = cursor.fetchone()["count"]

        sql = "SELECT COUNT(DISTINCT `protocol`, `host`, `port`) AS `count` FROM `misses`"
        cursor.execute(sql)
        miss_unique_host_count = cursor.fetchone()["count"]

    print("Crow\n")
    print(f"Database size is {db_size} MB")
    print(f"{queue_count} links are in the queue, including {queue_unique_host_count} unique hosts")
    print(f"{resource_count} resources have been found, including {resource_unique_host_count} unique hosts")
    print(f"There have been {miss_count} misses, including {miss_unique_host_count} unique hosts")
finally:
    connection.close()
