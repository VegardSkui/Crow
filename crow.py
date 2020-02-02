import datetime
import hashlib
import pymysql.cursors
import re
import time
import urllib.request

connection = pymysql.connect(
    host="localhost",
    user="root",
    password="",
    db="crow",
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

linky = re.compile(r'(href|src)="(.*?)"')
queue = []


def retrieve(protocol, host, port, path):
    url = f"{protocol}://{host}:{port}{path}"
    with urllib.request.urlopen(url, timeout=5) as response:
        # Must have 200 response
        if response.status != 200:
            print("didnt return 200 :-(", response.status, response.reason)
            return

        content = response.read()

        hash = hashlib.md5(content).hexdigest()
        type = re.compile(r"^[^;]*").match(response.getheader("Content-Type")).group()
        last_changed = last_retrieved = datetime.datetime.now().strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        links = []

        if type == "text/html":
            try:
                raw_links = list(
                    map(
                        lambda groups: groups[1], linky.findall(content.decode("utf-8"))
                    )
                )
                for raw_link in raw_links:
                    link = decode_raw_link(raw_link)
                    if link:
                        links.append(link)
                    #    enqueue(*link)
            except UnicodeDecodeError:
                print("skipping links, couldnt decode to utf-8")

        return (
            protocol,
            host,
            port,
            path,
            hash,
            type,
            last_changed,
            last_retrieved,
            links,
        )


def decode_raw_link(raw_link):
    protocol_match = re.compile(r"(.*):\/\/").match(raw_link)
    if not protocol_match:
        return
    protocol = protocol_match.group(1)

    link = raw_link.replace(f"{protocol}://", "")

    host_match = re.compile(r"(.*?)(:|\/)").match(link)
    if not host_match:
        return
    host = host_match.group(1)

    link = link.replace(host, "")

    port_match = re.compile(r"^:(\d*)").match(link)
    if port_match:
        port = port_match.group(1)
    else:
        if protocol == "http":
            port = 80
        elif protocol == "https":
            port = 443
        else:
            return

    if port_match:
        path = link[1 + len(str(port)) :]
    else:
        path = link

    # discard GET params and hash
    param_match = re.compile(r"(.*?)(\?|&|#)").match(path)
    if param_match:
        path = param_match.group(1)

    # links with long paths
    if len(path) > 512:
        print("discarded because of long path!!")
        return

    return protocol, host, port, path


def enqueue(protocol, host, port, path):
    # Check if already in queue
    with connection.cursor() as cursor:
        sql = "SELECT EXISTS(SELECT * FROM `queue` WHERE `protocol` = %s AND `host` = %s AND `port` = %s AND `path` = %s) AS `exists`"
        cursor.execute(sql, (protocol, host, port, path))
        result = cursor.fetchone()
        if result["exists"] == 1:
            # print("already in!", protocol, host, port, path)
            return

    # Check if in DB
    with connection.cursor() as cursor:
        sql = "SELECT EXISTS(SELECT * FROM `resources` WHERE `protocol` = %s AND `host` = %s AND `port` = %s AND `path` = %s) AS `exists`"
        cursor.execute(sql, (protocol, host, port, path))
        result = cursor.fetchone()
        if result["exists"] == 1:
            # print("already in!", protocol, host, port, path)
            return

    # Check if in misses, don't bother trying again...
    with connection.cursor() as cursor:
        sql = "SELECT EXISTS(SELECT * FROM `misses` WHERE `protocol` = %s AND `host` = %s AND `port` = %s AND `path` = %s) AS `exists`"
        cursor.execute(sql, (protocol, host, port, path))
        result = cursor.fetchone()
        if result["exists"] == 1:
            # print("already in!", protocol, host, port, path)
            return

    with connection.cursor() as cursor:
        sql = "INSERT INTO `queue` VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(
            sql,
            (
                protocol,
                host,
                port,
                path,
                datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            ),
        )
        connection.commit()


# insert initial queue item
enqueue("https", "en.wikipedia.org", 443, "/wiki/List_of_JavaScript_libraries")


def dequeue():
    with connection.cursor() as cursor:
        sql = "SELECT * FROM `queue` AS q WHERE `protocol` <> 'http' AND NOT EXISTS(SELECT * FROM `resources` AS r WHERE q.`protocol` = r.`protocol` AND q.`host` = r.`host` AND q.`port` = r.`port`) AND NOT EXISTS(SELECT * FROM `misses` AS m WHERE q.`protocol` = m.`protocol` AND q.`host` = m.`host` AND q.`port` = m.`port` AND q.`path` = m.`path`) ORDER BY `queued` LIMIT 1"
        cursor.execute(sql)
        result = cursor.fetchone()
        if not result:
            return
        link = (result["protocol"], result["host"], result["port"], result["path"])
        sql = "DELETE FROM `queue` WHERE `protocol` = %s AND `host` = %s AND `port` = %s AND `path` = %s"
        cursor.execute(sql, link)
        connection.commit()
        return link


def record_miss(protocol, host, port, path, reason="Unknown"):
    # Retrieve the miss record if it already exists
    print("Missed, Reason:", reason)
    with connection.cursor() as cursor:
        sql = "SELECT * FROM `misses` WHERE `protocol` = %s AND `host` = %s AND `port` AND %s AND `path` = %s"
        cursor.execute(sql, (protocol, host, port, path))
        result = cursor.fetchone()
        if result:
            print("missed AGAIN")
            sql = "INSERT INTO `misses` SET `reason` = %s, `last_miss` = %s WHERE `protocol` = %s AND `host` = %s AND `port` AND %s AND `path` = %s"
            cursor.execute(
                sql,
                (
                    reason,
                    datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    protocol,
                    host,
                    port,
                    path,
                ),
            )
            connection.commit()
        else:
            print("MISSED")
            sql = "INSERT INTO `misses` (`protocol`, `host`, `port`, `path`, `reason`, `last_miss`) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(
                sql,
                (
                    protocol,
                    host,
                    port,
                    path,
                    reason,
                    datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                ),
            )
            connection.commit()
        connection.commit()


try:
    while True:
        link = dequeue()
        if not link:
            print("nothing more")
            exit(0)
        print("Retrieving", link)
        try:
            result = retrieve(*link)
        except urllib.error.HTTPError as err:
            print("some http error :-(", err)
            record_miss(*link, f"{err.code} {err.reason}")
            continue
        except urllib.error.URLError as err:
            print("some url error :-(", err)
            record_miss(*link, str(err.reason))
            continue
        except ConnectionResetError:
            print("connection reset :-(")
            record_miss(*link)
            continue
        except OSError as err:
            print("oserror", err)
            record_miss(*link, str(err))
            continue

        # Insert result into DB
        with connection.cursor() as cursor:
            sql = "INSERT INTO `resources` VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, result[:8])
            connection.commit()

        for link in result[8]:
            enqueue(*link)

        # wait
        time.sleep(0.1)
finally:
    connection.close()
