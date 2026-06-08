"""
Ticket Auto-Assignment System
-----------------------------

This service automatically assigns incoming support tickets to available agents
based on configured department policies.

Core Features
-------------
1. Multi-tenant architecture (supports many clients).
2. Per-client thread management.
3. Per-department worker threads.
4. Supports assignment policies:
      • Round Robin
      • LTR (Least Tickets Remaining)
5. Automatic daily queue reset.
6. Database primary/secondary failover.
7. Thread-safe shared state management.
8. Parameterized SQL queries to prevent SQL injection.
9. Graceful thread shutdown using threading.Event().

Thread Hierarchy
----------------

Main Thread
    └── Client Manager Thread (per client)
            └── Department Manager Thread (per department)

Each department thread continuously polls for new tickets and assigns them
to available agents.

This architecture allows the system to scale across many clients and departments.
"""

import sys
import time
import threading
import logging
import logging.config
from configparser import ConfigParser
from dateutil.parser import parse

import pymysql as MySQLdb
import requests


# -------------------------------------------------------
# Command Line Arguments
# -------------------------------------------------------
# Script requires:
#   1. MySQL port
#   2. MySQL socket path
#
# Example:
# python autoTicket.py 3306 /var/run/mysqld/mysqld.sock
# -------------------------------------------------------

if len(sys.argv) < 3:
    print("Usage: autoTicket.py <dbport> <dbsocket>")
    sys.exit(1)

DB_PORT = int(sys.argv[1])
DB_SOCKET = sys.argv[2]


# -------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------
# Logging is configured via external file.
# This allows dynamic log format/levels without modifying code.
# -------------------------------------------------------

LOG_CONFIG = "/etc/logConfig/ticketLog.conf"
logging.config.fileConfig(LOG_CONFIG)

log_main = logging.getLogger("main")


# -------------------------------------------------------
# Load Configuration
# -------------------------------------------------------
# Reads system configuration such as:
#   • Database names
#   • API endpoint
#   • Valid ticket sources
#   • MySQL server credentials
# -------------------------------------------------------

cfg = ConfigParser()
cfg.read("/etc/logConfig/autoTicket.conf")

CENTRAL_DB = cfg.get("Mysql_Info", "centralDb")
CLIENT_DB_PREFIX = cfg.get("Mysql_Info", "clientDb")
API_URL = cfg.get("Mysql_Info", "api")

# Valid ticket sources
SOURCE_ARR = [
    s.strip().lower()
    for s in cfg.get("Mysql_Info", "source_arr").split(",")
]


# MySQL connection groups
MYSQL_PRIMARY_WRITE = dict(cfg.items("mysql_primary_write"))
MYSQL_PRIMARY_READ = dict(cfg.items("mysql_primary_read"))
MYSQL_SECONDARY_WRITE = dict(cfg.items("mysql_secodary_write"))
MYSQL_SECONDARY_READ = dict(cfg.items("mysql_secodary_read"))


# -------------------------------------------------------
# Shared Global State
# -------------------------------------------------------
# These structures are accessed by multiple threads.
# Therefore they must be protected with locks.
# -------------------------------------------------------

# Mapping:
# "clientid_deptid" -> [user_id, user_id]
dept_wise_user_info = {}

# Departments that currently have no active agents
# (used for fallback assignment logic)
all_dept_email_arr = []
all_dept_phone_arr = []

# Locks to ensure thread-safe access
dept_user_lock = threading.Lock()
dept_arr_lock = threading.Lock()
query_lock = threading.Lock()

# Thread kill signals for clients
client_kill_events = {}
client_kill_lock = threading.Lock()

# Thread kill signals for departments
dept_kill_events = {}
dept_kill_lock = threading.Lock()


# -------------------------------------------------------
# Database Connection Helper
# -------------------------------------------------------
# Establishes a connection to MySQL using either
# socket or TCP depending on configuration.
# -------------------------------------------------------

def connect_mysql(host, user, passwd, db):

    try:
        conn = MySQLdb.connect(
            host=host,
            user=user,
            password=passwd,
            database=db,
            port=DB_PORT,
            unix_socket=DB_SOCKET
        )

        log_main.info("Connected DB %s/%s", host, db)
        return conn

    except Exception:
        #print("Error")
        log_main.error("DB connection failed %s/%s", host, db, exc_info=True)
        return None


# -------------------------------------------------------
# Database Connection Bundle
# -------------------------------------------------------
# Each worker thread maintains:
#   • One WRITE connection
#   • One READ connection
#
# This separation improves performance and allows read replicas.
# -------------------------------------------------------

class DbBundle:

    def __init__(self, w_conn, r_conn):

        self.w_conn = w_conn
        self.r_conn = r_conn

        self.w_cur = w_conn.cursor() if w_conn else None
        self.r_cur = r_conn.cursor() if r_conn else None

    def ping(self):
        """
        Ensures DB connection is alive.
        Automatically reconnects if connection dropped.
        """

        for conn in (self.w_conn, self.r_conn):

            if not conn:
                continue

            try:
                conn.ping(reconnect=True)

            except Exception:
                log_main.error("DB reconnect failed", exc_info=True)


# -------------------------------------------------------
# DB Failover Handler
# -------------------------------------------------------
# Tries primary DB servers first.
# If unavailable, switches to secondary servers.
# -------------------------------------------------------

def open_db_bundle(db):

    w_conn = connect_mysql(
        MYSQL_PRIMARY_WRITE["mysqlhost"],
        MYSQL_PRIMARY_WRITE["mysqluser"],
        MYSQL_PRIMARY_WRITE["mysqlpasswd"],
        db
    )

    if not w_conn:
        w_conn = connect_mysql(
            MYSQL_SECONDARY_WRITE["mysqlhost"],
            MYSQL_SECONDARY_WRITE["mysqluser"],
            MYSQL_SECONDARY_WRITE["mysqlpasswd"],
            db
        )

    r_conn = connect_mysql(
        MYSQL_PRIMARY_READ["mysqlhost"],
        MYSQL_PRIMARY_READ["mysqluser"],
        MYSQL_PRIMARY_READ["mysqlpasswd"],
        db
    )

    if not r_conn:
        r_conn = connect_mysql(
            MYSQL_SECONDARY_READ["mysqlhost"],
            MYSQL_SECONDARY_READ["mysqluser"],
            MYSQL_SECONDARY_READ["mysqlpasswd"],
            db
        )

    return DbBundle(w_conn, r_conn)


# -------------------------------------------------------
# Query Executor
# -------------------------------------------------------
# Centralized query execution function.
#
# Benefits:
#   • prevents duplicated DB code
#   • enforces locking
#   • unified logging
# -------------------------------------------------------

def execute_query(flag, query, params, conn, cur):

    log = logging.getLogger("queryExecuter")

    try:

        with query_lock:

            cur.execute(query, params)

            if flag == "SELECT":
                rows = cur.fetchall()
                conn.commit()
                return rows

            conn.commit()
            return cur.rowcount

    except Exception:

        log.error("Query failed: %s", query, exc_info=True)
        return [] if flag == "SELECT" else 0


# -------------------------------------------------------
# Time Utilities
# -------------------------------------------------------

def unix_now():
    """Return current unix timestamp"""
    return int(time.time())


def start_of_today_unix():
    """Return unix timestamp for today's midnight"""

    date_str = time.strftime("%Y-%m-%d")
    return int(time.mktime(parse(date_str).timetuple()))


# -------------------------------------------------------
# Ticket Assignment Logic
# -------------------------------------------------------
# Responsible for:
#   • updating ticket_details
#   • updating user queue stats
#   • removing ticket from unprocessed queue
#   • notifying external API
# -------------------------------------------------------

def assign_ticket(user_id, dept_id, ticket_id, db, client_id):

    log = logging.getLogger("ticketAssign")

    now = unix_now()

    # Assign ticket to user
    execute_query(
        "UPDATE",
        """
        UPDATE ticket_details
        SET assigned_to_user_id=%s,
            assigned_to_dept_id=%s
        WHERE ticket_id=%s
        """,
        (user_id, dept_id, ticket_id),
        db.w_conn,
        db.w_cur
    )

    # Update agent queue counters
    execute_query(
        "UPDATE",
        """
        UPDATE UsersQue
        SET ticket_queue_count=ticket_queue_count+1,
            ticket_max_assign_count=ticket_max_assign_count+1,
            ticket_timestamp=%s
        WHERE user_id=%s AND dept_id=%s
        """,
        (now, user_id, dept_id),
        db.w_conn,
        db.w_cur
    )

    # Remove ticket from processing queue
    execute_query(
        "DELETE",
        "DELETE FROM unprocessed_tickets WHERE ticket_id=%s",
        (ticket_id,),
        db.w_conn,
        db.w_cur
    )

    # Notify external system
    try:

        url = (
            f"{API_URL}"
            f"client_id={client_id}"
            f"&assigned_to_dept_id={dept_id}"
            f"&assigned_to_user_id={user_id}"
            f"&ticket_id={ticket_id}"
        )

        requests.get(url, timeout=10)

    except Exception:
        log.error("API notify failed", exc_info=True)


# -------------------------------------------------------
# Department Manager Thread
# -------------------------------------------------------
# Each department has a dedicated worker thread.
#
# Responsibilities:
#   • fetch active agents
#   • fetch unprocessed tickets
#   • apply assignment policy
#   • assign tickets
# -------------------------------------------------------

def dept_manager(dept_id, dept_email, policy,
                 dept_phone, client_id, db_name):

    log = logging.getLogger("DeptMngr")

    key = f"{client_id}_{dept_id}"

    db = open_db_bundle(db_name)

    log.info("Dept manager started %s", key)

    while True:

        # Check stop signal
        with dept_kill_lock:
            evt = dept_kill_events.get(key)

        if evt and evt.is_set():
            log.info("Dept manager stopping %s", key)
            break

        db.ping()

        # Fetch agent queues
        users = execute_query(
            "SELECT",
            """
            SELECT user_id,ticket_queue_count,ticket_timestamp
            FROM UsersQue
            WHERE dept_id=%s
            """,
            (dept_id,),
            db.r_conn,
            db.r_cur
        )

        if not users:
            time.sleep(5)
            continue

        # Fetch unprocessed tickets
        tickets = execute_query(
            "SELECT",
            """
            SELECT ticket_id,source
            FROM unprocessed_tickets
            WHERE assigned_to_dept_id=%s
            ORDER BY id ASC
            LIMIT 10
            """,
            (dept_id,),
            db.r_conn,
            db.r_cur
        )

        if not tickets:
            time.sleep(5)
            continue

        # Assignment loop
        for ticket in tickets:

            if ticket[1].lower() not in SOURCE_ARR:
                continue

            # Choose agent according to policy
            if policy.lower() == "round robin":
                user = min(users, key=lambda x: x[2])
            else:
                user = min(users, key=lambda x: x[1])

            assign_ticket(user[0], dept_id, ticket[0], db, client_id)

            users.remove(user)

            if not users:
                break

        time.sleep(5)


# -------------------------------------------------------
# Client Manager Thread
# -------------------------------------------------------
# One thread per client.
#
# Responsibilities:
#   • monitor departments
#   • spawn department workers
#   • maintain active department list
# -------------------------------------------------------

def client_manager(client_id, db_name):

    log = logging.getLogger("clientMngr")

    db = open_db_bundle(db_name)

    alive_depts = set()

    log.info("Client manager started %s", client_id)

    while True:

        with client_kill_lock:
            evt = client_kill_events.get(client_id)

        if evt and evt.is_set():
            break

        db.ping()

        depts = execute_query(
            "SELECT",
            "SELECT dept_id,dept_email,policy,dept_phone FROM departments",
            (),
            db.r_conn,
            db.r_cur
        )

        for dept in depts:

            dept_id, email, policy, phone = dept

            key = f"{client_id}_{dept_id}"

            if key in alive_depts:
                continue

            with dept_kill_lock:
                dept_kill_events[key] = threading.Event()

            t = threading.Thread(
                target=dept_manager,
                name=key,
                args=(dept_id, email, policy, phone, client_id, db_name),
                daemon=True
            )

            t.start()

            alive_depts.add(key)

            log.info("Dept thread started %s", key)

        time.sleep(10)


# -------------------------------------------------------
# Main Service Loop
# -------------------------------------------------------
# Continuously monitors active clients.
#
# When a new client is activated:
#   • start client manager thread
#
# When a client is disabled:
#   • stop client thread
# -------------------------------------------------------

def main():

    log_main.info("Ticket Auto Assignment starting")

    cntrl_db = open_db_bundle(CENTRAL_DB)
    #print("Central_good")

    running_clients = set()

    while True:

        cntrl_db.ping()

        clients = execute_query(
            "SELECT",
            """
            SELECT registration_id
            FROM clientRegistrationBasic
            WHERE status=1
            AND verificationStatus='VERIFIED'
            AND auto_assign_flag=1
            """,
            (),
            cntrl_db.r_conn,
            cntrl_db.r_cur
        )

        active = set()

        for row in clients:

            cid = str(row[0])
            dbname = f"{CLIENT_DB_PREFIX}_{cid}"

            active.add(cid)

            if cid in running_clients:
                continue

            with client_kill_lock:
                client_kill_events[cid] = threading.Event()

            t = threading.Thread(
                target=client_manager,
                name=f"Client_{cid}",
                args=(cid, dbname),
                daemon=True
            )

            t.start()

            running_clients.add(cid)

            log_main.info("Client thread started %s", cid)

        # Stop inactive clients
        stale = running_clients - active

        for cid in stale:

            with client_kill_lock:
                evt = client_kill_events.get(cid)

                if evt:
                    evt.set()

            running_clients.remove(cid)

        time.sleep(10)


if __name__ == "__main__":
    main()
