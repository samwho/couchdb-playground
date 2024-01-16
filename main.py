from time import sleep
import requests
from os import system
from common import nodes, num_dbs
from cluster import setup_cluster
from seed import create_seed_data, wait_for_replication, assert_all_dbs_have_one_doc
from dbs import get_total_dbs, get_missing_db, create_db, get_doc_count

total_dbs = get_total_dbs()
if total_dbs == 0:
    print("cluster is empty, setting up cluster")
    setup_cluster()
    create_seed_data()
    wait_for_replication()
elif total_dbs == num_dbs + 2:
    print("cluster is already setup, making sure all dbs have 1 document in them...")
    try:
        assert_all_dbs_have_one_doc()
    except Exception as e:
        print(f"cluster not valid: {e}")
        exit(1)
else:
    print("must run against a fresh cluster, found existing dbs, exiting")
    exit(1)

print("simulating a node failure by destroying couchdb3 and its data...")
system("docker compose down -v couchdb3")
system("docker compose up -d couchdb3")
system("docker compose restart couchdb1")

print()
print("couchdb3 is back up, it should have started replicating again")

print("finding a database that's missing on couchdb3...")
node, db = get_missing_db()

print(f"found missing database {db} on node {nodes[node]['private_address']}")

if node == -1:
    print(
        "no missing databases found, unable to replicate problem this time, try again"
    )
    exit(0)

print(f"creating new empty database {db} on node {nodes[node]['private_address']}...")

try:
    create_db(node, db)
except requests.RequestException as e:
    print(f"error while creating db (this is expected): {e.response.text}")
    pass

print("polling doc count from all nodes...")
print()

while True:
    counts = {}
    for node in range(len(nodes)):
        count = get_doc_count(node, db)
        counts[node] = count
        print(f"{nodes[node]['private_address']}/{db} doc count: {count}")
    print()
    if len(set(counts.values())) == 1:
        val = set(counts.values()).pop()
        if val == 0:
            print("all nodes reached zero doc count, this means the cluster lost data")
            exit(0)
        else:
            print("all nodes reached a doc count of 1, no data loss this time")
            exit(0)
    sleep(2)
