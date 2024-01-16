from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from common import cpost, cput, num_dbs, nodes
from dbs import get_doc_count


def create_seed_data():
    print("creating seed data...")
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = []
        for i in range(num_dbs):

            def do(i):
                cput(0, f"/db-{i}")
                cpost(0, f"/db-{i}", json={})

            futures.append(executor.submit(do, i))

        with tqdm(total=num_dbs) as pbar:
            for future in as_completed(futures):
                future.result()
                pbar.update(1)
    print("done")


def wait_for_replication():
    print("waiting for replication...")
    for node_index, node in enumerate(nodes):
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []

            def do(node_index, db):
                while True:
                    if get_doc_count(node_index, f"/db-{db}") == 1:
                        break

            with tqdm(total=num_dbs) as pbar:
                pbar.set_description(node["private_address"])
                for db in range(num_dbs):
                    futures.append(executor.submit(do, node_index, db))

                for future in as_completed(futures):
                    future.result()
                    pbar.update(1)

    print("done")


def assert_all_dbs_have_one_doc():
    for node_index, node in enumerate(nodes):
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []

            def do(node_index, db):
                count = get_doc_count(node_index, f"/db-{db}")
                if count != 1:
                    raise Exception(
                        f"node {nodes[node_index]['private_address']}/db-{db} has {count} docs"
                    )

            with tqdm(total=num_dbs) as pbar:
                pbar.set_description(node["private_address"])
                for db in range(num_dbs):
                    futures.append(executor.submit(do, node_index, db))

                for future in as_completed(futures):
                    future.result()
                    pbar.update(1)
