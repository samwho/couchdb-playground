from common import cget, cpost, nodes, username, password


def setup_cluster():
    print("setting up cluster...")
    for node in nodes[1:]:
        print(f"joining {node['private_address']} to cluster...")
        cpost(
            0,
            "/_cluster_setup",
            {
                "action": "enable_cluster",
                "bind_address": "0.0.0.0",
                "username": username,
                "password": password,
                "node_count": str(len(nodes)),
                "remote_node": node["private_address"],
                "remote_current_user": username,
                "remote_current_password": password,
            },
        )

        cpost(
            0,
            "/_cluster_setup",
            {
                "action": "add_node",
                "host": node["private_address"],
                "port": 5984,
                "username": username,
                "password": password,
            },
        )

    print("finishing cluster...")

    cpost(
        0,
        "/_cluster_setup",
        {
            "action": "finish_cluster",
        },
    )

    cget(0, "/_cluster_setup")

    print("done")


if __name__ == "__main__":
    setup_cluster()
