from typing import TypedDict


class MembershipResponse(TypedDict):
    all_nodes: list[str]
    cluster_nodes: list[str]
