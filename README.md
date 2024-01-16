# CouchDB data loss reproduction

We had a data loss incident on January 9th 2024, and this project is an automatic
reproduction of what happened.

Broadly, this is what it does:

1. A 3 node cluster is brought up using `docker-compose.yaml`
2. 2000 databases are created, each with a single document in.
3. One node is killed off and its volume destroyed, then brought back up fresh.
4. While this node is doing its initial replication, we create a database on it.
5. Then we wait to see if that database replicates to the other nodes with 0 docs in it.

The reproduction doesn't have a 100% success rate, in my own testing I got it to
work maybe 20% of the time.

## Running it yourself

In one terminal:

```
$ python -m pip install -r requirements.txt
$ docker compose up
```

In another terminal:

```
$ python ./main.py
```

The script will run through all of the steps and tell you at the end if data was
lost or not. If it wasn't, you can re-run the script until it happens. It
detects if the setup has already been done and skips it.
