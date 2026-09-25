Originally built for CS550 Advanced Operating Systems at Illinois Institute of Technology (Fall 2020). See [DESIGN.md](DESIGN.md) for the architecture, protocol, and known limitations.

# P2P File Storage Cluster with Leader Election

A peer-to-peer file sharing cluster in Python where every node both hosts and downloads files, coordinated by a self-elected leader. Built with only the Python standard library (sockets, threading, concurrent.futures).

**How it works**
- **Leader election:** Nodes discover each other, join the existing leader, or claim leadership and announce it if none exists. After failover, the new leader rebuilds the file index from peer re-registrations.
- **File index:** The leader tracks which nodes host each file, plus likely sources that are mid-download.
- **Parallel downloads:** Files are split into chunks, and each source serves a range concurrently over its own TCP connection.
- **Integrity checks:** Every chunk is verified with an MD5 hash and re-requested on mismatch.
- **Self-spreading files:** A node that finishes downloading registers as a new source, so popular files spread across the cluster.

**Quick start**
    python node.py --port 9001 --dir ./hosted_files

Put files to share in `hosted_files/<port>/`. Start more nodes on other ports (9000–9129) to form a cluster. Add `-t` for automated test mode with bandwidth logging.

