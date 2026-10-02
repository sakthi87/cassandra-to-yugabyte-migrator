Absolutely. Assuming **10 TB raw input data**, and you want the architecture to scale from your current 6-node MVP toward a more production-grade Druid deployment, I’d structure it like this:

 ### 1\) 6-node architecture — MVP / initial production

 | Node | Services | CPU/RAM | Disk |
| --- | --- | --- | --- |
| druid-master-1 | Coordinator, Overlord, ZooKeeper, Router, Broker | 16 / 64 GB | 200–300 GB SSD |
| druid-master-2 | Coordinator, Overlord, ZooKeeper, Router, Broker | 16 / 64 GB | 200–300 GB SSD |
| druid-master-3 | Coordinator, Overlord, ZooKeeper, Router, Broker | 16 / 64 GB | 200–300 GB SSD |
| druid-data-1 | Historical, MiddleManager | 16 / 64 GB | 1.5–2 TB Premium SSD/NVMe |
| druid-data-2 | Historical, MiddleManager | 16 / 64 GB | 1.5–2 TB Premium SSD/NVMe |
| druid-data-3 | Historical, MiddleManager | 16 / 64 GB | 1.5–2 TB Premium SSD/NVMe |

**Total:** 6 nodes / 96 vCPU / 384 GB RAM / \~4.5–6 TB local data disk.

 > For 10 TB raw data, the 6-node setup is better treated as an MVP/starting point rather than the final capacity design. Druid's actual disk requirement depends heavily on segment compression, retention, replication, rollup, and ingestion/query workload.

---

 ### 2\) Next version — 9-node architecture

 Here I would separate the **query/control plane** from the **data/ingestion plane**. This gives you more room to scale Brokers and Historical nodes independently.

 | Node | Services | CPU/RAM | Disk |
| --- | --- | --- | --- |
| druid-master-1 | Coordinator, Overlord, ZooKeeper | 8 / 32 GB | 200–300 GB SSD |
| druid-master-2 | Coordinator, Overlord, ZooKeeper | 8 / 32 GB | 200–300 GB SSD |
| druid-master-3 | Coordinator, Overlord, ZooKeeper | 8 / 32 GB | 200–300 GB SSD |
| druid-query-1 | Router, Broker | 16 / 64 GB | 300–500 GB SSD |
| druid-query-2 | Router, Broker | 16 / 64 GB | 300–500 GB SSD |
| druid-data-1 | Historical, MiddleManager | 16 / 64 GB | 2 TB Premium SSD/NVMe |
| druid-data-2 | Historical, MiddleManager | 16 / 64 GB | 2 TB Premium SSD/NVMe |
| druid-data-3 | Historical, MiddleManager | 16 / 64 GB | 2 TB Premium SSD/NVMe |
| druid-data-4 | Historical, MiddleManager | 16 / 64 GB | 2 TB Premium SSD/NVMe |

**Total:** 9 nodes / 120 vCPU / 480 GB RAM / \~8 TB data disk.

 This gives you a cleaner scaling path: add **query nodes** when query concurrency increases and **data nodes** when retained data/segment volume increases.

---

 ### 3) 15-node architecture — larger production deployment

 For a 10 TB raw-data environment where you want significantly more HA and room for growth, I'd split the roles further:

 | Node | Services | CPU/RAM | Disk |
| --- | --- | --- | --- |
| druid-master-1 | Coordinator, Overlord, ZooKeeper | 8 / 32 GB | 300 GB SSD |
| druid-master-2 | Coordinator, Overlord, ZooKeeper | 8 / 32 GB | 300 GB SSD |
| druid-master-3 | Coordinator, Overlord, ZooKeeper | 8 / 32 GB | 300 GB SSD |
| druid-query-1 | Router, Broker | 16 / 64 GB | 500 GB SSD |
| druid-query-2 | Router, Broker | 16 / 64 GB | 500 GB SSD |
| druid-query-3 | Router, Broker | 16 / 64 GB | 500 GB SSD |
| druid-data-1 | Historical | 16 / 64 GB | 2 TB Premium SSD/NVMe |
| druid-data-2 | Historical | 16 / 64 GB | 2 TB Premium SSD/NVMe |
| druid-data-3 | Historical | 16 / 64 GB | 2 TB Premium SSD/NVMe |
| druid-data-4 | Historical | 16 / 64 GB | 2 TB Premium SSD/NVMe |
| druid-data-5 | Historical | 16 / 64 GB | 2 TB Premium SSD/NVMe |
| druid-ingest-1 | MiddleManager / Worker | 16 / 64 GB | 1 TB Premium SSD/NVMe |
| druid-ingest-2 | MiddleManager / Worker | 16 / 64 GB | 1 TB Premium SSD/NVMe |
| druid-ingest-3 | MiddleManager / Worker | 16 / 64 GB | 1 TB Premium SSD/NVMe |
| druid-ingest-4 | MiddleManager / Worker | 16 / 64 GB | 1 TB Premium SSD/NVMe |

**Total:** 15 nodes / 168 vCPU / 672 GB RAM.

 ### Architecture progression

 |  | 6-node MVP | 9-node next version | 15-node production |
| --- | --- | --- | --- |
| Master/control | 3 | 3 | 3 |
| Query | 0\* | 2 | 3 |
| Historical | 3 | 4 | 5 |
| Ingestion | 0\* | 4 combined | 4 dedicated |
| **Total nodes** | **6** | **9** | **15** |
| Approx. CPU | 96 vCPU | 120 vCPU | 168 vCPU |
| Approx. RAM | 384 GB | 480 GB | 672 GB |
| Raw data assumption | 10 TB | 10 TB+ | 10 TB+ |
| Primary goal | MVP | HA + scale | Performance \+ independent scaling |

\*In the 6-node and 9-node designs, the services are intentionally colocated to reduce infrastructure overhead.

 One important point: **10 TB raw data does not automatically mean you need 15 Druid nodes.** The right number depends heavily on the **post-ingestion Druid segment size, replication factor, retention period, ingestion rate, query concurrency, and whether deep storage is used**. If you give me those 5 parameters, I can size the 6 → 9 → 15 node architectures much more precisely, including **expected usable TB, replication overhead, segment storage, and recommended JVM heap/off-heap memory per node**.
