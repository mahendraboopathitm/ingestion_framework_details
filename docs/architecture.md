# Architecture

```mermaid
flowchart TD
    A["Lakeflow jobs orchestrator<br/>parallel fan-out"] --> B["pipeline_orchestrator<br/>main entry point — 11_Orchestration"]
    B --> C["Connector factory<br/>JDBC · File · API · Streaming"]
    B --> D["Loader factory<br/>Full · Incremental · CDC · SCD1/2"]
    C --> E["Source readers"]
    D --> F["Transform engine<br/>column mapping + type casting"]
    E --> G["Metadata control tables<br/>ingestion_framework.config.* / .audit.*"]
    F --> G
    G --> H["Cross-cutting concerns"]

    subgraph H["Cross-cutting concerns"]
        direction LR
        H1["Audit<br/>execution log"]
        H2["Logging<br/>structured logs"]
        H3["Monitoring<br/>metrics & alerts"]
        H4["Validation<br/>data quality"]
        H5["Recovery<br/>failed-run re-run"]
        H6["Parallel executor<br/>retry logic"]
    end
```

**Flow:** A Lakeflow job triggers `pipeline_orchestrator`, the single entry point for every pipeline. It reads one row from the `pipeline_config` metadata table to decide *which* connector and loader to use — no per-table code. Data flows source → connector → reader → transform engine → target table, while audit, logging, monitoring, validation, and recovery wrap every run.
