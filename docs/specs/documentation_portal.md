# Spec: Documentation Portal & DAG Visualization

> **NOTE:** Once this specification is fully implemented and the official documentation is updated, this file MUST be deleted.

## Objective
Create a comprehensive, structured Documentation Portal hosted on GitHub Pages. The portal will serve as the single source of truth for SynaFlow, providing step-by-step tutorials, deep dives into core concepts, and architectural references. It will feature modern documentation UI elements like Sync/Async code tabs and auto-generated DAG architecture diagrams.

## Architecture & Tooling
1. **Static Site Generator:** Use a modern generator like **MkDocs (with Material for MkDocs)** or Docusaurus. MkDocs-Material is highly recommended for Python projects as it natively supports Markdown, sidebar generation, code tabs (via SuperFences), and Mermaid diagrams.
2. **Directory Structure:** Source markdown files will be housed in a structured directory (e.g., `docs/user_docs/`).
3. **DAG Visualization Script:** Implement a dev utility script (e.g., `scripts/visualize_dag.py`) that reads a SynaFlow pipeline's exported JSON (`pipeline.to_dict()`) and generates a **Mermaid.js** or Graphviz diagram. These diagrams will visually represent data flows, consumers, and producers, and will be embedded into the documentation pages.

## Content Structure (Sidebar)
The documentation must be logically organized in the sidebar with the following sections:

### 1. Introduction & Getting Started
* What is SynaFlow and why it exists.
* **Lockstep Data Flow:** A visual and textual explanation of how a single data item flows entirely through the pipeline (from producer to materializer) before the second item is consumed, guaranteeing extreme memory efficiency.
* Links pointing developers to the living example pipelines in the test corpus.

### 2. Step-by-Step Tutorial
A progressive, hands-on guide that builds a pipeline from scratch:
* **Level 1:** Hello World pipeline.
* **Level 2:** Adding multiple processing steps and type-hint dependencies.
* **Level 3:** Attaching Observers to monitor lifecycle events.
* **Level 4:** Upgrading the pipeline with a Disk/Database Materializer.

### 3. Core Concepts
* **Semantic Naming Rules:** Explanation of plural/singular dataset rules and the "Smart Binding" engine.
* **DAG Construction:** How pipelines read type hints to wire Directed Acyclic Graphs automatically.
* **Code Tabs (Sync/Async Parity):** Every code snippet showing a user action MUST use UI tabs to display the **Synchronous** and **Asynchronous** versions of the code side-by-side.

### 4. Advanced Guides & Reference
* **Custom Materializers:** Guide on how to write custom Materializer Factories.
* **Custom Observers:** Guide on building tracking/telemetry observers.
* **Export Guidance:** The strict contract for downstream orchestrators (Airflow/Prefect) on how to interpret `mode` and `each_mode_deps` from the exported DAG JSON.

## Implementation Plan
1. **Setup:** Initialize `mkdocs.yml` (or equivalent) and configure the GitHub Actions workflow to auto-deploy to the `gh-pages` branch on merge to `main`.
2. **README Update:** Modify the project root `README.md` to add a highly visible link pointing to the new GitHub Pages URL at the very top.
3. **Visualizer:** Write the `json-to-mermaid` dev script and generate the first flow images for the examples.
4. **Drafting:** Write the markdown content matching the structure outlined above.
