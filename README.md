# The-Librarian

A production-grade, asynchronous Retrieval-Augmented Generation (RAG) platform that separates heavy document processing and vector ingestion pipelines from the user-facing application thread. Built with an event-driven framework, this project handles long-running document ingestion workflows reliably and at scale.

## 🏗️ System Architecture & Workflow

Unlike standard, synchronous RAG tutorials that block the application during document chunking and embedding, this platform utilizes a distributed, event-driven pattern:

1. **Decoupled Event Injection:** The frontend (**Streamlit**) uploads documents and immediately dispatches a fire-and-forget payload (`rag/inngest_pdf`) to the background event layer.
2. **Durable Execution & State Management:** An event coordinator (**Inngest**) intercepts the event and runs the workflow as isolated, trackable, and retriable steps inside a background worker (**FastAPI**).
3. **Chunking & Vectorization:** Documents are parsed into discrete node fragments (**LlamaIndex**) and mapped to highly multi-dimensional semantic vectors (**OpenAI `text-embedding-3-large`**).
4. **Isolated Memory Storage:** Vector structures and payload text strings are persisted inside a standalone vector indexing database (**Qdrant**).
5. **Asynchronous Query Resolution:** Query events look up local semantic context indices via cosine similarity calculations before passing data boundaries cleanly to open-weights large language models (**GPT-4o-mini**).

---

## 🛠️ Technology Stack

- **Framework & API Gateway:** FastAPI
- **Durable Event Orchestration:** Inngest
- **Vector Search Engine:** Qdrant Client
- **Orchestration & Data Parsing:** LlamaIndex (Core & File Readers)
- **Inference & Embedding Foundation:** OpenAI API (GPT-4o-mini / text-embedding-3-large)
- **User Dashboard:** Streamlit
- **Data Validation & Enforcement:** Pydantic (v2)

---

## 🚀 Key Engineering Features

### 1. Resilient & Fault-Tolerant Step Functions
By wrapping discrete tasks inside `ctx.step.run()`, the platform guarantees that if a database timeout occurs during vector upserts, the system will not waste computing costs re-parsing or re-chunking the original PDF. It retries precisely at the failure boundary.

### 2. High-Performance Token Management
Utilizes forced throttling (`count=2, period=1m`) and strict user/source-level rate-limiting (`limit=1, period=4h`) built directly into the background worker decorators. This effectively shields the backend system from API toll abuse, script exhaustion, and sudden payload floods.

### 3. Asynchronous Non-Blocking Polling
The Streamlit front-end decouples data generation from display logic. It captures a unique transactional execution ID upon file dispatch and invokes a structured interval network poll (`wait_for_run_output`) to continuously stream asynchronous pipeline updates cleanly back to the screen interface.

---

## 💻 Local Setup & Execution

### 1. Environmental Configuration
Create a `.env` file in the root directory of the application:
```env
OPENAI_API_KEY=your_openai_api_key_here
INNGEST_API_BASE=http://127.0.0
```

### 2. Launching Core Infrastructure Dependency
Ensure you have a local Qdrant database instance active:
```bash
docker run -p 6333:6333 qdrant/qdrant
```

### 3. Activating the Event Orchestrator
Launch the local Inngest development server bus:
```bash
npx inngest-cli@latest dev
```

### 4. Spanning Up Backend and Frontend Services
Execute the FastAPI event listener:
```bash
uvicorn app.main:app --reload --port 8000
```

In a separate terminal tab, activate the interactive user dashboard:
```bash
streamlit run frontend/streamlit_app.py
```
