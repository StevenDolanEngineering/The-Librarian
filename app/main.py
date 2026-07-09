import logging
from fastapi import FastAPI
import inngest
import inngest.fast_api
from inngest.experimental import ai
from dotenv import load_dotenv
import uuid
import os
import datetime
from data_loader import load_and_chunk_pdf, embed_texts
from vector_db import QdrantAutoAssistantStorage
from custom_types import RAGQueryResult, RAGSearchResult, RAGUpsertResult, RAGChunkandSrc

load_dotenv()

inngest_client = inngest.Inngest(
    app_id="Auto_Assistant",
    logger=logging.getLogger("uvicorn"),
    is_production=False,
    serializer=inngest.PydanticSerializer()
)

@inngest_client.create_function(
    fn_id="Inngest_PDF_RAG_Assistant",
    trigger=inngest.TriggerEvent(event="rag/inngest_pdf"),
    throttle=inngest.Throttle(
        count=2, period=datetime.timedelta(minutes=1)
    ),
    rate_limit=inngest.RateLimit(
        limit = 1,
        period = datetime.timedelta(hours=4),
        key="event.data.source_id",
    )
)

async def rag_inngest_pdf(ctx: inngest.Context):
    def _load(ctx: inngest.Context) -> RAGChunkandSrc:
        pdf_path = ctx.event.data["pdf_path"]
        source_id = ctx.event.data.get("source_id", pdf_path)
        chunks = load_and_chunk_pdf(pdf_path)
        return RAGChunkandSrc(chunks=chunks, source_id=source_id)


    def _upsert(chunks_and_src: RAGChunkandSrc) -> RAGUpsertResult:
        chunks = chunks_and_src.chunks
        source_id = chunks_and_src.source_id
        vecs = embed_texts(chunks)
        ids = [str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source_id}:{i}")) for i in range (len(chunks))]
        payloads = [{"source": source_id, "Text": chunks[i]} for i in range(len(chunks))]
        QdrantAutoAssistantStorage().upsert(ids, vecs, payloads)
        return RAGUpsertResult(ingest=len(chunks))

    chunks_and_src = await ctx.step.run("load-and-chunk", lambda: _load(ctx), output_type=RAGChunkandSrc)
    ingested = await ctx.step.run("embed-and-upsert", lambda: _upsert(chunks_and_src), output_type=RAGUpsertResult)
    return ingested.model_dump()


@inngest_client.create_function(
        fn_id="Inngest_PDF_RAG_Query",
        trigger=inngest.TriggerEvent(event="rag/inngest_pdf_query_ai"),
        concurrency=[
            inngest.Concurrency(
                limit = 10,
            )
        ]
)

async def rag_query_pdf_ai(ctx: inngest.Context):
    def _search(question: str, top_k, int = 5):
        query_vec = embed_texts([question])[0]
        store = QdrantAutoAssistantStorage()
        found = store.search(query_vec, top_k)
        return RAGSearchResult(contexts=found["contexts"], sources=found["sources"])
    
    question = ctx.event.data["question"]
    top_k = int(ctx.event.data.get("top_k", 5))

    found = await ctx.step.run("search", lambda: _search(question, top_k), output_type=RAGSearchResult)

    context_block = "\n\n".join(f"- {c}" for c in found.contexts)
    user_content = (
        f"Use the following retrieved contexts to answer the question.\n\n"
        f"Context:\n{context_block}\n\n"
        f"Question: {question}\n\n"
        f"Answer:"        
    )

    adapter = ai.openai.Adapter(
        auth_key=os.getenv("OPENAI_API_KEY"),
        model="gpt-4o-mini",
    )

    res = await ctx.step.ai.infer(
        "llm-response",
        adapter=adapter,
        body={
            "max_tokens": 1024,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": "You are an assistant for answering questions based on retrieved contexts."},
                {"role": "user", "content": user_content}
            ]
        }
    )

    answer = res["choices"][0]["message"]["content"].strip()
    return {"answer": answer, "sources": found.sources, "num_contexts": len(found.contexts)}

app=FastAPI()

inngest.fast_api.serve(app, inngest_client, [rag_inngest_pdf])