"use client";

import {
  ArrowUp,
  BookOpenText,
  Bot,
  Check,
  ChevronRight,
  Clock3,
  FileSearch,
  FileText,
  LoaderCircle,
  MessageSquareText,
  PanelRightClose,
  PanelRightOpen,
  Plus,
  RefreshCw,
  Search,
  Upload,
  X,
} from "lucide-react";
import { FormEvent, useEffect, useRef, useState } from "react";

type Source = {
  number: number;
  chunk_id: string;
  document_id: string;
  score: number;
  content: string;
  source_name: string;
  page_number: number | null;
};

type Message = {
  id: string;
  role: "user" | "assistant";
  content: string;
  sources?: Source[];
  requestId?: string;
};

type Job = {
  id: string;
  document_id: string;
  status: "queued" | "processing" | "retrying" | "completed" | "failed";
  attempts: number;
  indexed_chunks: number;
  error_message: string;
  created_at: string;
};

type IngestedDocument = {
  id: string;
  title: string;
  source_name: string;
  source_type: string;
  status: string;
  chunk_count: number;
  duplicate: boolean;
};

const starters = [
  "Como funciona o fluxo completo deste RAG?",
  "Quais tecnologias compõem a arquitetura?",
  "Como a qualidade das respostas é avaliada?",
];

const jobLabels: Record<Job["status"], string> = {
  queued: "Na fila",
  processing: "Indexando",
  retrying: "Nova tentativa",
  completed: "Concluído",
  failed: "Falhou",
};

const documentStatusLabels: Record<string, string> = {
  pending: "Pendente",
  processing: "Processando",
  chunked: "Pronto para indexar",
  indexed: "Indexado",
  failed: "Falhou",
};

function makeId() {
  return crypto.randomUUID();
}

async function readError(response: Response) {
  try {
    const body = await response.json();
    return body.detail ?? "A operação não pôde ser concluída.";
  } catch {
    return "A operação não pôde ser concluída.";
  }
}

export default function Dashboard() {
  const [view, setView] = useState<"chat" | "documents">("chat");
  const [messages, setMessages] = useState<Message[]>([]);
  const [question, setQuestion] = useState("");
  const [isAnswering, setIsAnswering] = useState(false);
  const [selectedSources, setSelectedSources] = useState<Source[]>([]);
  const [sourcesOpen, setSourcesOpen] = useState(false);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [documents, setDocuments] = useState<IngestedDocument[]>([]);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadError, setUploadError] = useState("");
  const [isLoadingDocuments, setIsLoadingDocuments] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!window.matchMedia("(max-width: 900px)").matches) {
      setSourcesOpen(true);
    }
  }, []);

  useEffect(() => {
    const activeJobs = jobs.filter((job) =>
      ["queued", "processing", "retrying"].includes(job.status),
    );
    if (!activeJobs.length) return;

    const timer = window.setInterval(async () => {
      const refreshed = await Promise.all(
        activeJobs.map(async (job) => {
          const response = await fetch(`/backend-api/rag/jobs/${job.id}`);
          return response.ok ? ((await response.json()) as Job) : job;
        }),
      );
      setJobs((current) =>
        current.map(
          (job) => refreshed.find((candidate) => candidate.id === job.id) ?? job,
        ),
      );
    }, 2500);
    return () => window.clearInterval(timer);
  }, [jobs]);

  async function loadDocuments() {
    setIsLoadingDocuments(true);
    try {
      const response = await fetch("/backend-api/documents");
      if (!response.ok) throw new Error(await readError(response));
      setDocuments((await response.json()) as IngestedDocument[]);
    } catch (error) {
      setUploadError(
        error instanceof Error ? error.message : "Falha ao carregar documentos.",
      );
    } finally {
      setIsLoadingDocuments(false);
    }
  }

  useEffect(() => {
    void loadDocuments();
  }, []);

  async function ask(questionText: string) {
    const trimmed = questionText.trim();
    if (!trimmed || isAnswering) return;

    setQuestion("");
    setIsAnswering(true);
    setMessages((current) => [
      ...current,
      { id: makeId(), role: "user", content: trimmed },
    ]);

    try {
      const response = await fetch("/backend-api/rag/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: trimmed, top_k: 5 }),
      });
      if (!response.ok) throw new Error(await readError(response));
      const result = await response.json();
      const assistantMessage: Message = {
        id: makeId(),
        role: "assistant",
        content: result.answer,
        sources: result.sources,
        requestId: result.request_id,
      };
      setMessages((current) => [...current, assistantMessage]);
      setSelectedSources(result.sources);
    } catch (error) {
      setMessages((current) => [
        ...current,
        {
          id: makeId(),
          role: "assistant",
          content:
            error instanceof Error
              ? error.message
              : "Não foi possível consultar o RAG.",
        },
      ]);
    } finally {
      setIsAnswering(false);
    }
  }

  async function uploadDocument(file: File) {
    setUploadError("");
    setIsUploading(true);
    const form = new FormData();
    form.append("file", file);
    try {
      const ingestResponse = await fetch("/backend-api/documents/ingest", {
        method: "POST",
        body: form,
      });
      if (!ingestResponse.ok) throw new Error(await readError(ingestResponse));
      const document = (await ingestResponse.json()) as IngestedDocument;
      setDocuments((current) => [
        document,
        ...current.filter((item) => item.id !== document.id),
      ]);

      const indexResponse = await fetch(
        `/backend-api/rag/documents/${document.id}/index-async`,
        { method: "POST" },
      );
      if (!indexResponse.ok) throw new Error(await readError(indexResponse));
      const job = (await indexResponse.json()) as Job;
      setJobs((current) => [job, ...current]);
    } catch (error) {
      setUploadError(
        error instanceof Error ? error.message : "Falha ao enviar documento.",
      );
    } finally {
      setIsUploading(false);
      if (fileInput.current) fileInput.current.value = "";
    }
  }

  function submitQuestion(event: FormEvent) {
    event.preventDefault();
    void ask(question);
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark"><BookOpenText size={19} /></div>
          <div><strong>RAG Workspace</strong><span>Conhecimento fundamentado</span></div>
        </div>

        <nav className="nav-list" aria-label="Navegação principal">
          <button aria-label="Consulta" className={view === "chat" ? "active" : ""} onClick={() => setView("chat")}>
            <MessageSquareText size={18} /><span>Consulta</span>
          </button>
          <button aria-label="Documentos" className={view === "documents" ? "active" : ""} onClick={() => setView("documents")}>
            <FileText size={18} /><span>Documentos</span>
            {jobs.some((job) => job.status === "processing") && <i />}
          </button>
        </nav>

        <div className="system-status">
          <span><i /> Sistema operacional</span>
          <small>Django · Qdrant · Maritaca</small>
        </div>
      </aside>

      {view === "chat" ? (
        <section className="workspace">
          <header className="topbar">
            <div><h1>Consulta ao conhecimento</h1><p>Respostas geradas a partir dos documentos indexados</p></div>
            <button className="icon-button" title={sourcesOpen ? "Ocultar fontes" : "Mostrar fontes"} onClick={() => setSourcesOpen((open) => !open)}>
              {sourcesOpen ? <PanelRightClose size={19} /> : <PanelRightOpen size={19} />}
            </button>
          </header>

          <div className={`chat-layout ${sourcesOpen ? "" : "sources-hidden"}`}>
            <div className="conversation">
              <div className="messages">
                {!messages.length && (
                  <div className="empty-chat">
                    <div className="empty-icon"><Bot size={28} /></div>
                    <h2>O que você quer investigar?</h2>
                    <p>Consulte a base e confira os trechos usados em cada resposta.</p>
                    <div className="starter-list">
                      {starters.map((starter) => (
                        <button key={starter} onClick={() => void ask(starter)}>
                          <span>{starter}</span><ChevronRight size={17} />
                        </button>
                      ))}
                    </div>
                  </div>
                )}

                {messages.map((message) => (
                  <article key={message.id} className={`message ${message.role}`}>
                    <div className="message-avatar">{message.role === "assistant" ? <Bot size={17} /> : "V"}</div>
                    <div className="message-body">
                      <div className="message-meta">{message.role === "assistant" ? "Assistente RAG" : "Você"}</div>
                      <p>{message.content}</p>
                      {!!message.sources?.length && (
                        <button className="source-link" onClick={() => { setSelectedSources(message.sources ?? []); setSourcesOpen(true); }}>
                          <FileSearch size={15} /> {message.sources.length} fontes utilizadas
                        </button>
                      )}
                    </div>
                  </article>
                ))}

                {isAnswering && (
                  <article className="message assistant">
                    <div className="message-avatar"><Bot size={17} /></div>
                    <div className="message-body"><div className="message-meta">Assistente RAG</div><div className="thinking"><i /><i /><i /></div></div>
                  </article>
                )}
              </div>

              <form className="composer" onSubmit={submitQuestion}>
                <input aria-label="Pergunta" value={question} onChange={(event) => setQuestion(event.target.value)} placeholder="Pergunte sobre os documentos..." />
                <button type="submit" title="Enviar pergunta" disabled={!question.trim() || isAnswering}><ArrowUp size={19} /></button>
              </form>
            </div>

            {sourcesOpen && (
              <aside className="sources-panel">
                <div className="panel-heading"><div><span>Contexto recuperado</span><strong>{selectedSources.length} fontes</strong></div><Search size={18} /></div>
                {!selectedSources.length ? (
                  <div className="panel-empty"><FileSearch size={24} /><p>As fontes da resposta aparecerão aqui.</p></div>
                ) : (
                  <div className="source-list">
                    {selectedSources.map((source) => (
                      <article key={source.chunk_id} className="source-item">
                        <div className="source-top"><span>Fonte {source.number}</span><strong>{Math.round(source.score * 100)}%</strong></div>
                        <h3>{source.source_name}</h3>
                        <p>{source.content}</p>
                        <small>{source.page_number ? `Página ${source.page_number}` : "Documento textual"}</small>
                      </article>
                    ))}
                  </div>
                )}
              </aside>
            )}
          </div>
        </section>
      ) : (
        <section className="workspace">
          <header className="topbar">
            <div><h1>Documentos</h1><p>Envie conteúdo e acompanhe a indexação vetorial</p></div>
            <button className="primary-action" onClick={() => fileInput.current?.click()} disabled={isUploading}>
              {isUploading ? <LoaderCircle className="spin" size={17} /> : <Plus size={17} />}
              Adicionar documento
            </button>
            <input ref={fileInput} hidden type="file" accept=".txt,.md,.pdf" onChange={(event) => event.target.files?.[0] && void uploadDocument(event.target.files[0])} />
          </header>

          <div className="documents-page">
            <section className="upload-zone" onClick={() => fileInput.current?.click()}>
              <Upload size={24} />
              <div><strong>Enviar documento para a base</strong><span>TXT, Markdown ou PDF · até 10 MB</span></div>
              <ChevronRight size={18} />
            </section>
            {uploadError && <div className="error-banner"><X size={17} />{uploadError}</div>}

            <div className="section-title"><div><h2>Base de conhecimento</h2><p>Documentos persistidos e atividade de indexação</p></div><button className="icon-button" title="Atualizar" onClick={() => void loadDocuments()} disabled={isLoadingDocuments}><RefreshCw className={isLoadingDocuments ? "spin" : ""} size={17} /></button></div>

            {!documents.length ? (
              <div className="document-empty"><FileText size={26} /><strong>Nenhum documento disponível</strong><span>Envie um arquivo para iniciar a base de conhecimento.</span></div>
            ) : (
              <div className="document-table">
                <div className="table-head"><span>Documento</span><span>Chunks</span><span>Indexação</span><span>Detalhe</span></div>
                {documents.map((document) => {
                  const job = jobs.find((item) => item.document_id === document.id);
                  const displayStatus = job?.status ?? document.status;
                  const badgeStatus =
                    displayStatus === "indexed" ? "completed" : displayStatus;
                  return (
                    <article key={document.id} className="document-row">
                      <div className="document-name"><div><FileText size={18} /></div><span><strong>{document.title}</strong><small>{document.source_name}</small></span></div>
                      <span>{document.chunk_count}</span>
                      <span className={`status-badge ${badgeStatus}`}>
                        {displayStatus === "completed" || displayStatus === "indexed" ? <Check size={14} /> : displayStatus === "failed" ? <X size={14} /> : displayStatus === "processing" ? <LoaderCircle className="spin" size={14} /> : <Clock3 size={14} />}
                        {job ? jobLabels[job.status] : documentStatusLabels[document.status] ?? document.status}
                      </span>
                      <small>{job ? `${job.attempts} tentativa(s) · ${job.indexed_chunks} indexados` : document.status}</small>
                    </article>
                  );
                })}
              </div>
            )}
          </div>
        </section>
      )}
    </main>
  );
}
