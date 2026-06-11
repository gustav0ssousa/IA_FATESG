"use client";

import {
  ArrowUp,
  Activity,
  AlertTriangle,
  BarChart3,
  BookOpenText,
  Bot,
  Check,
  ChevronLeft,
  ChevronRight,
  Clock3,
  Filter,
  FileSearch,
  FileText,
  LoaderCircle,
  LogOut,
  MessageSquareText,
  PanelRightClose,
  PanelRightOpen,
  Pencil,
  Plus,
  RefreshCw,
  RotateCw,
  Search,
  ShieldCheck,
  Tags,
  Upload,
  UserRound,
  Wrench,
  X,
} from "lucide-react";
import { FormEvent, useCallback, useEffect, useRef, useState } from "react";

type Source = {
  number: number;
  chunk_id: string;
  document_id: string;
  score: number;
  content: string;
  source_name: string;
  page_number: number | null;
  metadata?: {
    section_heading?: string;
    content_type?: string;
    error_codes?: string[];
    safety_level?: string;
    manufacturer?: string;
    models?: string[];
    manual_type?: string;
  };
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
  metadata?: {
    manufacturer?: string;
    models?: string[];
    equipment_type?: string;
    manual_type?: string;
  };
};

type DocumentOperation = {
  document: IngestedDocument;
  job: Job | null;
  duplicate?: boolean;
};

type DocumentListResponse = {
  results: IngestedDocument[];
  pagination: {
    page: number;
    page_size: number;
    total: number;
    total_pages: number;
  };
  facets: {
    manufacturers: string[];
    models: string[];
  };
};

type MetadataDraft = {
  title: string;
  manufacturer: string;
  models: string;
  equipment_type: string;
  manual_type: string;
};

type KPIOverview = {
  queries: {
    total: number;
    last_24h: number;
    successful: number;
    failed: number;
    error_rate: number;
    average_response_ms: number;
    p95_response_ms: number;
    average_sources: number;
  };
  documents: {
    total: number;
    indexed: number;
    top_retrieved: {
      document_id: string;
      source_name: string;
      retrieval_count: number;
      query_count: number;
      average_score: number;
    }[];
  };
  indexing_jobs: Record<"queued" | "processing" | "retrying" | "completed" | "failed", number>;
  timeline: { date: string; total: number; errors: number }[];
  recent_queries: {
    request_id: string;
    question: string;
    status: "success" | "error";
    model: string;
    source_count: number;
    duration_ms: number;
    created_at: string;
  }[];
};

type AuthUser = {
  id: number;
  username: string;
  is_staff: boolean;
};

const authTokenKey = "adaptive-rag-auth-token";

const starters = [
  "O que verificar antes de iniciar um troubleshooting?",
  "Quais cuidados de segurança precedem a manutenção?",
  "Como diagnosticar problemas de alimentação de papel?",
];

const contentTypeLabels: Record<string, string> = {
  safety: "Segurança",
  troubleshooting: "Troubleshooting",
  error_reference: "Códigos e erros",
  procedure: "Procedimentos",
  specification: "Especificações",
  maintenance: "Manutenção",
  technical_reference: "Referência técnica",
};

const manualTypeLabels: Record<string, string> = {
  service_manual: "Manual de serviço",
  user_manual: "Manual do usuário",
  installation_manual: "Instalação",
  parts_catalog: "Catálogo de peças",
  technical_document: "Documento técnico",
};

const jobLabels: Record<Job["status"], string> = {
  queued: "Na fila",
  processing: "Processando",
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
    const detail = body.detail ?? "A operação não pôde ser concluída.";
    return body.request_id ? `${detail} Referência: ${body.request_id}` : detail;
  } catch {
    return "A operação não pôde ser concluída.";
  }
}

async function apiFetch(input: string, init: RequestInit = {}) {
  const headers = new Headers(init.headers);
  const token = window.localStorage.getItem(authTokenKey);
  if (token) headers.set("Authorization", `Token ${token}`);
  return fetch(input, { ...init, headers });
}

function formatDuration(milliseconds: number) {
  return milliseconds >= 1000
    ? `${(milliseconds / 1000).toFixed(1)} s`
    : `${milliseconds} ms`;
}

export default function Dashboard() {
  const [view, setView] = useState<"chat" | "documents" | "analytics">("chat");
  const [messages, setMessages] = useState<Message[]>([]);
  const [question, setQuestion] = useState("");
  const [isAnswering, setIsAnswering] = useState(false);
  const [selectedSources, setSelectedSources] = useState<Source[]>([]);
  const [sourcesOpen, setSourcesOpen] = useState(false);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [documents, setDocuments] = useState<IngestedDocument[]>([]);
  const [documentPage, setDocumentPage] = useState(1);
  const [documentPagination, setDocumentPagination] = useState({ page: 1, page_size: 25, total: 0, total_pages: 1 });
  const [documentFacets, setDocumentFacets] = useState({ manufacturers: [] as string[], models: [] as string[] });
  const [isUploading, setIsUploading] = useState(false);
  const [uploadError, setUploadError] = useState("");
  const [isLoadingDocuments, setIsLoadingDocuments] = useState(false);
  const [kpis, setKpis] = useState<KPIOverview | null>(null);
  const [isLoadingKpis, setIsLoadingKpis] = useState(false);
  const [kpiError, setKpiError] = useState("");
  const [authRequired, setAuthRequired] = useState(false);
  const [authReady, setAuthReady] = useState(false);
  const [user, setUser] = useState<AuthUser | null>(null);
  const [loginUsername, setLoginUsername] = useState("");
  const [loginPassword, setLoginPassword] = useState("");
  const [loginError, setLoginError] = useState("");
  const [isLoggingIn, setIsLoggingIn] = useState(false);
  const [manufacturerFilter, setManufacturerFilter] = useState("");
  const [modelFilter, setModelFilter] = useState("");
  const [contentTypeFilter, setContentTypeFilter] = useState("");
  const [editingDocument, setEditingDocument] = useState<IngestedDocument | null>(null);
  const [metadataDraft, setMetadataDraft] = useState<MetadataDraft | null>(null);
  const [isSavingMetadata, setIsSavingMetadata] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);
  const canManage = !authRequired || user?.is_staff;
  const manufacturers = documentFacets.manufacturers;
  const models = documentFacets.models;
  const activeFilterCount = [manufacturerFilter, modelFilter, contentTypeFilter].filter(Boolean).length;

  useEffect(() => {
    if (!window.matchMedia("(max-width: 900px)").matches) {
      setSourcesOpen(true);
    }
  }, []);

  useEffect(() => {
    async function initializeAuth() {
      try {
        const configResponse = await fetch("/backend-api/auth/config");
        const config = configResponse.ok
          ? ((await configResponse.json()) as { required: boolean })
          : { required: false };
        setAuthRequired(config.required);

        if (window.localStorage.getItem(authTokenKey)) {
          const meResponse = await apiFetch("/backend-api/auth/me");
          if (meResponse.ok) setUser((await meResponse.json()) as AuthUser);
          else window.localStorage.removeItem(authTokenKey);
        }
      } finally {
        setAuthReady(true);
      }
    }
    void initializeAuth();
  }, []);

  const loadDocuments = useCallback(async () => {
    setIsLoadingDocuments(true);
    try {
      const response = await apiFetch(`/backend-api/documents?page=${documentPage}&page_size=25`);
      if (!response.ok) throw new Error(await readError(response));
      const body = (await response.json()) as DocumentListResponse;
      setDocuments(body.results);
      setDocumentPagination(body.pagination);
      setDocumentFacets(body.facets);
    } catch (error) {
      setUploadError(
        error instanceof Error ? error.message : "Falha ao carregar documentos.",
      );
    } finally {
      setIsLoadingDocuments(false);
    }
  }, [documentPage]);

  useEffect(() => {
    const activeJobs = jobs.filter((job) =>
      ["queued", "processing", "retrying"].includes(job.status),
    );
    if (!activeJobs.length) return;

    const timer = window.setInterval(async () => {
      const refreshed = await Promise.all(
        activeJobs.map(async (job) => {
          const response = await apiFetch(`/backend-api/rag/jobs/${job.id}`);
          return response.ok ? ((await response.json()) as Job) : job;
        }),
      );
      setJobs((current) =>
        current.map(
          (job) => refreshed.find((candidate) => candidate.id === job.id) ?? job,
        ),
      );
      if (refreshed.some((job) => ["completed", "failed"].includes(job.status))) {
        void loadDocuments();
      }
    }, 2500);
    return () => window.clearInterval(timer);
  }, [jobs, loadDocuments]);

  useEffect(() => {
    if (authReady && (!authRequired || user)) void loadDocuments();
  }, [authReady, authRequired, user, loadDocuments]);

  async function loadKpis() {
    setIsLoadingKpis(true);
    setKpiError("");
    try {
      const response = await apiFetch("/backend-api/rag/kpis/overview");
      if (!response.ok) throw new Error(await readError(response));
      setKpis((await response.json()) as KPIOverview);
    } catch (error) {
      setKpiError(error instanceof Error ? error.message : "Falha ao carregar indicadores.");
    } finally {
      setIsLoadingKpis(false);
    }
  }

  useEffect(() => {
    if (view === "analytics") void loadKpis();
  }, [view]);

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
      const response = await apiFetch("/backend-api/rag/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question: trimmed,
          top_k: 5,
          ...(manufacturerFilter && { manufacturer: manufacturerFilter }),
          ...(modelFilter && { model: modelFilter }),
          ...(contentTypeFilter && { content_type: contentTypeFilter }),
        }),
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
      const ingestResponse = await apiFetch("/backend-api/documents/ingest", {
        method: "POST",
        body: form,
      });
      if (!ingestResponse.ok) throw new Error(await readError(ingestResponse));
      const operation = (await ingestResponse.json()) as DocumentOperation;
      const document = operation.document;
      setDocuments((current) => [
        document,
        ...current.filter((item) => item.id !== document.id),
      ]);
      if (operation.job) setJobs((current) => [operation.job!, ...current]);
    } catch (error) {
      setUploadError(
        error instanceof Error ? error.message : "Falha ao enviar documento.",
      );
    } finally {
      setIsUploading(false);
      if (fileInput.current) fileInput.current.value = "";
    }
  }

  function openMetadataEditor(document: IngestedDocument) {
    setEditingDocument(document);
    setMetadataDraft({
      title: document.title,
      manufacturer: document.metadata?.manufacturer ?? "",
      models: document.metadata?.models?.join(", ") ?? "",
      equipment_type: document.metadata?.equipment_type ?? "other",
      manual_type: document.metadata?.manual_type ?? "technical_document",
    });
  }

  async function saveMetadata(event: FormEvent) {
    event.preventDefault();
    if (!editingDocument || !metadataDraft) return;
    setIsSavingMetadata(true);
    setUploadError("");
    try {
      const response = await apiFetch(`/backend-api/documents/${editingDocument.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...metadataDraft,
          models: metadataDraft.models.split(",").map((model) => model.trim()).filter(Boolean),
        }),
      });
      if (!response.ok) throw new Error(await readError(response));
      const operation = (await response.json()) as DocumentOperation;
      setDocuments((current) => current.map((item) => item.id === operation.document.id ? operation.document : item));
      if (operation.job) setJobs((current) => [operation.job!, ...current]);
      setEditingDocument(null);
      setMetadataDraft(null);
    } catch (error) {
      setUploadError(error instanceof Error ? error.message : "Falha ao atualizar metadados.");
    } finally {
      setIsSavingMetadata(false);
    }
  }

  async function reprocessDocument(document: IngestedDocument) {
    setUploadError("");
    try {
      const response = await apiFetch(`/backend-api/documents/${document.id}/reprocess`, { method: "POST" });
      if (!response.ok) throw new Error(await readError(response));
      const job = (await response.json()) as Job;
      setJobs((current) => [job, ...current]);
      setDocuments((current) => current.map((item) => item.id === document.id ? { ...item, status: "pending" } : item));
    } catch (error) {
      setUploadError(error instanceof Error ? error.message : "Falha ao reprocessar manual.");
    }
  }

  function submitQuestion(event: FormEvent) {
    event.preventDefault();
    void ask(question);
  }

  async function login(event: FormEvent) {
    event.preventDefault();
    setIsLoggingIn(true);
    setLoginError("");
    try {
      const response = await fetch("/backend-api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: loginUsername, password: loginPassword }),
      });
      if (!response.ok) throw new Error(await readError(response));
      const result = (await response.json()) as { token: string; user: AuthUser };
      window.localStorage.setItem(authTokenKey, result.token);
      setUser(result.user);
      setLoginPassword("");
    } catch (error) {
      setLoginError(error instanceof Error ? error.message : "Falha ao entrar.");
    } finally {
      setIsLoggingIn(false);
    }
  }

  async function logout() {
    await apiFetch("/backend-api/auth/logout", { method: "POST" });
    window.localStorage.removeItem(authTokenKey);
    setUser(null);
    setMessages([]);
    setDocuments([]);
    setKpis(null);
  }

  if (!authReady) {
    return <main className="auth-page"><LoaderCircle className="spin" size={24} /></main>;
  }

  if (authRequired && !user) {
    return (
      <main className="auth-page">
        <form className="login-panel" onSubmit={login}>
          <div className="login-mark"><ShieldCheck size={24} /></div>
          <div><h1>Central de Manuais Técnicos</h1><p>Entre para consultar procedimentos e diagnósticos.</p></div>
          <label>Usuário<input autoComplete="username" value={loginUsername} onChange={(event) => setLoginUsername(event.target.value)} /></label>
          <label>Senha<input autoComplete="current-password" type="password" value={loginPassword} onChange={(event) => setLoginPassword(event.target.value)} /></label>
          {loginError && <div className="error-banner"><X size={17} />{loginError}</div>}
          <button className="primary-action" type="submit" disabled={!loginUsername || !loginPassword || isLoggingIn}>
            {isLoggingIn ? <LoaderCircle className="spin" size={17} /> : <UserRound size={17} />}
            Entrar
          </button>
        </form>
      </main>
    );
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark"><BookOpenText size={19} /></div>
          <div><strong>Tech Manuals</strong><span>Suporte técnico fundamentado</span></div>
        </div>

        <nav className="nav-list" aria-label="Navegação principal">
          <button aria-label="Consulta" className={view === "chat" ? "active" : ""} onClick={() => setView("chat")}>
            <MessageSquareText size={18} /><span>Consulta</span>
          </button>
          <button aria-label="Documentos" className={view === "documents" ? "active" : ""} onClick={() => setView("documents")}>
            <FileText size={18} /><span>Manuais</span>
            {jobs.some((job) => job.status === "processing") && <i />}
          </button>
          {canManage && <button aria-label="Indicadores" className={view === "analytics" ? "active" : ""} onClick={() => setView("analytics")}>
            <BarChart3 size={18} /><span>Indicadores</span>
          </button>}
        </nav>

        <div className="system-status">
          <span><i /> Sistema operacional</span>
          <small>{user ? `${user.username} · ${user.is_staff ? "Gestor técnico" : "Consulta"}` : "Base técnica local"}</small>
          {user && <button title="Sair" onClick={() => void logout()}><LogOut size={14} /><span>Sair</span></button>}
        </div>
      </aside>

      {view === "chat" ? (
        <section className="workspace">
          <header className="topbar">
            <div><h1>Assistente técnico</h1><p>Diagnósticos e procedimentos fundamentados nos manuais indexados</p></div>
            <button className="icon-button" title={sourcesOpen ? "Ocultar fontes" : "Mostrar fontes"} onClick={() => setSourcesOpen((open) => !open)}>
              {sourcesOpen ? <PanelRightClose size={19} /> : <PanelRightOpen size={19} />}
            </button>
          </header>

          <div className={`chat-layout ${sourcesOpen ? "" : "sources-hidden"}`}>
            <div className="conversation">
              <section className="technical-filters" aria-label="Filtros técnicos">
                <div className="filter-heading"><Filter size={15} /><span>Escopo da consulta</span>{activeFilterCount > 0 && <b>{activeFilterCount}</b>}</div>
                <label>
                  <span>Fabricante</span>
                  <select aria-label="Fabricante" value={manufacturerFilter} onChange={(event) => { setManufacturerFilter(event.target.value); setModelFilter(""); }}>
                    <option value="">Todos</option>
                    {manufacturers.map((manufacturer) => <option key={manufacturer}>{manufacturer}</option>)}
                  </select>
                </label>
                <label>
                  <span>Modelo</span>
                  <select aria-label="Modelo" value={modelFilter} onChange={(event) => setModelFilter(event.target.value)}>
                    <option value="">Todos</option>
                    {models.map((model) => <option key={model}>{model}</option>)}
                  </select>
                </label>
                <label>
                  <span>Conteúdo</span>
                  <select aria-label="Tipo de conteúdo" value={contentTypeFilter} onChange={(event) => setContentTypeFilter(event.target.value)}>
                    <option value="">Todo o manual</option>
                    {Object.entries(contentTypeLabels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}
                  </select>
                </label>
                {activeFilterCount > 0 && <button title="Limpar filtros" onClick={() => { setManufacturerFilter(""); setModelFilter(""); setContentTypeFilter(""); }}><X size={15} /></button>}
              </section>
              <div className="messages">
                {!messages.length && (
                  <div className="empty-chat">
                    <div className="empty-icon"><Wrench size={27} /></div>
                    <h2>Qual equipamento precisa de suporte?</h2>
                    <p>Informe sintomas, códigos de erro ou procedimentos e confira as páginas usadas.</p>
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
                      <div className="message-meta">{message.role === "assistant" ? "Assistente técnico" : "Você"}</div>
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
                    <div className="message-body"><div className="message-meta">Consultando manuais</div><div className="thinking"><i /><i /><i /></div></div>
                  </article>
                )}
              </div>

              <form className="composer" onSubmit={submitQuestion}>
                <input aria-label="Pergunta" value={question} onChange={(event) => setQuestion(event.target.value)} placeholder="Descreva o sintoma, erro ou procedimento..." />
                <button type="submit" title="Enviar pergunta" disabled={!question.trim() || isAnswering}><ArrowUp size={19} /></button>
              </form>
            </div>

            {sourcesOpen && (
              <aside className="sources-panel">
                <div className="panel-heading"><div><span>Evidências do manual</span><strong>{selectedSources.length} trechos recuperados</strong></div><Search size={18} /></div>
                {!selectedSources.length ? (
                  <div className="panel-empty"><FileSearch size={24} /><p>As fontes da resposta aparecerão aqui.</p></div>
                ) : (
                  <div className="source-list">
                    {selectedSources.map((source) => (
                      <article key={source.chunk_id} className="source-item">
                        <div className="source-top"><span>Fonte {source.number}</span><strong>{Math.round(source.score * 100)}%</strong></div>
                        <h3>{source.source_name}</h3>
                        <div className="source-tags">
                          {source.metadata?.content_type && <span>{contentTypeLabels[source.metadata.content_type] ?? source.metadata.content_type}</span>}
                          {source.metadata?.safety_level && <span className="warning"><AlertTriangle size={11} />{source.metadata.safety_level}</span>}
                          {source.metadata?.error_codes?.slice(0, 3).map((code) => <span className="code" key={code}>{code}</span>)}
                        </div>
                        {source.metadata?.section_heading && <small className="source-section">{source.metadata.section_heading}</small>}
                        <p>{source.content}</p>
                        <small>{source.metadata?.manufacturer ? `${source.metadata.manufacturer} · ` : ""}{source.page_number ? `Página ${source.page_number}` : "Documento textual"}</small>
                      </article>
                    ))}
                  </div>
                )}
              </aside>
            )}
          </div>
        </section>
      ) : view === "documents" ? (
        <section className="workspace">
          <header className="topbar">
            <div><h1>Biblioteca de manuais</h1><p>Cobertura técnica, modelos e estado da indexação</p></div>
            {canManage && <button className="primary-action" onClick={() => fileInput.current?.click()} disabled={isUploading}>
              {isUploading ? <LoaderCircle className="spin" size={17} /> : <Plus size={17} />}
              Adicionar manual
            </button>}
            <input ref={fileInput} hidden type="file" accept=".txt,.md,.pdf" onChange={(event) => event.target.files?.[0] && void uploadDocument(event.target.files[0])} />
          </header>

          <div className="documents-page">
            {canManage && <section className="upload-zone" onClick={() => fileInput.current?.click()}>
              <Upload size={24} />
              <div><strong>Adicionar manual à base técnica</strong><span>Fabricante, modelos, seções e códigos serão identificados automaticamente</span></div>
              <ChevronRight size={18} />
            </section>}
            {uploadError && <div className="error-banner"><X size={17} />{uploadError}</div>}

            <div className="section-title"><div><h2>Manuais disponíveis</h2><p>{documentPagination.total} arquivo(s) cobrindo {models.length} modelo(s)</p></div><button className="icon-button" title="Atualizar" onClick={() => void loadDocuments()} disabled={isLoadingDocuments}><RefreshCw className={isLoadingDocuments ? "spin" : ""} size={17} /></button></div>

            {!documents.length ? (
              <div className="document-empty"><FileText size={26} /><strong>Nenhum manual disponível</strong><span>Adicione um manual técnico para iniciar a biblioteca.</span></div>
            ) : (
              <div className="document-table">
                <div className="table-head"><span>Manual</span><span>Cobertura</span><span>Conteúdo</span><span>Indexação</span><span>Ações</span></div>
                {documents.map((document) => {
                  const job = jobs.find((item) => item.document_id === document.id);
                  const displayStatus = job?.status ?? document.status;
                  const badgeStatus =
                    displayStatus === "indexed" ? "completed" : displayStatus;
                  return (
                    <article key={document.id} className="document-row">
                      <div className="document-name"><div><FileText size={18} /></div><span><strong>{document.title}</strong><small>{document.metadata?.manufacturer ? `${document.metadata.manufacturer} · ` : ""}{document.source_name}</small></span></div>
                      <div className="document-coverage"><strong>{document.metadata?.models?.length ?? 0} modelos</strong><small>{document.metadata?.models?.slice(0, 2).join(" · ") || "Cobertura não identificada"}{(document.metadata?.models?.length ?? 0) > 2 ? ` +${(document.metadata?.models?.length ?? 0) - 2}` : ""}</small></div>
                      <div className="document-kind"><Tags size={14} /><span>{manualTypeLabels[document.metadata?.manual_type ?? ""] ?? "Documento técnico"}</span><small>{document.chunk_count} chunks</small></div>
                      <span className={`status-badge ${badgeStatus}`}>
                        {displayStatus === "completed" || displayStatus === "indexed" ? <Check size={14} /> : displayStatus === "failed" ? <X size={14} /> : displayStatus === "processing" ? <LoaderCircle className="spin" size={14} /> : <Clock3 size={14} />}
                        {job ? jobLabels[job.status] : documentStatusLabels[document.status] ?? document.status}
                      </span>
                      <div className="document-actions">
                        {canManage && <button className="icon-button" title="Revisar metadados" onClick={() => openMetadataEditor(document)}><Pencil size={14} /></button>}
                        {canManage && <button className="icon-button" title="Reprocessar manual" onClick={() => void reprocessDocument(document)} disabled={job ? ["queued", "processing", "retrying"].includes(job.status) : false}><RotateCw size={14} /></button>}
                      </div>
                    </article>
                  );
                })}
              </div>
            )}
            {documentPagination.total_pages > 1 && <nav className="document-pagination" aria-label="Paginação de manuais">
              <button className="icon-button" title="Página anterior" disabled={documentPage <= 1} onClick={() => setDocumentPage((page) => page - 1)}><ChevronLeft size={15} /></button>
              <span>Página {documentPagination.page} de {documentPagination.total_pages}</span>
              <button className="icon-button" title="Próxima página" disabled={documentPage >= documentPagination.total_pages} onClick={() => setDocumentPage((page) => page + 1)}><ChevronRight size={15} /></button>
            </nav>}
          </div>
          {editingDocument && metadataDraft && (
            <div className="modal-backdrop" role="presentation" onMouseDown={() => setEditingDocument(null)}>
              <form className="metadata-dialog" onSubmit={saveMetadata} onMouseDown={(event) => event.stopPropagation()}>
                <div className="dialog-heading"><div><h2>Revisar metadados</h2><p>{editingDocument.source_name}</p></div><button className="icon-button" type="button" title="Fechar" onClick={() => setEditingDocument(null)}><X size={16} /></button></div>
                <label>Título<input value={metadataDraft.title} onChange={(event) => setMetadataDraft({ ...metadataDraft, title: event.target.value })} /></label>
                <label>Fabricante<input value={metadataDraft.manufacturer} onChange={(event) => setMetadataDraft({ ...metadataDraft, manufacturer: event.target.value })} /></label>
                <label>Modelos<input value={metadataDraft.models} onChange={(event) => setMetadataDraft({ ...metadataDraft, models: event.target.value })} placeholder="MFC-L5710DN, MFC-L5715DW" /></label>
                <div className="dialog-grid">
                  <label>Equipamento<select value={metadataDraft.equipment_type} onChange={(event) => setMetadataDraft({ ...metadataDraft, equipment_type: event.target.value })}><option value="printer">Impressora</option><option value="scanner">Scanner</option><option value="multifunction">Multifuncional</option><option value="other">Outro</option></select></label>
                  <label>Tipo de manual<select value={metadataDraft.manual_type} onChange={(event) => setMetadataDraft({ ...metadataDraft, manual_type: event.target.value })}>{Object.entries(manualTypeLabels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
                </div>
                <button className="primary-action" type="submit" disabled={isSavingMetadata}>{isSavingMetadata ? <LoaderCircle className="spin" size={16} /> : <Check size={16} />}Salvar e reindexar</button>
              </form>
            </div>
          )}
        </section>
      ) : (
        <section className="workspace">
          <header className="topbar">
            <div><h1>Indicadores da base técnica</h1><p>Uso dos manuais, recuperação e saúde da indexação</p></div>
            <button className="icon-button" title="Atualizar indicadores" onClick={() => void loadKpis()} disabled={isLoadingKpis}>
              <RefreshCw className={isLoadingKpis ? "spin" : ""} size={17} />
            </button>
          </header>

          <div className="analytics-page">
            {kpiError && <div className="error-banner"><X size={17} />{kpiError}</div>}
            {!kpis ? (
              <div className="analytics-empty"><LoaderCircle className="spin" size={24} /><span>Carregando indicadores...</span></div>
            ) : (
              <>
                <section className="metric-strip">
                  <article><span>Consultas totais</span><strong>{kpis.queries.total}</strong><small>{kpis.queries.last_24h} nas últimas 24h</small></article>
                  <article><span>Tempo médio</span><strong>{formatDuration(kpis.queries.average_response_ms)}</strong><small>P95 em {formatDuration(kpis.queries.p95_response_ms)}</small></article>
                  <article><span>Taxa de erro</span><strong>{(kpis.queries.error_rate * 100).toFixed(1)}%</strong><small>{kpis.queries.failed} consultas com falha</small></article>
                  <article><span>Documentos indexados</span><strong>{kpis.documents.indexed}/{kpis.documents.total}</strong><small>{kpis.queries.average_sources} fontes por resposta</small></article>
                </section>

                <div className="analytics-grid">
                  <section className="analytics-section">
                    <div className="analytics-heading"><div><Activity size={17} /><span>Consultas nos últimos 7 dias</span></div><small>Volume e erros</small></div>
                    <div className="timeline-chart">
                      {kpis.timeline.map((day) => {
                        const max = Math.max(...kpis.timeline.map((item) => item.total), 1);
                        return (
                          <div key={day.date} className="timeline-day">
                            <div className="bar-track"><i style={{ height: `${Math.max((day.total / max) * 100, day.total ? 8 : 0)}%` }} /></div>
                            <strong>{day.total}</strong>
                            <span>{new Intl.DateTimeFormat("pt-BR", { weekday: "short" }).format(new Date(`${day.date}T12:00:00`))}</span>
                            {day.errors > 0 && <small>{day.errors} erro</small>}
                          </div>
                        );
                      })}
                    </div>
                  </section>

                  <section className="analytics-section">
                    <div className="analytics-heading"><div><FileSearch size={17} /><span>Documentos mais recuperados</span></div><small>Consultas distintas</small></div>
                    <div className="ranking-list">
                      {!kpis.documents.top_retrieved.length ? <p>Nenhuma recuperação registrada.</p> : kpis.documents.top_retrieved.map((document, index) => (
                        <article key={`${document.document_id}-${document.source_name}`}>
                          <strong>{index + 1}</strong>
                          <div><span>{document.source_name}</span><small>{Math.round(document.average_score * 100)}% de score médio</small></div>
                          <b>{document.query_count}</b>
                        </article>
                      ))}
                    </div>
                  </section>
                </div>

                <section className="analytics-section query-history">
                  <div className="analytics-heading"><div><Clock3 size={17} /><span>Histórico recente</span></div><small>Últimas 10 consultas</small></div>
                  {!kpis.recent_queries.length ? <div className="history-empty">Nenhuma consulta registrada.</div> : (
                    <div className="history-table">
                      <div className="history-head"><span>Pergunta</span><span>Status</span><span>Fontes</span><span>Tempo</span></div>
                      {kpis.recent_queries.map((query) => (
                        <article key={query.request_id}>
                          <div><strong>{query.question}</strong><small>{query.model || "Sem geração"} · {new Date(query.created_at).toLocaleString("pt-BR")}</small></div>
                          <span className={`query-status ${query.status}`}>{query.status === "success" ? <Check size={13} /> : <AlertTriangle size={13} />}{query.status === "success" ? "Sucesso" : "Erro"}</span>
                          <span>{query.source_count}</span>
                          <span>{formatDuration(query.duration_ms)}</span>
                        </article>
                      ))}
                    </div>
                  )}
                </section>
              </>
            )}
          </div>
        </section>
      )}
    </main>
  );
}
