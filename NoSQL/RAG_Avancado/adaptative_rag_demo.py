import re
import math
from enum import Enum
from dataclasses import dataclass
from collections import Counter, defaultdict
from typing import List, Dict, Optional


# ============================================================
# 1. MODELOS DE DADOS
# ============================================================

@dataclass
class Document:
    id: str
    title: str
    content: str
    metadata: Dict[str, str]


@dataclass
class SearchResult:
    document: Document
    score: float
    source: str


class RagStrategy(str, Enum):
    DIRECT_ANSWER = "direct_answer"
    BASIC_RAG = "basic_rag"
    HYBRID_RAG_RERANK = "hybrid_rag_rerank"
    MULTI_HOP_RAG = "multi_hop_rag"
    CORRECTIVE_RAG = "corrective_rag"


@dataclass
class QueryDecision:
    strategy: RagStrategy
    reason: str
    subqueries: Optional[List[str]] = None


# ============================================================
# 2. FUNÇÕES AUXILIARES
# ============================================================

STOPWORDS = {
    "a", "o", "os", "as", "um", "uma", "de", "do", "da", "dos", "das",
    "em", "no", "na", "nos", "nas", "para", "por", "com", "como",
    "que", "qual", "quais", "é", "e", "ou", "se", "me", "meu", "minha"
}


def tokenize(text: str) -> List[str]:
    text = text.lower()
    tokens = re.findall(r"\b[a-záéíóúãõâêîôûç0-9_\-]+\b", text)
    return [token for token in tokens if token not in STOPWORDS]


def cosine_similarity(vec_a: Dict[str, float], vec_b: Dict[str, float]) -> float:
    common_terms = set(vec_a.keys()) & set(vec_b.keys())

    dot_product = sum(vec_a[t] * vec_b[t] for t in common_terms)

    norm_a = math.sqrt(sum(v ** 2 for v in vec_a.values()))
    norm_b = math.sqrt(sum(v ** 2 for v in vec_b.values()))

    if norm_a == 0 or norm_b == 0:
        return 0.0

    return dot_product / (norm_a * norm_b)


# ============================================================
# 3. ÍNDICE SIMPLES COM BM25 + TF-IDF
# ============================================================

class SimpleRagIndex:
    def __init__(self, documents: List[Document]):
        self.documents = documents
        self.doc_tokens = {
            doc.id: tokenize(doc.title + " " + doc.content)
            for doc in documents
        }

        self.doc_lengths = {
            doc_id: len(tokens)
            for doc_id, tokens in self.doc_tokens.items()
        }

        self.avg_doc_length = (
            sum(self.doc_lengths.values()) / len(self.doc_lengths)
            if self.doc_lengths else 0
        )

        self.document_frequency = self._build_document_frequency()
        self.idf = self._build_idf()

    def _build_document_frequency(self) -> Dict[str, int]:
        df = defaultdict(int)

        for tokens in self.doc_tokens.values():
            unique_terms = set(tokens)
            for term in unique_terms:
                df[term] += 1

        return dict(df)

    def _build_idf(self) -> Dict[str, float]:
        total_docs = len(self.documents)
        idf = {}

        for term, freq in self.document_frequency.items():
            idf[term] = math.log((total_docs - freq + 0.5) / (freq + 0.5) + 1)

        return idf

    def _tfidf_vector(self, tokens: List[str]) -> Dict[str, float]:
        counter = Counter(tokens)
        vector = {}

        for term, freq in counter.items():
            if term in self.idf:
                vector[term] = (1 + math.log(freq)) * self.idf[term]

        return vector

    def bm25_search(self, query: str, top_k: int = 5) -> List[SearchResult]:
        query_terms = tokenize(query)

        k1 = 1.5
        b = 0.75

        results = []

        for doc in self.documents:
            doc_id = doc.id
            tokens = self.doc_tokens[doc_id]
            term_freq = Counter(tokens)

            score = 0.0

            for term in query_terms:
                if term not in term_freq:
                    continue

                tf = term_freq[term]
                idf = self.idf.get(term, 0)
                doc_len = self.doc_lengths[doc_id]

                numerator = tf * (k1 + 1)
                denominator = tf + k1 * (
                    1 - b + b * (doc_len / self.avg_doc_length)
                )

                score += idf * (numerator / denominator)

            if score > 0:
                results.append(SearchResult(doc, score, source="bm25"))

        return sorted(results, key=lambda r: r.score, reverse=True)[:top_k]

    def vector_search(self, query: str, top_k: int = 5) -> List[SearchResult]:
        query_vector = self._tfidf_vector(tokenize(query))

        results = []

        for doc in self.documents:
            doc_vector = self._tfidf_vector(self.doc_tokens[doc.id])
            score = cosine_similarity(query_vector, doc_vector)

            if score > 0:
                results.append(SearchResult(doc, score, source="tfidf_vector"))

        return sorted(results, key=lambda r: r.score, reverse=True)[:top_k]


# ============================================================
# 4. CLASSIFICADOR ADAPTATIVO DE PERGUNTAS
# ============================================================

class AdaptiveQueryClassifier:
    """
    Em produção, este classificador poderia ser:
    - uma LLM pequena;
    - um modelo treinado;
    - uma árvore de decisão;
    - regras + LLM;
    - classificador baseado em embeddings.
    """

    def classify(self, question: str) -> QueryDecision:
        question_lower = question.lower()
        tokens = tokenize(question)

        complex_terms = [
            "arquitetura", "implementar", "pipeline", "comparar",
            "multi", "projeto", "passo a passo", "estratégia",
            "documentação", "sprints", "módulos"
        ]

        technical_terms = [
            "erro", "exception", "traceback", "classe", "função",
            "endpoint", "api", "banco", "sql", "docker", "jwt",
            "rag", "embedding", "reranking"
        ]

        vague_terms = [
            "não encontrei", "não sei", "não funcionou", "falhou",
            "resultado ruim", "contexto insuficiente"
        ]

        if len(tokens) <= 4 and question_lower.startswith(("o que é", "explique")):
            return QueryDecision(
                strategy=RagStrategy.DIRECT_ANSWER,
                reason="Pergunta simples e conceitual. Pode ser respondida diretamente."
            )

        if any(term in question_lower for term in vague_terms):
            return QueryDecision(
                strategy=RagStrategy.CORRECTIVE_RAG,
                reason="Pergunta indica falha ou contexto insuficiente. Usar busca corretiva."
            )

        if any(term in question_lower for term in complex_terms):
            return QueryDecision(
                strategy=RagStrategy.MULTI_HOP_RAG,
                reason="Pergunta complexa. Exige decomposição em subconsultas.",
                subqueries=self._decompose(question)
            )

        if any(term in question_lower for term in technical_terms):
            return QueryDecision(
                strategy=RagStrategy.HYBRID_RAG_RERANK,
                reason="Pergunta técnica. Melhor usar busca híbrida com reranking."
            )

        return QueryDecision(
            strategy=RagStrategy.BASIC_RAG,
            reason="Pergunta direta. Usar RAG básico."
        )

    def _decompose(self, question: str) -> List[str]:
        return [
            question,
            f"Conceitos principais relacionados a: {question}",
            f"Componentes técnicos necessários para: {question}",
            f"Boas práticas e riscos em: {question}"
        ]


# ============================================================
# 5. RERANKER SIMPLES
# ============================================================

class SimpleReranker:
    def rerank(self, question: str, results: List[SearchResult], top_k: int = 3) -> List[SearchResult]:
        query_terms = set(tokenize(question))

        reranked = []

        for result in results:
            doc = result.document
            title_terms = set(tokenize(doc.title))
            content_terms = set(tokenize(doc.content))

            title_overlap = len(query_terms & title_terms)
            content_overlap = len(query_terms & content_terms)

            title_boost = title_overlap * 0.30
            content_boost = content_overlap * 0.10

            final_score = result.score + title_boost + content_boost

            reranked.append(
                SearchResult(
                    document=doc,
                    score=final_score,
                    source=f"{result.source}+rerank"
                )
            )

        return sorted(reranked, key=lambda r: r.score, reverse=True)[:top_k]


# ============================================================
# 6. AVALIADOR DE CONTEXTO
# ============================================================

class ContextEvaluator:
    def evaluate(self, question: str, results: List[SearchResult]) -> Dict[str, object]:
        if not results:
            return {
                "is_sufficient": False,
                "confidence": 0.0,
                "reason": "Nenhum documento recuperado."
            }

        query_terms = set(tokenize(question))
        context_terms = set()

        for result in results:
            context_terms.update(tokenize(result.document.content))

        coverage = len(query_terms & context_terms) / max(len(query_terms), 1)
        avg_score = sum(r.score for r in results) / len(results)

        confidence = min((coverage * 0.7) + (avg_score * 0.3), 1.0)

        return {
            "is_sufficient": confidence >= 0.25,
            "confidence": round(confidence, 2),
            "reason": "Contexto suficiente." if confidence >= 0.25 else "Contexto fraco ou insuficiente."
        }


# ============================================================
# 7. GERADOR DE RESPOSTA
# ============================================================

class AnswerGenerator:
    """
    Aqui você trocaria por uma chamada real para LLM:
    - OpenAI
    - Azure OpenAI
    - Ollama
    - Gemini
    - Claude
    - Llama local
    """

    def generate(self, question: str, results: List[SearchResult], strategy: RagStrategy) -> str:
        if strategy == RagStrategy.DIRECT_ANSWER:
            return self._generate_direct_answer(question)

        if not results:
            return (
                "Não encontrei informações suficientes na base documental "
                "para responder com segurança."
            )

        context = "\n\n".join(
            [
                f"[Fonte: {r.document.title} | Score: {r.score:.2f}]\n{r.document.content}"
                for r in results
            ]
        )

        prompt = f"""
Você é um assistente especializado em RAG.

Responda à pergunta usando apenas o contexto abaixo.

Pergunta:
{question}

Estratégia usada:
{strategy.value}

Contexto recuperado:
{context}

Regras:
1. Responda de forma objetiva.
2. Cite as fontes usadas.
3. Se o contexto não for suficiente, diga isso claramente.
"""

        return self._fake_llm_response(question, results, strategy, prompt)

    def _generate_direct_answer(self, question: str) -> str:
        return (
            f"Resposta direta para: '{question}'.\n\n"
            "Essa pergunta foi classificada como simples, então o sistema decidiu "
            "não consultar a base vetorial. Em produção, essa resposta poderia vir "
            "diretamente da LLM."
        )

    def _fake_llm_response(
        self,
        question: str,
        results: List[SearchResult],
        strategy: RagStrategy,
        prompt: str
    ) -> str:
        sources = "\n".join(
            [f"- {r.document.title} ({r.source}, score={r.score:.2f})" for r in results]
        )

        summary_points = []

        for result in results:
            content_preview = result.document.content[:220].replace("\n", " ")
            summary_points.append(f"- {content_preview}...")

        return f"""
Estratégia escolhida: {strategy.value}

Pergunta:
{question}

Resposta gerada com base nos documentos recuperados:

{chr(10).join(summary_points)}

Fontes utilizadas:
{sources}
""".strip()


# ============================================================
# 8. ADAPTIVE RAG PRINCIPAL
# ============================================================

class AdaptiveRAG:
    def __init__(self, documents: List[Document]):
        self.index = SimpleRagIndex(documents)
        self.classifier = AdaptiveQueryClassifier()
        self.reranker = SimpleReranker()
        self.context_evaluator = ContextEvaluator()
        self.generator = AnswerGenerator()

    def answer(self, question: str) -> str:
        decision = self.classifier.classify(question)

        print("\n==============================")
        print("DECISÃO DO ROUTER")
        print("==============================")
        print(f"Estratégia: {decision.strategy.value}")
        print(f"Motivo: {decision.reason}")

        if decision.subqueries:
            print("Subconsultas:")
            for subquery in decision.subqueries:
                print(f"- {subquery}")

        if decision.strategy == RagStrategy.DIRECT_ANSWER:
            return self.generator.generate(question, [], decision.strategy)

        if decision.strategy == RagStrategy.BASIC_RAG:
            results = self._basic_rag(question)
            return self._generate_or_correct(question, results, decision.strategy)

        if decision.strategy == RagStrategy.HYBRID_RAG_RERANK:
            results = self._hybrid_rag_rerank(question)
            return self._generate_or_correct(question, results, decision.strategy)

        if decision.strategy == RagStrategy.MULTI_HOP_RAG:
            results = self._multi_hop_rag(question, decision.subqueries or [])
            return self._generate_or_correct(question, results, decision.strategy)

        if decision.strategy == RagStrategy.CORRECTIVE_RAG:
            results = self._corrective_rag(question)
            return self.generator.generate(question, results, decision.strategy)

        return "Estratégia não reconhecida."

    def _basic_rag(self, question: str) -> List[SearchResult]:
        return self.index.vector_search(question, top_k=4)

    def _hybrid_rag_rerank(self, question: str) -> List[SearchResult]:
        bm25_results = self.index.bm25_search(question, top_k=5)
        vector_results = self.index.vector_search(question, top_k=5)

        merged = self._merge_results(bm25_results + vector_results)

        return self.reranker.rerank(question, merged, top_k=3)

    def _multi_hop_rag(self, question: str, subqueries: List[str]) -> List[SearchResult]:
        all_results = []

        for subquery in subqueries:
            bm25_results = self.index.bm25_search(subquery, top_k=3)
            vector_results = self.index.vector_search(subquery, top_k=3)
            all_results.extend(bm25_results + vector_results)

        merged = self._merge_results(all_results)

        return self.reranker.rerank(question, merged, top_k=5)

    def _corrective_rag(self, question: str) -> List[SearchResult]:
        print("\nExecutando busca inicial...")
        initial_results = self.index.vector_search(question, top_k=3)
        evaluation = self.context_evaluator.evaluate(question, initial_results)

        print(f"Avaliação inicial: {evaluation}")

        if evaluation["is_sufficient"]:
            return initial_results

        print("Contexto fraco. Reformulando consulta...")

        rewritten_query = self._rewrite_query(question)

        print(f"Nova consulta: {rewritten_query}")

        corrected_results = self._hybrid_rag_rerank(rewritten_query)
        corrected_evaluation = self.context_evaluator.evaluate(question, corrected_results)

        print(f"Avaliação após correção: {corrected_evaluation}")

        return corrected_results

    def _generate_or_correct(
        self,
        question: str,
        results: List[SearchResult],
        strategy: RagStrategy
    ) -> str:
        evaluation = self.context_evaluator.evaluate(question, results)

        print("\n==============================")
        print("AVALIAÇÃO DO CONTEXTO")
        print("==============================")
        print(evaluation)

        if evaluation["is_sufficient"]:
            return self.generator.generate(question, results, strategy)

        print("Contexto insuficiente. Acionando Corrective RAG...")

        corrected_results = self._corrective_rag(question)

        return self.generator.generate(
            question,
            corrected_results,
            RagStrategy.CORRECTIVE_RAG
        )

    def _rewrite_query(self, question: str) -> str:
        """
        Em produção, isso pode ser feito por uma LLM.
        Aqui usamos uma reformulação simples baseada em regras.
        """

        return (
            f"{question} documentação técnica implementação solução "
            f"passo a passo erro configuração arquitetura"
        )

    def _merge_results(self, results: List[SearchResult]) -> List[SearchResult]:
        merged = {}

        for result in results:
            doc_id = result.document.id

            if doc_id not in merged:
                merged[doc_id] = result
            else:
                existing = merged[doc_id]

                if result.score > existing.score:
                    merged[doc_id] = SearchResult(
                        document=result.document,
                        score=result.score,
                        source=f"{existing.source}+{result.source}"
                    )

        return sorted(merged.values(), key=lambda r: r.score, reverse=True)


# ============================================================
# 9. BASE DE DOCUMENTOS DE EXEMPLO
# ============================================================

documents = [
    Document(
        id="doc_1",
        title="Introdução ao RAG",
        content="""
RAG significa Retrieval-Augmented Generation. Essa arquitetura combina
modelos de linguagem com uma base externa de documentos. O objetivo é
responder perguntas usando informações recuperadas de uma fonte confiável.
""",
        metadata={"type": "concept", "module": "rag"}
    ),
    Document(
        id="doc_2",
        title="Adaptive RAG",
        content="""
Adaptive RAG é uma abordagem em que o sistema escolhe dinamicamente a
estratégia de recuperação conforme a complexidade da pergunta. Perguntas
simples podem ser respondidas diretamente, perguntas médias podem usar
RAG básico e perguntas complexas podem usar recuperação multi-hop.
""",
        metadata={"type": "architecture", "module": "adaptive_rag"}
    ),
    Document(
        id="doc_3",
        title="Hybrid Search com BM25 e Embeddings",
        content="""
A busca híbrida combina busca lexical, como BM25, com busca semântica
baseada em embeddings. Essa técnica melhora a recuperação em consultas
técnicas, logs, erros, nomes de classes, endpoints, funções e termos exatos.
""",
        metadata={"type": "retrieval", "module": "search"}
    ),
    Document(
        id="doc_4",
        title="Reranking no RAG",
        content="""
O reranking reordena os documentos recuperados inicialmente. Após buscar
vários chunks, um reranker escolhe os trechos mais relevantes para enviar
ao modelo de linguagem, reduzindo ruído no contexto.
""",
        metadata={"type": "retrieval", "module": "reranking"}
    ),
    Document(
        id="doc_5",
        title="Corrective RAG",
        content="""
Corrective RAG avalia se os documentos recuperados são suficientes para
responder à pergunta. Caso o contexto seja fraco, o sistema reformula a
consulta, executa nova busca ou informa que não possui evidência suficiente.
""",
        metadata={"type": "quality", "module": "corrective_rag"}
    ),
    Document(
        id="doc_6",
        title="Multi-hop RAG",
        content="""
Multi-hop RAG decompõe uma pergunta complexa em várias subconsultas.
Cada subconsulta recupera evidências diferentes. Depois, o sistema junta
as informações para gerar uma resposta mais completa.
""",
        metadata={"type": "advanced", "module": "multi_hop"}
    ),
    Document(
        id="doc_7",
        title="Arquitetura de um sistema RAG avançado",
        content="""
Uma arquitetura avançada de RAG pode conter ingestão de documentos,
chunking, metadados, embeddings, banco vetorial, busca híbrida, reranking,
compressão de contexto, avaliação de resposta e logs de observabilidade.
""",
        metadata={"type": "architecture", "module": "rag_system"}
    ),
]


# ============================================================
# 10. EXECUÇÃO
# ============================================================

if __name__ == "__main__":
    rag = AdaptiveRAG(documents)

    questions = [
        "O que é RAG?",
        "Como funciona Adaptive RAG?",
        "Como implementar uma arquitetura completa de RAG com busca híbrida, reranking e avaliação?",
        "Meu RAG não funcionou e trouxe contexto insuficiente",
        "Como usar reranking em uma API de RAG?"
    ]

    for question in questions:
        print("\n\n##################################################")
        print(f"PERGUNTA: {question}")
        print("##################################################")

        answer = rag.answer(question)

        print("\n==============================")
        print("RESPOSTA FINAL")
        print("==============================")
        print(answer)