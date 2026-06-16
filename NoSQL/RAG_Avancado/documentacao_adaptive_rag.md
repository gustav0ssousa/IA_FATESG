# Documentação Técnica — Adaptive RAG

## 1. Visão geral

O **Adaptive RAG** é uma abordagem em que o sistema decide dinamicamente qual estratégia usar para responder uma pergunta.

Em um RAG tradicional, o fluxo costuma ser fixo:

```text
Pergunta do usuário
    ↓
Busca no banco vetorial
    ↓
Recuperação dos top-k chunks
    ↓
Envio para a LLM
    ↓
Resposta final
```

Esse fluxo funciona, mas tem limitações:

- Perguntas simples podem gastar recursos desnecessários com busca vetorial.
- Perguntas complexas podem precisar de várias buscas, não apenas uma.
- Perguntas ambíguas podem precisar de reescrita antes da busca.
- Perguntas com baixa qualidade de recuperação podem exigir correção ou nova busca.
- Algumas perguntas podem ser respondidas diretamente sem consultar a base.

O **Adaptive RAG** melhora isso adicionando uma etapa de decisão:

```text
Pergunta do usuário
    ↓
Classificador de complexidade
    ↓
Escolha da estratégia
    ↓
Execução do pipeline adequado
    ↓
Validação da resposta
    ↓
Resposta final
```

---

## 2. Ideia central

A ideia principal é classificar a pergunta antes de executar o RAG.

Exemplo:

```text
Pergunta: "O que é Pydantic?"
Classificação: simples
Estratégia: resposta direta ou RAG mínimo
```

```text
Pergunta: "Como implementar autenticação MFA no Django REST usando TOTP e JWT?"
Classificação: intermediária
Estratégia: RAG simples + reranking
```

```text
Pergunta: "Analise minha arquitetura de RAG, identifique gargalos e proponha uma solução com busca híbrida, reranking e avaliação."
Classificação: complexa
Estratégia: query decomposition + multi-hop RAG + validação
```

---

## 3. Diferença entre RAG tradicional e Adaptive RAG

| Aspecto | RAG tradicional | Adaptive RAG |
|---|---|---|
| Estratégia | Fixa | Dinâmica |
| Busca | Sempre busca | Busca apenas quando necessário |
| Complexidade da pergunta | Geralmente ignorada | Usada para escolher o fluxo |
| Custo | Pode ser alto desnecessariamente | Melhor controle de custo |
| Qualidade em perguntas complexas | Pode ser limitada | Melhor com multi-hop e decomposição |
| Robustez | Depende do top-k recuperado | Pode corrigir busca ruim |

---

## 4. Níveis de complexidade sugeridos

Uma implementação prática pode usar quatro níveis:

### 4.1 Nível 0 — Resposta direta

Use quando a pergunta for conceitual, genérica ou não depender da base de conhecimento.

Exemplos:

```text
"O que é RAG?"
"O que é embedding?"
"Explique o conceito de API."
```

Fluxo:

```text
Pergunta → LLM → Resposta
```

---

### 4.2 Nível 1 — RAG simples

Use quando a pergunta depende da base, mas é direta.

Exemplos:

```text
"Qual endpoint faz login no sistema?"
"Onde está documentado o MFA?"
"Qual sensor mede inclinação no projeto?"
```

Fluxo:

```text
Pergunta → busca vetorial/top-k → contexto → LLM → resposta
```

---

### 4.3 Nível 2 — RAG com busca híbrida e reranking

Use quando a pergunta contém termos técnicos, siglas, nomes de arquivos, logs, códigos de erro ou detalhes específicos.

Exemplos:

```text
"Como resolver o erro 'Exclusive access could not be obtained' no SQL Server?"
"Onde está a configuração JWT no backend?"
"Como funciona o GenerateEmbedding no projeto MobileFaceNet?"
```

Fluxo:

```text
Pergunta
  ↓
Busca híbrida: BM25 + embeddings
  ↓
Reranking
  ↓
Contexto filtrado
  ↓
LLM
  ↓
Resposta com fontes
```

---

### 4.4 Nível 3 — RAG multi-hop / iterativo

Use quando a pergunta exige raciocínio em etapas ou consulta a múltiplos documentos.

Exemplos:

```text
"Quais sensores do meu projeto ajudam na predição de deslizamento e como cada variável entra no modelo?"
"Compare a abordagem atual de reconhecimento facial com a arquitetura offline usando MobileFaceNet."
"Monte um plano de implementação de MFA considerando backend, frontend, banco e segurança."
```

Fluxo:

```text
Pergunta
  ↓
Decomposição em subperguntas
  ↓
Busca para cada subpergunta
  ↓
Reranking por subpergunta
  ↓
Síntese das evidências
  ↓
Resposta final validada
```

---

### 4.5 Nível 4 — RAG corretivo

Use quando o sistema percebe que os documentos recuperados são fracos, contraditórios ou insuficientes.

Exemplos:

```text
"Como configurar o HC220G5 como repetidor em um roteador Zyxel?"
```

Se a base interna não tiver informação suficiente, o sistema pode:

- Reescrever a consulta.
- Buscar novamente.
- Usar outra fonte.
- Informar que a base não contém informação suficiente.
- Pedir mais contexto apenas quando realmente necessário.

Fluxo:

```text
Pergunta
  ↓
Busca inicial
  ↓
Avaliação da qualidade dos documentos
  ↓
Se ruim: corrigir query e buscar novamente
  ↓
Se ainda ruim: responder com limitação explícita
```

---

## 5. Arquitetura recomendada

```text
adaptive-rag/
├── app/
│   ├── main.py
│   ├── api/
│   │   └── routes.py
│   ├── core/
│   │   ├── config.py
│   │   └── logging.py
│   ├── rag/
│   │   ├── classifier.py
│   │   ├── router.py
│   │   ├── retriever.py
│   │   ├── hybrid_search.py
│   │   ├── reranker.py
│   │   ├── decomposer.py
│   │   ├── evaluator.py
│   │   ├── generator.py
│   │   └── prompts.py
│   ├── ingestion/
│   │   ├── loaders.py
│   │   ├── chunker.py
│   │   ├── metadata.py
│   │   └── embedder.py
│   └── schemas/
│       └── rag_schema.py
├── data/
│   ├── raw/
│   └── processed/
├── docs/
│   └── adaptive-rag.md
├── tests/
│   ├── test_classifier.py
│   ├── test_retriever.py
│   └── test_router.py
├── requirements.txt
└── README.md
```

---

## 6. Componentes principais

### 6.1 Classificador de complexidade

Responsável por decidir qual estratégia será usada.

Entradas:

```json
{
  "question": "Como implementar MFA no Django REST usando TOTP?"
}
```

Saída:

```json
{
  "level": "intermediate",
  "strategy": "hybrid_rag_rerank",
  "reason": "A pergunta envolve implementação técnica específica e depende de documentação."
}
```

Critérios possíveis:

| Critério | Indício de complexidade |
|---|---|
| Pergunta muito curta | Pode exigir expansão |
| Termos como "compare", "analise", "monte", "planeje" | Complexa |
| Código de erro ou log | Busca híbrida |
| Menção a arquivo, endpoint ou classe | Busca na base |
| Pergunta conceitual simples | Resposta direta |
| Pergunta com múltiplas partes | Multi-hop |

---

### 6.2 Router

O router recebe a classificação e chama o pipeline correto.

Exemplo:

```text
simple → direct_answer_chain
basic_rag → vector_rag_chain
intermediate → hybrid_rag_rerank_chain
complex → multi_hop_chain
low_confidence → corrective_rag_chain
```

---

### 6.3 Retriever

Responsável pela recuperação de documentos.

Tipos:

- Vetorial: busca por embeddings.
- Lexical: BM25 ou full-text search.
- Híbrido: combinação de vetorial + lexical.
- Filtrado: busca usando metadados.

---

### 6.4 Reranker

Recebe muitos documentos candidatos e reordena com base na relevância real para a pergunta.

Fluxo:

```text
30 documentos recuperados → reranker → 5 melhores documentos
```

---

### 6.5 Decomposer

Quebra perguntas complexas em subperguntas.

Exemplo:

```text
Pergunta original:
"Quais sensores do projeto de deslizamento podem alimentar o modelo preditivo e como cada variável deve ser tratada?"

Subperguntas:
1. Quais sensores existem no projeto?
2. Quais variáveis cada sensor coleta?
3. Quais variáveis são úteis para predição?
4. Como tratar essas variáveis no pipeline de ML?
```

---

### 6.6 Evaluator

Avalia se os documentos recuperados são bons o suficiente.

Critérios:

- Relevância.
- Cobertura.
- Ausência de contradição.
- Similaridade com a pergunta.
- Presença de fontes confiáveis.
- Score mínimo.

Saída possível:

```json
{
  "retrieval_quality": "low",
  "action": "rewrite_query_and_retry"
}
```

---

## 7. Fluxo completo do Adaptive RAG

```text
1. Receber pergunta do usuário.
2. Normalizar texto.
3. Classificar complexidade.
4. Escolher estratégia.
5. Executar pipeline correspondente.
6. Avaliar qualidade do contexto recuperado.
7. Reexecutar busca se necessário.
8. Gerar resposta.
9. Validar se a resposta está apoiada no contexto.
10. Retornar resposta com fontes ou limitação explícita.
```

---

## 8. Prompt para classificar complexidade

```text
Você é um classificador de complexidade para um sistema Adaptive RAG.

Classifique a pergunta do usuário em uma das estratégias abaixo:

1. direct_answer
   Use quando a pergunta for conceitual, simples e não depender da base de documentos.

2. basic_rag
   Use quando a pergunta depender da base de documentos, mas for direta.

3. hybrid_rag_rerank
   Use quando a pergunta contiver termos técnicos, nomes específicos, códigos de erro, logs, classes, endpoints, arquivos ou siglas.

4. multi_hop_rag
   Use quando a pergunta exigir várias etapas, comparação, análise, planejamento ou combinação de informações de múltiplos documentos.

5. corrective_rag
   Use quando a pergunta for ambígua, incompleta ou tiver alta chance de recuperação ruim.

Responda apenas em JSON válido:

{
  "strategy": "...",
  "complexity": "simple | basic | intermediate | complex | uncertain",
  "reason": "...",
  "needs_retrieval": true,
  "suggested_top_k": 5
}

Pergunta:
{question}
```

---

## 9. Exemplo prático em Python

Abaixo está um exemplo simplificado de implementação.

### 9.1 `classifier.py`

```python
from enum import Enum
from pydantic import BaseModel


class RagStrategy(str, Enum):
    DIRECT_ANSWER = "direct_answer"
    BASIC_RAG = "basic_rag"
    HYBRID_RAG_RERANK = "hybrid_rag_rerank"
    MULTI_HOP_RAG = "multi_hop_rag"
    CORRECTIVE_RAG = "corrective_rag"


class ClassificationResult(BaseModel):
    strategy: RagStrategy
    complexity: str
    reason: str
    needs_retrieval: bool
    suggested_top_k: int = 5


class HeuristicComplexityClassifier:
    """
    Classificador heurístico inicial.
    Em produção, pode ser substituído por uma LLM pequena ou modelo treinado.
    """

    COMPLEX_TERMS = [
        "analise", "compare", "monte", "planeje", "arquitetura",
        "implemente", "passo a passo", "vantagens", "desvantagens"
    ]

    TECHNICAL_TERMS = [
        "erro", "exception", "endpoint", "classe", "função", "método",
        "jwt", "mfa", "sql", "docker", "api", "log", "stacktrace"
    ]

    AMBIGUOUS_TERMS = [
        "isso", "esse erro", "essa parte", "aquele arquivo"
    ]

    def classify(self, question: str) -> ClassificationResult:
        q = question.lower().strip()

        if any(term in q for term in self.AMBIGUOUS_TERMS) and len(q.split()) < 8:
            return ClassificationResult(
                strategy=RagStrategy.CORRECTIVE_RAG,
                complexity="uncertain",
                reason="Pergunta curta ou ambígua, pode exigir reescrita e nova recuperação.",
                needs_retrieval=True,
                suggested_top_k=8,
            )

        if any(term in q for term in self.COMPLEX_TERMS):
            return ClassificationResult(
                strategy=RagStrategy.MULTI_HOP_RAG,
                complexity="complex",
                reason="Pergunta exige análise, planejamento ou múltiplas etapas.",
                needs_retrieval=True,
                suggested_top_k=10,
            )

        if any(term in q for term in self.TECHNICAL_TERMS):
            return ClassificationResult(
                strategy=RagStrategy.HYBRID_RAG_RERANK,
                complexity="intermediate",
                reason="Pergunta técnica com termos específicos, ideal para busca híbrida e reranking.",
                needs_retrieval=True,
                suggested_top_k=12,
            )

        if len(q.split()) <= 6:
            return ClassificationResult(
                strategy=RagStrategy.DIRECT_ANSWER,
                complexity="simple",
                reason="Pergunta curta e conceitual.",
                needs_retrieval=False,
                suggested_top_k=0,
            )

        return ClassificationResult(
            strategy=RagStrategy.BASIC_RAG,
            complexity="basic",
            reason="Pergunta direta que pode se beneficiar da base de conhecimento.",
            needs_retrieval=True,
            suggested_top_k=5,
        )
```

---

### 9.2 `router.py`

```python
from app.rag.classifier import RagStrategy


class AdaptiveRagRouter:
    def __init__(
        self,
        classifier,
        generator,
        vector_retriever,
        hybrid_retriever,
        reranker,
        decomposer,
        evaluator,
    ):
        self.classifier = classifier
        self.generator = generator
        self.vector_retriever = vector_retriever
        self.hybrid_retriever = hybrid_retriever
        self.reranker = reranker
        self.decomposer = decomposer
        self.evaluator = evaluator

    def answer(self, question: str) -> dict:
        classification = self.classifier.classify(question)

        if classification.strategy == RagStrategy.DIRECT_ANSWER:
            return self._direct_answer(question, classification)

        if classification.strategy == RagStrategy.BASIC_RAG:
            return self._basic_rag(question, classification)

        if classification.strategy == RagStrategy.HYBRID_RAG_RERANK:
            return self._hybrid_rag_rerank(question, classification)

        if classification.strategy == RagStrategy.MULTI_HOP_RAG:
            return self._multi_hop_rag(question, classification)

        return self._corrective_rag(question, classification)

    def _direct_answer(self, question: str, classification) -> dict:
        answer = self.generator.generate_without_context(question)
        return {
            "strategy": classification.strategy,
            "classification": classification.model_dump(),
            "answer": answer,
            "sources": [],
        }

    def _basic_rag(self, question: str, classification) -> dict:
        docs = self.vector_retriever.search(
            query=question,
            top_k=classification.suggested_top_k,
        )
        answer = self.generator.generate_with_context(question, docs)
        return {
            "strategy": classification.strategy,
            "classification": classification.model_dump(),
            "answer": answer,
            "sources": docs,
        }

    def _hybrid_rag_rerank(self, question: str, classification) -> dict:
        docs = self.hybrid_retriever.search(
            query=question,
            top_k=classification.suggested_top_k,
        )
        reranked_docs = self.reranker.rerank(question, docs, top_n=5)
        answer = self.generator.generate_with_context(question, reranked_docs)
        return {
            "strategy": classification.strategy,
            "classification": classification.model_dump(),
            "answer": answer,
            "sources": reranked_docs,
        }

    def _multi_hop_rag(self, question: str, classification) -> dict:
        subquestions = self.decomposer.decompose(question)
        all_docs = []

        for subquestion in subquestions:
            docs = self.hybrid_retriever.search(subquestion, top_k=8)
            docs = self.reranker.rerank(subquestion, docs, top_n=3)
            all_docs.extend(docs)

        answer = self.generator.generate_with_context(
            question=question,
            docs=all_docs,
            extra_instructions="Sintetize as evidências por etapa e evite conclusões sem fonte."
        )

        return {
            "strategy": classification.strategy,
            "classification": classification.model_dump(),
            "subquestions": subquestions,
            "answer": answer,
            "sources": all_docs,
        }

    def _corrective_rag(self, question: str, classification) -> dict:
        docs = self.hybrid_retriever.search(question, top_k=10)
        quality = self.evaluator.evaluate(question, docs)

        if quality["retrieval_quality"] == "high":
            answer = self.generator.generate_with_context(question, docs)
            return {
                "strategy": classification.strategy,
                "classification": classification.model_dump(),
                "answer": answer,
                "sources": docs,
                "quality": quality,
            }

        rewritten_query = self.generator.rewrite_query(question)
        retry_docs = self.hybrid_retriever.search(rewritten_query, top_k=10)
        retry_quality = self.evaluator.evaluate(question, retry_docs)

        if retry_quality["retrieval_quality"] == "low":
            return {
                "strategy": classification.strategy,
                "classification": classification.model_dump(),
                "answer": "Não encontrei informação suficiente na base para responder com segurança.",
                "sources": retry_docs,
                "quality": retry_quality,
            }

        answer = self.generator.generate_with_context(question, retry_docs)
        return {
            "strategy": classification.strategy,
            "classification": classification.model_dump(),
            "answer": answer,
            "sources": retry_docs,
            "quality": retry_quality,
        }
```

---

### 9.3 `hybrid_search.py`

```python
class HybridRetriever:
    def __init__(self, vector_store, lexical_store, alpha: float = 0.6):
        self.vector_store = vector_store
        self.lexical_store = lexical_store
        self.alpha = alpha

    def search(self, query: str, top_k: int = 10):
        vector_results = self.vector_store.search(query, top_k=top_k)
        lexical_results = self.lexical_store.search(query, top_k=top_k)

        merged = {}

        for doc in vector_results:
            doc_id = doc["id"]
            merged.setdefault(doc_id, doc)
            merged[doc_id]["vector_score"] = doc.get("score", 0.0)
            merged[doc_id].setdefault("lexical_score", 0.0)

        for doc in lexical_results:
            doc_id = doc["id"]
            merged.setdefault(doc_id, doc)
            merged[doc_id]["lexical_score"] = doc.get("score", 0.0)
            merged[doc_id].setdefault("vector_score", 0.0)

        results = []
        for doc in merged.values():
            vector_score = doc.get("vector_score", 0.0)
            lexical_score = doc.get("lexical_score", 0.0)

            doc["final_score"] = (
                self.alpha * vector_score +
                (1 - self.alpha) * lexical_score
            )
            results.append(doc)

        return sorted(results, key=lambda x: x["final_score"], reverse=True)[:top_k]
```

---

### 9.4 `evaluator.py`

```python
class RetrievalEvaluator:
    def __init__(self, min_score: float = 0.55, min_docs: int = 2):
        self.min_score = min_score
        self.min_docs = min_docs

    def evaluate(self, question: str, docs: list[dict]) -> dict:
        if not docs:
            return {
                "retrieval_quality": "low",
                "reason": "Nenhum documento recuperado.",
                "action": "rewrite_query_and_retry",
            }

        relevant_docs = [
            doc for doc in docs
            if doc.get("final_score", doc.get("score", 0.0)) >= self.min_score
        ]

        if len(relevant_docs) < self.min_docs:
            return {
                "retrieval_quality": "low",
                "reason": "Poucos documentos acima do score mínimo.",
                "action": "rewrite_query_and_retry",
            }

        return {
            "retrieval_quality": "high",
            "reason": "Documentos suficientes e com score aceitável.",
            "action": "generate_answer",
        }
```

---

### 9.5 `decomposer.py`

```python
class QuestionDecomposer:
    def decompose(self, question: str) -> list[str]:
        """
        Versão simplificada.
        Em produção, use uma LLM com prompt estruturado.
        """

        q = question.lower()

        if "sensores" in q and "modelo" in q:
            return [
                "Quais sensores são citados no projeto?",
                "Quais variáveis cada sensor coleta?",
                "Quais variáveis podem ser usadas em um modelo preditivo?",
                "Como preparar dados de sensores para machine learning?",
            ]

        if "implementar" in q or "arquitetura" in q:
            return [
                f"Quais requisitos aparecem na pergunta: {question}",
                f"Quais componentes técnicos são necessários para: {question}",
                f"Quais riscos, dependências e etapas de implementação existem para: {question}",
            ]

        return [question]
```

---

### 9.6 `generator.py`

```python
class AnswerGenerator:
    def __init__(self, llm):
        self.llm = llm

    def generate_without_context(self, question: str) -> str:
        prompt = f"""
Responda de forma objetiva e didática.

Pergunta:
{question}
"""
        return self.llm.invoke(prompt)

    def generate_with_context(
        self,
        question: str,
        docs: list[dict],
        extra_instructions: str = ""
    ) -> str:
        context = "\n\n".join(
            f"[Fonte: {doc.get('source', 'desconhecida')}]\n{doc.get('content', '')}"
            for doc in docs
        )

        prompt = f"""
Você é um assistente técnico especializado em RAG.

Regras:
1. Responda apenas com base no contexto fornecido.
2. Se a resposta não estiver no contexto, diga que a informação não foi encontrada.
3. Não invente nomes de arquivos, endpoints, classes ou configurações.
4. Cite as fontes usando o campo Fonte.
5. Seja claro, técnico e direto.

Instruções extras:
{extra_instructions}

Pergunta:
{question}

Contexto:
{context}
"""
        return self.llm.invoke(prompt)

    def rewrite_query(self, question: str) -> str:
        prompt = f"""
Reescreva a pergunta abaixo para melhorar a busca em uma base técnica.
Preserve termos importantes, siglas, nomes de ferramentas e mensagens de erro.
Retorne apenas a nova consulta.

Pergunta:
{question}
"""
        return self.llm.invoke(prompt)
```

---

## 10. Exemplo de endpoint com FastAPI

```python
from fastapi import FastAPI
from pydantic import BaseModel

from app.rag.classifier import HeuristicComplexityClassifier
from app.rag.router import AdaptiveRagRouter


app = FastAPI(title="Adaptive RAG API")


class QuestionRequest(BaseModel):
    question: str


class MockLLM:
    def invoke(self, prompt: str) -> str:
        return "Resposta gerada pela LLM com base no prompt."


class MockRetriever:
    def search(self, query: str, top_k: int = 5):
        return [
            {
                "id": "doc-1",
                "content": "Documento recuperado sobre o tema perguntado.",
                "source": "docs/exemplo.md",
                "score": 0.82,
                "final_score": 0.82,
            }
        ]


class MockReranker:
    def rerank(self, question: str, docs: list[dict], top_n: int = 5):
        return docs[:top_n]


class MockEvaluator:
    def evaluate(self, question: str, docs: list[dict]):
        return {
            "retrieval_quality": "high",
            "reason": "Mock de avaliação aprovado.",
            "action": "generate_answer",
        }


class MockDecomposer:
    def decompose(self, question: str):
        return [question]


llm = MockLLM()
classifier = HeuristicComplexityClassifier()
retriever = MockRetriever()

router = AdaptiveRagRouter(
    classifier=classifier,
    generator=None,
    vector_retriever=retriever,
    hybrid_retriever=retriever,
    reranker=MockReranker(),
    decomposer=MockDecomposer(),
    evaluator=MockEvaluator(),
)


@app.post("/ask")
def ask(payload: QuestionRequest):
    # Em um projeto real, injete o AnswerGenerator com a LLM usada.
    return {
        "message": "Configure o AnswerGenerator real para execução completa.",
        "classification": classifier.classify(payload.question).model_dump(),
    }
```

---

## 11. Exemplo de decisões do Adaptive RAG

### Exemplo A

Pergunta:

```text
"O que é embedding?"
```

Classificação esperada:

```json
{
  "strategy": "direct_answer",
  "complexity": "simple",
  "needs_retrieval": false
}
```

Motivo:

```text
Pergunta conceitual simples. Não precisa consultar a base.
```

---

### Exemplo B

Pergunta:

```text
"Onde está documentada a configuração de MFA no backend?"
```

Classificação esperada:

```json
{
  "strategy": "basic_rag",
  "complexity": "basic",
  "needs_retrieval": true
}
```

Motivo:

```text
Depende da base interna, mas é uma pergunta direta.
```

---

### Exemplo C

Pergunta:

```text
"Como resolver o erro 'could not enter raw repl' no mpremote com ESP32?"
```

Classificação esperada:

```json
{
  "strategy": "hybrid_rag_rerank",
  "complexity": "intermediate",
  "needs_retrieval": true
}
```

Motivo:

```text
Contém mensagem de erro específica. Busca lexical e vetorial juntas aumentam a chance de recuperar o trecho correto.
```

---

### Exemplo D

Pergunta:

```text
"Monte uma arquitetura completa para um RAG usando Markdown, PDF, XLSX, reranking e avaliação contínua."
```

Classificação esperada:

```json
{
  "strategy": "multi_hop_rag",
  "complexity": "complex",
  "needs_retrieval": true
}
```

Motivo:

```text
Exige planejamento, múltiplos componentes e síntese de várias fontes.
```

---

## 12. Como implementar em sprints

### Sprint 1 — RAG básico

Entregas:

- Loader de documentos Markdown/PDF.
- Chunking inicial.
- Geração de embeddings.
- Banco vetorial.
- Endpoint `/ask`.
- Resposta com contexto.

---

### Sprint 2 — Classificador de complexidade

Entregas:

- Classificador heurístico.
- Estratégias: `direct_answer`, `basic_rag`, `hybrid_rag_rerank`.
- Logs da estratégia escolhida.
- Testes unitários do classificador.

---

### Sprint 3 — Busca híbrida

Entregas:

- Busca vetorial.
- Busca BM25/full-text.
- Combinação de scores.
- Filtros por metadados.

---

### Sprint 4 — Reranking

Entregas:

- Reranker local ou via API.
- Comparação top-k antes/depois do reranking.
- Métricas de precisão do contexto.

---

### Sprint 5 — Multi-hop RAG

Entregas:

- Decomposição de perguntas.
- Busca por subpergunta.
- Consolidação de evidências.
- Prompt de síntese final.

---

### Sprint 6 — Corrective RAG

Entregas:

- Avaliador de qualidade da recuperação.
- Reescrita automática de query.
- Segunda tentativa de busca.
- Resposta com limitação explícita quando a base for insuficiente.

---

### Sprint 7 — Avaliação contínua

Entregas:

- Dataset de perguntas de teste.
- Métricas: recall@k, precision@k, faithfulness, answer relevancy.
- Dashboard ou relatório de qualidade.
- Coleta de feedback do usuário.

---

## 13. Métricas recomendadas

### 13.1 Métricas de recuperação

| Métrica | Objetivo |
|---|---|
| Recall@K | Verificar se o documento correto aparece entre os K primeiros |
| Precision@K | Medir quantos documentos recuperados são realmente úteis |
| MRR | Avaliar a posição do primeiro documento relevante |
| nDCG | Avaliar ordenação dos documentos por relevância |

---

### 13.2 Métricas de geração

| Métrica | Objetivo |
|---|---|
| Faithfulness | Verificar se a resposta está apoiada no contexto |
| Answer Relevancy | Verificar se a resposta responde à pergunta |
| Context Precision | Verificar se o contexto usado é útil |
| Context Recall | Verificar se o contexto contém o necessário |

---

## 14. Logs importantes

Registre sempre:

```json
{
  "question": "pergunta do usuário",
  "strategy": "hybrid_rag_rerank",
  "complexity": "intermediate",
  "retrieved_docs": 12,
  "reranked_docs": 5,
  "retrieval_quality": "high",
  "latency_ms": 1830,
  "total_tokens": 4200,
  "answer_has_sources": true
}
```

Esses logs ajudam a responder perguntas como:

- O classificador está escolhendo bem?
- O sistema está usando multi-hop demais?
- A busca está trazendo documentos ruins?
- O custo está aumentando sem ganho de qualidade?
- Quais perguntas mais falham?

---

## 15. Boas práticas

1. Comece com regras heurísticas antes de treinar um classificador.
2. Registre todas as decisões do router.
3. Nunca use sempre multi-hop, pois aumenta custo e latência.
4. Use busca híbrida para documentação técnica.
5. Use reranking antes de enviar contexto para a LLM.
6. Use metadados para restringir a busca por projeto, módulo, tipo de arquivo e data.
7. Quando a recuperação for ruim, não invente resposta.
8. Avalie o RAG com perguntas reais dos usuários.
9. Separe métricas de recuperação das métricas de geração.
10. Adicione feedback humano para melhorar a classificação.

---

## 16. Prompt final para geração com contexto

```text
Você é um assistente técnico especializado em responder com base em documentos recuperados.

Regras obrigatórias:
1. Use apenas o contexto fornecido.
2. Se a informação não estiver no contexto, diga claramente que não encontrou a informação na base.
3. Não invente endpoints, classes, arquivos, datas ou configurações.
4. Sempre que possível, cite o documento, seção ou metadado de origem.
5. Se houver conflito entre documentos, informe a divergência.
6. Responda de forma técnica, didática e objetiva.

Estratégia escolhida pelo Adaptive RAG:
{strategy}

Motivo da estratégia:
{reason}

Pergunta do usuário:
{question}

Contexto recuperado:
{context}

Resposta:
```

---

## 17. Quando usar Adaptive RAG

Use Adaptive RAG quando:

- Existem perguntas simples e complexas no mesmo sistema.
- O custo de chamadas à LLM e ao banco vetorial importa.
- A base documental é grande.
- A qualidade da recuperação varia muito.
- Você precisa responder perguntas técnicas, analíticas e comparativas.
- Você quer reduzir alucinações.
- Você quer rastrear por que uma estratégia foi escolhida.

Evite Adaptive RAG quando:

- O sistema é muito pequeno.
- Todas as perguntas são parecidas.
- A base tem poucos documentos.
- A latência precisa ser mínima e previsível.
- Você ainda não tem um RAG básico funcionando.

---

## 18. Referências recomendadas

- Jeong et al. — Adaptive-RAG: Learning to Adapt Retrieval-Augmented Large Language Models through Question Complexity.
- Asai et al. — Self-RAG: Learning to Retrieve, Generate, and Critique through Self-Reflection.
- Yan et al. — Corrective Retrieval Augmented Generation.

---

## 19. Resumo executivo

O **Adaptive RAG** é uma evolução natural de sistemas RAG tradicionais. Em vez de aplicar sempre a mesma estratégia, ele classifica a pergunta e escolhe o melhor pipeline.

A arquitetura mais prática para começar é:

```text
classificador heurístico
+ busca vetorial
+ busca híbrida
+ reranking
+ multi-hop para perguntas complexas
+ corrective RAG para baixa confiança
+ logs e avaliação contínua
```

Essa abordagem melhora o equilíbrio entre custo, latência e qualidade, principalmente em sistemas técnicos com documentação extensa, perguntas variadas e necessidade de respostas fundamentadas.
