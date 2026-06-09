# Visao geral do sistema RAG

O sistema RAG combina recuperacao de informacao com geracao de respostas. Antes
de responder uma pergunta, o sistema procura trechos relevantes nos documentos
indexados.

## Ingestao

Na ingestao, cada documento recebe um identificador, um hash de conteudo e
metadados de origem. O texto extraido e normalizado antes de ser dividido em
chunks.

## Recuperacao

Durante uma consulta, a pergunta do usuario sera transformada em embedding. Os
chunks semanticamente mais proximos serao recuperados do banco vetorial.

## Geracao

A LLM recebera a pergunta e o contexto recuperado. A resposta devera citar as
fontes utilizadas e evitar afirmacoes sem suporte nos documentos.
