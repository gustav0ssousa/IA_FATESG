# Manuais Tecnicos

## Contexto do dominio

O RAG atende inicialmente suporte tecnico de impressoras, scanners e
multifuncionais. As respostas devem ajudar a localizar especificacoes, codigos
de erro, causas, verificacoes, manutencao e procedimentos, sempre citando o
manual e respeitando alertas de seguranca.

A implementacao permanece expansivel: regras de dominio geram metadados
genericos e nao dependem exclusivamente de uma marca.

## Manual analisado

Arquivo: `docs/sm_elfb_e_ver2.pdf`.

- Brother Laser MFC Service Manual, versao 2.
- 513 paginas e aproximadamente 50 MB.
- 25 modelos DCP/MFC/EX.
- Conteudo em ingles.
- PDF AES com extracao permitida.
- Inclui seguranca, especificacoes, troubleshooting, codigos de erro,
  desmontagem, ajustes, funcoes de servico e manutencao periodica.

Com chunking padrao (`1200/200`), o manual gera 802 chunks:

| Tipo | Chunks |
| --- | ---: |
| Procedimento | 478 |
| Troubleshooting | 114 |
| Referencia de erro | 92 |
| Referencia tecnica | 74 |
| Seguranca | 23 |
| Manutencao | 13 |
| Especificacao | 8 |

Foram detectados 23 chunks com alertas e 87 com codigos de erro.

## Metadados

Documento:

- `domain`, `manufacturer`, `models`, `equipment_type`.
- `manual_type`, `language`, `page_count`, `source_name`.

Chunk:

- Metadados do documento.
- `page_number`, `chapter`, `section_heading`.
- `content_type`, `error_codes`, `safety_level`.

Valores de `content_type`: `safety`, `troubleshooting`, `error_reference`,
`procedure`, `specification`, `maintenance` e `technical_reference`.

## Retrieval seguro

Busca e consulta aceitam filtros opcionais:

```json
{
  "question": "Como diagnosticar falha de alimentação?",
  "manufacturer": "Brother",
  "model": "MFC-L5710DN",
  "manual_type": "service_manual",
  "content_type": "troubleshooting"
}
```

O filtro por modelo reduz mistura entre famílias. Quando a pergunta não informa
o modelo, a resposta deve explicitar a aplicabilidade encontrada nas fontes.

## Gate de confiabilidade

Resultados com score inferior a `RAG_MIN_RELEVANCE_SCORE` sao removidos antes
da montagem do prompt. O valor inicial e `0.35` e deve ser calibrado com o
dataset técnico sempre que o modelo de embeddings ou o acervo mudar.

Quando a pergunta cita explicitamente um fabricante conhecido ou um modelo
identificavel, a geração somente acontece se ao menos uma fonte recuperada for
compativel. Caso contrario, o sistema retorna uma recusa sem chamar a LLM.

## Limitações

- Heuristicas podem exigir ajuste para novas marcas e formatos.
- Tabelas complexas podem perder relações entre colunas durante a extração.
- Diagramas, imagens e PDFs escaneados não são interpretados.
- O request HTTP persiste o upload e publica o job; extracao, chunking e
  indexacao executam no worker.
- O worker ainda carrega o conteudo exigido pelos extratores atuais; arquivos
  muito maiores podem exigir extracao incremental ou object storage.
- Procedimentos de serviço podem exigir técnico qualificado e equipamentos de
  proteção; o RAG não substitui treinamento ou normas do fabricante.
