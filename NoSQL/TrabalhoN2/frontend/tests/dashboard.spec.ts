import { expect, test } from "@playwright/test";

const source = {
  number: 1,
  chunk_id: "chunk-1",
  document_id: "document-1",
  score: 0.94,
  content: "O retrieval recupera contexto relevante antes da geração.",
  source_name: "rag_overview.md",
  page_number: null,
};

test.beforeEach(async ({ page }) => {
  await page.route("**/backend-api/documents", (route) =>
    route.fulfill({
      json: [
        {
          id: "document-1",
          title: "Visão geral do RAG",
          source_name: "rag_overview.md",
          source_type: "md",
          status: "indexed",
          chunk_count: 3,
        },
      ],
    }),
  );
  await page.route("**/backend-api/rag/query", (route) =>
    route.fulfill({
      json: {
        answer: "O sistema recupera contexto antes de gerar a resposta [Fonte 1].",
        sources: [source],
        request_id: "request-1",
      },
    }),
  );
});

test("consulta exibe resposta e fonte recuperada", async ({ page }) => {
  await page.goto("/");
  await page.getByLabel("Pergunta").fill("Como funciona o retrieval?");
  await page.getByTitle("Enviar pergunta").click();

  await expect(page.getByText("O sistema recupera contexto")).toBeVisible();
  await page.getByRole("button", { name: "1 fontes utilizadas" }).click();
  await expect(page.getByText("rag_overview.md")).toBeVisible();
});

test("documentos persistidos aparecem na área de trabalho", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Documentos" }).click();

  await expect(page.getByText("Visão geral do RAG")).toBeVisible();
  await expect(page.getByText("rag_overview.md")).toBeVisible();
});
