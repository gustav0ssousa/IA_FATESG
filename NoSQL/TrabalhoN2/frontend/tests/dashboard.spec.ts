import { expect, test } from "@playwright/test";

const source = {
  number: 1,
  chunk_id: "chunk-1",
  document_id: "document-1",
  score: 0.94,
  content: "Antes do troubleshooting, verifique se o papel não está úmido.",
  source_name: "sm_elfb_e_ver2.pdf",
  page_number: 35,
  metadata: {
    manufacturer: "Brother",
    models: ["MFC-L5710DN"],
    section_heading: "Checks before Commencing Troubleshooting",
    content_type: "troubleshooting",
    error_codes: ["0501"],
    safety_level: "warning",
  },
};

test.beforeEach(async ({ page }) => {
  await page.route("**/backend-api/auth/config", (route) =>
    route.fulfill({ json: { required: false } }),
  );
  await page.route("**/backend-api/documents**", (route) =>
    route.fulfill({
      json: {
        results: [{
          id: "document-1",
          title: "Brother - DCP-L5510DN + 24 models - Service Manual",
          source_name: "sm_elfb_e_ver2.pdf",
          source_type: "pdf",
          status: "indexed",
          chunk_count: 802,
          metadata: {
            manufacturer: "Brother",
            models: ["MFC-L5710DN", "MFC-L5715DW", "DCP-L5510DN"],
            equipment_type: "multifunction",
            manual_type: "service_manual",
          },
        }],
        pagination: { page: 1, page_size: 25, total: 1, total_pages: 1 },
        facets: {
          manufacturers: ["Brother"],
          models: ["MFC-L5710DN", "MFC-L5715DW", "DCP-L5510DN"],
        },
      },
    }),
  );
  await page.route("**/backend-api/rag/query", (route) =>
    route.fulfill({
      json: {
        answer: "Verifique se o papel recomendado está seco antes do diagnóstico [Fonte 1].",
        sources: [source],
        request_id: "request-1",
      },
    }),
  );
  await page.route("**/backend-api/rag/kpis/overview", (route) =>
    route.fulfill({
      json: {
        queries: {
          total: 12,
          last_24h: 3,
          successful: 11,
          failed: 1,
          error_rate: 0.0833,
          average_response_ms: 1200,
          p95_response_ms: 2400,
          average_sources: 2.5,
        },
        documents: {
          total: 4,
          indexed: 3,
          top_retrieved: [
            {
              document_id: "document-1",
              source_name: "sm_elfb_e_ver2.pdf",
              retrieval_count: 8,
              query_count: 6,
              average_score: 0.93,
            },
          ],
        },
        indexing_jobs: { queued: 0, processing: 0, retrying: 0, completed: 4, failed: 0 },
        timeline: [
          { date: "2026-06-04", total: 0, errors: 0 },
          { date: "2026-06-05", total: 1, errors: 0 },
          { date: "2026-06-06", total: 2, errors: 0 },
          { date: "2026-06-07", total: 1, errors: 0 },
          { date: "2026-06-08", total: 3, errors: 1 },
          { date: "2026-06-09", total: 2, errors: 0 },
          { date: "2026-06-10", total: 3, errors: 0 },
        ],
        recent_queries: [
          {
            request_id: "request-1",
            question: "Como funciona o retrieval?",
            status: "success",
            model: "sabia-4",
            source_count: 2,
            duration_ms: 1200,
            created_at: "2026-06-10T10:00:00Z",
          },
        ],
      },
    }),
  );
});

test("autenticação obrigatória permite login no dashboard", async ({ page }) => {
  await page.unroute("**/backend-api/auth/config");
  await page.route("**/backend-api/auth/config", (route) =>
    route.fulfill({ json: { required: true } }),
  );
  await page.route("**/backend-api/auth/login", (route) =>
    route.fulfill({
      json: {
        token: "token-teste",
        user: { id: 1, username: "gestor", is_staff: true },
      },
    }),
  );

  await page.goto("/");
  await page.getByLabel("Usuário").fill("gestor");
  await page.getByLabel("Senha").fill("senha-forte");
  await page.getByRole("button", { name: "Entrar" }).click();

  await expect(page.getByText("Assistente técnico")).toBeVisible();
  await expect(page.getByTitle("Sair")).toBeVisible();
});

test("consulta técnica envia filtros e exibe evidências do manual", async ({ page }) => {
  let queryPayload: Record<string, string> = {};
  await page.unroute("**/backend-api/rag/query");
  await page.route("**/backend-api/rag/query", async (route) => {
    queryPayload = route.request().postDataJSON();
    await route.fulfill({
      json: {
        answer: "Verifique se o papel recomendado está seco antes do diagnóstico [Fonte 1].",
        sources: [source],
        request_id: "request-1",
      },
    });
  });
  await page.goto("/");
  await page.getByLabel("Fabricante").selectOption("Brother");
  await page.getByLabel("Modelo").selectOption("MFC-L5710DN");
  await page.getByLabel("Tipo de conteúdo").selectOption("troubleshooting");
  await page.getByLabel("Pergunta").fill("O que verificar antes do troubleshooting?");
  await page.getByTitle("Enviar pergunta").click();

  await expect(page.getByText("Verifique se o papel recomendado")).toBeVisible();
  await page.getByRole("button", { name: "1 fontes utilizadas" }).click();
  await expect(page.getByText("sm_elfb_e_ver2.pdf")).toBeVisible();
  await expect(page.getByText("Checks before Commencing Troubleshooting")).toBeVisible();
  expect(queryPayload).toMatchObject({
    manufacturer: "Brother",
    model: "MFC-L5710DN",
    content_type: "troubleshooting",
  });
});

test("biblioteca exibe cobertura e tipo dos manuais", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Documentos" }).click();

  await expect(page.getByText("Brother - DCP-L5510DN")).toBeVisible();
  await expect(page.getByText("3 modelos")).toBeVisible();
  await expect(page.getByText("Manual de serviço")).toBeVisible();
});

test("indicadores exibem KPIs e histórico recente", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Indicadores" }).click();

  await expect(page.getByText("Consultas totais")).toBeVisible();
  await expect(page.getByText("Documentos mais recuperados")).toBeVisible();
  await expect(page.getByText("Como funciona o retrieval?")).toBeVisible();
});
