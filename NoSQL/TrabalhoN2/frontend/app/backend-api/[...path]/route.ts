import { NextRequest } from "next/server";

type RouteContext = {
  params: Promise<{ path: string[] }>;
};

const apiBaseUrl = process.env.API_BASE_URL ?? "http://127.0.0.1:8000";
const apiAccessKey = process.env.API_ACCESS_KEY ?? "";
const authenticationRequired = process.env.API_REQUIRE_AUTHENTICATION === "True";

async function proxy(request: NextRequest, context: RouteContext) {
  const { path } = await context.params;
  const apiPath = path.length === 1 && path[0] === "documents"
    ? "documents/"
    : path.join("/");
  const target = new URL(`/api/${apiPath}${request.nextUrl.search}`, apiBaseUrl);
  const headers = new Headers(request.headers);
  headers.delete("accept-encoding");
  headers.delete("connection");
  headers.delete("content-length");
  headers.delete("host");
  if (apiAccessKey && !authenticationRequired) headers.set("x-api-key", apiAccessKey);

  const body = ["GET", "HEAD"].includes(request.method)
    ? undefined
    : await request.arrayBuffer();
  const fetchUpstream = () =>
    fetch(target, {
      method: request.method,
      headers,
      body,
      cache: "no-store",
    });
  let upstream: Response;
  try {
    upstream = await fetchUpstream();
  } catch (error) {
    if (request.method !== "GET") throw error;
    await new Promise((resolve) => setTimeout(resolve, 250));
    upstream = await fetchUpstream();
  }
  const responseHeaders = new Headers();
  for (const name of ["content-type", "content-disposition"]) {
    const value = upstream.headers.get(name);
    if (value) responseHeaders.set(name, value);
  }

  return new Response(upstream.body, {
    status: upstream.status,
    headers: responseHeaders,
  });
}

export const DELETE = proxy;
export const GET = proxy;
export const PATCH = proxy;
export const POST = proxy;
export const PUT = proxy;
