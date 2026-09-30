const port = Number(Bun.env.PORT ?? 3000);

Bun.serve({
  port,
  routes: {
    "/demo/completed.mp3": () => new Response(Bun.file("public/demo/completed.mp3"), {
      headers: { "Content-Type": "audio/mpeg" },
    }),
    "/demo/needs-input.mp3": () => new Response(Bun.file("public/demo/needs-input.mp3"), {
      headers: { "Content-Type": "audio/mpeg" },
    }),
    "/api/health": () => Response.json({ status: "ok" }),
    "/api/hello": () => Response.json({ message: "Hello from Speak Now!" }),
    "/assets/client.js": () => new Response(Bun.file("public/assets/client.js"), {
      headers: { "Content-Type": "text/javascript; charset=utf-8" },
    }),
    "/": () => new Response(Bun.file("public/index.html"), {
      headers: { "Content-Type": "text/html; charset=utf-8" },
    }),
  },
  fetch: () => new Response("Not found", { status: 404 }),
});

console.log(`Speak Now is running at http://localhost:${port}`);
