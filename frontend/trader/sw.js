self.addEventListener('install', () => self.skipWaiting());

self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.map((k) => caches.delete(k)));
    await self.clients.claim();
  })());
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  // Never intercept APIs or HTML pages (prevents stale dashboard / Offline).
  if (url.pathname.startsWith('/api/')) return;
  if (event.request.mode === 'navigate') return;
  if (url.pathname.endsWith('.html')) return;

  event.respondWith(
    fetch(event.request).catch(async () => {
      const cached = await caches.match(event.request);
      return cached || new Response('Offline', { status: 503 });
    })
  );
});
