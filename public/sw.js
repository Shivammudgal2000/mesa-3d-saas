// Global life-cycle events listener to satisfy native PWA validation requirements
self.addEventListener('install', (event) => {
    // Force the waiting service worker to become the active service worker immediately
    self.skipWaiting();
});

self.addEventListener('activate', (event) => {
    // Allow the active service worker to set itself as the controller for all clients within its scope
    self.clients.claim();
});

// Fetch event proxy interceptor to handle future asset caching mechanisms seamlessly
self.addEventListener('fetch', (event) => {
    // Right now, pass requests straight to the live network with zero local thread delays
    event.respondWith(fetch(event.request));
});