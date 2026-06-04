// 🟩 SYSTEM FIX: High-Performance Multi-Tenant Service Worker Caching Cache
const CACHE_NAME_TOKEN = 'mesa3d-core-v2';

// Core static script infrastructure targets assets to lock locally in mobile memory storage
const STATIC_ASSETS_TO_CACHE = [
    '/customer/index.html',
    '/admin/admin.html',
    '/kitchen/kitchen.html',
    '/manifest.json'
];

// Life-cycle Install event handler: Compiles core static assets down into local disk partitions
self.addEventListener('install', (event) => {
    event.waitUntil(
        caches.open(CACHE_NAME_TOKEN).then((cacheStorageObject) => {
            console.log('[Mesa PWA Memory Engine] Compiling platform static assets down into cache storage layers...');
            return cacheStorageObject.addAll(STATIC_ASSETS_TO_CACHE);
        })
    );
    self.skipWaiting();
});

self.addEventListener('activate', (event) => {
    // Clear out preceding legacy cache pools to avoid memory allocation footprint collision errors
    event.waitUntil(
        caches.keys().then((cacheKeyCollection) => {
            return Promise.all(
                cacheKeyCollection.map((key) => {
                    if (key !== CACHE_NAME_TOKEN) {
                        console.log('[Mesa PWA Memory Engine] Purging expired legacy cache metrics maps partitions:', key);
                        return caches.delete(key);
                    }
                })
            );
        })
    );
    self.clients.claim();
});

// 🟩 NETWORK-FIRST SMART CACHE FALLBACK PIPELINE INTERCEPTOR
self.addEventListener('fetch', (event) => {
    const requestUrlString = event.request.url;

    // RULE 1: Completely ignore Socket.io handshake streams and real-time database inputs routes
    if (requestUrlString.includes('/socket.io/') || requestUrlString.includes('/api/')) {
        return; 
    }

    // RULE 2: Execute Network-First fallback logic path queries loops maps metrics
    event.respondWith(
        fetch(event.request)
            .then((networkResponsePacket) => {
                // If network response packet returns healthy assets, replicate a reference mapping tag copy directly into local cache partitions
                if (networkResponsePacket && networkResponsePacket.status === 200 && networkResponsePacket.type === 'basic') {
                    const cacheCloneInstance = networkResponsePacket.clone();
                    caches.open(CACHE_NAME_TOKEN).then((cacheMemoryObject) => {
                        cacheMemoryObject.put(event.request, cacheCloneInstance);
                    });
                }
                return networkResponsePacket;
            })
            .catch(() => {
                // IF SMARTPHONE ENCOUNTERS DEAD ZONE: Instantly serve the cached static file instead of failing
                return caches.match(event.request).then((cachedResponsePayload) => {
                    if (cachedResponsePayload) return cachedResponsePayload;
                });
            })
    );
});
