// =================================================================================
//                 MESA 3D ENTERPRISE SaaS BACKEND MOTOR ENGINE
// =================================================================================
// Author: Software Engineer Shivam Mudgal
// Architecture: Node.js, Express, SQLite3 Relational Database, Real-Time Socket.io
// Sandbox Rule: Demo Tenant Account (#999) resets automatically every 30 minutes!
// =================================================================================

import express from 'express';          // Web framework handling HTTP routing pipelines
import { createServer } from 'http';     // Native HTTP server infrastructure core binding
import { Server } from 'socket.io';      // WebSockets engine for instant cross-screen syncing
import sqlite3 from 'sqlite3';           // Low-level structural database disk driver engine
import { open } from 'sqlite';           // Modern promise-based async SQLite interface wrapper
import path from 'path';                 // Core file pathway parsing framework resolver
import { fileURLToPath } from 'url';     // Helper parsing ES module system path parameters
import multer from 'multer';             // Multi-part binary file streaming directory driver
import QRCode from 'qrcode';             // Advanced matrix vector graphic barcode stream builder

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// Configure binary multi-part storage targets for 3D meshes and profile image avatars
const storageDiskConfiguration = multer.diskStorage({
    destination: (req, file, cb) => {
        if (file.fieldname === 'dish3DFile') {
            cb(null, path.join(__dirname, 'public/uploads/models3d'));
        } else {
            cb(null, path.join(__dirname, 'public/uploads/chefs'));
        }
    },
    filename: (req, file, cb) => {
        const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
        const prefix = file.fieldname === 'dish3DFile' ? 'model3d-' : 'chef-';
        cb(null, prefix + uniqueSuffix + path.extname(file.originalname));
    }
});
const uploadProcessor = multer({ storage: storageDiskConfiguration });

let db;

// =================================================================================
//              RELATIONAL DATABASE & SANDBOX DEMO INITIALIZATION
// =================================================================================
(async () => {
    try {
        db = await open({ filename: './restaurant.db', driver: sqlite3.Database });

        // Build relational core tracking database tables layout maps
        await db.exec(`
            CREATE TABLE IF NOT EXISTS restaurants (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT UNIQUE NOT NULL, password TEXT NOT NULL,
                mobile_number TEXT, property_address TEXT, property_owner TEXT, property_mail_id TEXT, property_phone_number TEXT,
                aadhaar_card TEXT, live_selfie_url TEXT, kitchen_passcode TEXT NOT NULL DEFAULT '1234', is_active INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS chefs (
                id INTEGER PRIMARY KEY AUTOINCREMENT, restaurant_id INTEGER, name TEXT NOT NULL, specialties TEXT, experience TEXT, photo_url TEXT
            );
            CREATE TABLE IF NOT EXISTS dishes (
                id INTEGER PRIMARY KEY AUTOINCREMENT, restaurant_id INTEGER, name TEXT NOT NULL, price REAL NOT NULL, image_url TEXT, is_promo INTEGER DEFAULT 0, dietary_type TEXT DEFAULT 'veg', allergens TEXT DEFAULT 'none', specialties TEXT DEFAULT '0'
            );
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT, restaurant_id INTEGER, location_context TEXT NOT NULL, location_identifier TEXT NOT NULL, items TEXT NOT NULL, total_price REAL NOT NULL, status TEXT DEFAULT 'New', estimated_prep_time INTEGER DEFAULT 15, assigned_chef_id INTEGER, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT, restaurant_id INTEGER, order_id INTEGER, food_rating INTEGER, service_rating INTEGER, comments TEXT, chef_id INTEGER, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        `);

        // Check if universal Sandbox Account exists. If not, explicitly provision Tenant #999
        const checkDemo = await db.get('SELECT id FROM restaurants WHERE id = 999');
        if (!checkDemo) {
            await db.run('INSERT INTO restaurants (id, name, email, password, kitchen_passcode) VALUES (999, "Demo Test Diner", "demo@mesa3d.io", "demo123", "1234")');
        }

        // Initialize pristine baseline demo data assets records instantly
        await resetDemoSandboxEnvironment();
        console.log('Mesa 3D Engine: Storage matrices and Sandbox environment active.');

        // ⏱️ AUTOMATED 30-MINUTE SANDBOX MAINTENANCE ENGINE RESET TRACE LOOP
        setInterval(async () => {
            console.log('[Maintenance Cron] 30-Minute Boundary Reached! Scrubbing Demo Tenant #999...');
            await resetDemoSandboxEnvironment();
            // Evict active demo frontends instantly by broadcasting a synchronized system refresh socket wave
            io.to('restaurant_999').emit('demo_expired', { message: "The 30-minute sandbox cycle has expired! Resetting profile data." });
        }, 30 * 60 * 1000);

    } catch (e) { console.error('Database Bootstrap Exception Failed:', e); }
})();

// Helper routine method to execute table purges and overwrite default demo assets records
async function resetDemoSandboxEnvironment() {
    await db.run('DELETE FROM dishes WHERE restaurant_id = 999');
    await db.run('DELETE FROM orders WHERE restaurant_id = 999');
    await db.run('DELETE FROM chefs WHERE restaurant_id = 999');
    await db.run('DELETE FROM feedback WHERE restaurant_id = 999');

    // Populate standard un-compromised baseline volumetric 3D models links fallback assets
    await db.run('INSERT INTO dishes (restaurant_id, name, price, image_url, is_promo, dietary_type, allergens, specialties) VALUES (999, "3D Fusion Premium Burger", 249.00, "/uploads/models3d/default-cube.gltf", 1, "veg", "gluten", "10")');
    await db.run('INSERT INTO dishes (restaurant_id, name, price, image_url, is_promo, dietary_type, allergens, specialties) VALUES (999, "Artisan Volcano Crust Pizza", 499.00, "/uploads/models3d/default-cube.gltf", 0, "veg", "dairy,gluten", "0")');
    await db.run('INSERT INTO chefs (restaurant_id, name, specialties, experience, photo_url) VALUES (999, "Master Chef (Demo)", "Volumetric Culinary Arts", "10+ Years", "https://img.icons8.com/color/100/chef-hat.png")');
}

// =================================================================================
//                      HTTP REST API ROUTING MATRIX LAYER
// =================================================================================

// Multi-Stage Registration Route 1: Validates and logs new enterprise accounts row records
app.post('/api/auth/signup-corporate', async (req, res) => {
    try {
        const { name, email, mobile_number, password, property_name, property_address, property_owner, property_mail_id, property_phone_number, aadhaar_card, live_selfie_url } = req.body;
        const result = await db.run(
            `INSERT INTO restaurants (name, email, password, mobile_number, property_address, property_owner, property_mail_id, property_phone_number, aadhaar_card, live_selfie_url) 
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [property_name, email, password, mobile_number, property_address, property_owner, property_mail_id, property_phone_number, aadhaar_card, live_selfie_url]
        );
        res.json({ success: true, assigned_tenant_id: result.lastID });
    } catch(err) { res.status(500).json({ success: false, error: "Email already registered or structural validation fault." }); }
});

// Authentication Route 2: Validates admin dashboard dashboard credentials profiles
app.post('/api/auth/login-admin', async (req, res) => {
    try {
        const { login_id, password } = req.body;
        const account = await db.get('SELECT * FROM restaurants WHERE (email = ? OR id = ?) AND password = ?', [login_id, login_id, password]);
        if (account) {
            res.json({ success: true, tenant_id: account.id, venue_name: account.name });
        } else { res.status(401).json({ success: false, error: "Invalid Admin Reference ID or security password profile match." }); }
    } catch(err) { res.status(500).json({ success: false, error: err.message }); }
});

// Authentication Route 3: Verifies line crew entry matching workspace scopes boundaries keys
app.post('/api/auth/login-kitchen', async (req, res) => {
    try {
        const { staff_id, kitchen_passcode } = req.body;
        const account = await db.get('SELECT * FROM restaurants WHERE id = ? AND kitchen_passcode = ?', [staff_id, kitchen_passcode]);
        if (account) {
            res.json({ success: true, tenant_id: account.id, venue_name: account.name + " (Kitchen Line)" });
        } else { res.status(401).json({ success: false, error: "Security PIN or Restaurant context ID verification failed." }); }
    } catch(err) { res.status(500).json({ success: false, error: err.message }); }
});

// Standard Catalog Data Retrieval Endpoint Pipeline Stream Router
app.get('/api/dishes', async (req, res) => {
    try { res.json(await db.all('SELECT * FROM dishes WHERE restaurant_id = ?', [req.query.restaurant_id || 1])); } catch (err) { res.json([]); }
});

app.post('/api/dishes/add', uploadProcessor.single('dish3DFile'), async (req, res) => {
    try {
        const { restaurant_id, name, price, dietary_type, allergens, offer_percentage } = req.body;
        let pth = req.file ? `/uploads/models3d/${req.file.filename}` : '/uploads/models3d/default-cube.gltf';
        await db.run('INSERT INTO dishes (restaurant_id, name, price, image_url, dietary_type, allergens, specialties) VALUES (?, ?, ?, ?, ?, ?, ?)', [restaurant_id || 1, name, parseFloat(price), pth, dietary_type, allergens, offer_percentage]);
        res.json({ success: true });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/chefs', async (req, res) => {
    try { res.json(await db.all('SELECT * FROM chefs WHERE restaurant_id = ?', [req.query.restaurant_id || 1])); } catch (err) { res.json([]); }
});

app.post('/api/chefs/add', uploadProcessor.single('chefPhotoFile'), async (req, res) => {
    try {
        const { restaurant_id, name, specialties, experience } = req.body;
        let pth = req.file ? `/uploads/chefs/${req.file.filename}` : 'https://img.icons8.com/color/100/chef-hat.png';
        await db.run('INSERT INTO chefs (restaurant_id, name, specialties, experience, photo_url) VALUES (?, ?, ?, ?, ?)', [restaurant_id || 1, name, specialties, experience, pth]);
        res.json({ success: true });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.post('/api/dishes/toggle-highlight', async (req, res) => {
    await db.run('UPDATE dishes SET is_promo = ? WHERE id = ? AND restaurant_id = ?', [req.query.is_promo, req.query.id, req.query.restaurant_id]);
    res.json({ success: true });
});

app.post('/api/dishes/toggle-offer', async (req, res) => {
    await db.run('UPDATE dishes SET is_promo = ?, specialties = ? WHERE id = ? AND restaurant_id = ?', [req.query.is_promo, String(req.query.percentage), req.query.id, req.query.restaurant_id]);
    res.json({ success: true });
});

app.post('/api/dishes/delete', async (req, res) => {
    await db.run('DELETE FROM dishes WHERE id = ? AND restaurant_id = ?', [req.query.id, req.query.restaurant_id]);
    res.json({ success: true });
});

app.post('/api/chefs/delete', async (req, res) => {
    await db.run('DELETE FROM chefs WHERE id = ? AND restaurant_id = ?', [req.query.id, req.query.restaurant_id]);
    res.json({ success: true });
});

app.get('/api/analytics/popular', async (req, res) => {
    try {
        const rows = await db.all('SELECT items FROM orders WHERE restaurant_id = ?', [req.query.restaurant_id || 1]);
        const map = {};
        rows.forEach(r => {
            const list = typeof r.items === 'string' ? JSON.parse(r.items) : r.items;
            if (Array.isArray(list)) list.forEach(i => { map[i.name] = (map[i.name] || 0) + 1; });
        });
        res.json(Object.keys(map).map(n => ({ name: n, count: map[n] })).sort((a,b)=>b.count-a.count).slice(0,5));
    } catch(e) { res.status(500).json([]); }
});

app.post('/api/feedback', async (req, res) => {
    const { restaurant_id, order_id, food_rating, service_rating, comments, chef_id } = req.body;
    const r = await db.run('INSERT INTO feedback (restaurant_id, order_id, food_rating, service_rating, comments, chef_id) VALUES (?, ?, ?, ?, ?, ?)', [restaurant_id || 1, order_id, food_rating, service_rating, comments, chef_id]);
    io.to(`restaurant_${restaurant_id}`).emit('incoming_feedback', { id: r.lastID, order_id, food_rating, service_rating, comments });
    res.json({ success: true });
});

// 🖨️ ADVANCED PWA AUTO-LAUNCHER STICKER QR GENERATOR ENDPOINT ENGINE
app.get('/api/inventory/qr', async (req, res) => {
    try {
        const targetURL = req.query.url;
        const lbl = req.query.label || 'Mesa3D Node';
        if (!targetURL) return res.status(400).send('Missing endpoint target URL configuration strings.');

        res.setHeader('Content-Type', 'image/png');
        res.setHeader('Content-Disposition', `attachment; filename="Mesa3D-QR-${lbl.replace(/\s+/g, '-')}.png"`);
        await QRCode.toFileStream(res, targetURL, { errorCorrectionLevel: 'H', margin: 3, color: { dark: '#0B0F19', light: '#FFFFFF' } });
    } catch (err) { res.status(500).send('QR Engine Serialization Fault Exception.'); }
});

// =================================================================================
//                     REAL-TIME WEBSOCKET PIPELINE NETWORKS LOGIC
// =================================================================================
io.on('connection', (socket) => {
    socket.on('join_workspace', ({ restaurantId }) => { socket.join(`restaurant_${restaurantId || 1}`); });
    
    socket.on('place_order', async (data) => {
        const r = await db.run('INSERT INTO orders (restaurant_id, location_context, location_identifier, items, total_price) VALUES (?, ?, ?, ?, ?)', [data.restaurant_id || 1, data.location_context, data.location_identifier, JSON.stringify(data.items), data.total_price]);
        io.to(`restaurant_${data.restaurant_id}`).emit('new_order_alert', { id: r.lastID, ...data, status: 'New' });
    });

    socket.on('update_order_status', async (d) => {
        if (d.status === 'Cooking') {
            await db.run('UPDATE orders SET status = ?, assigned_chef_id = ? WHERE id = ?', [d.status, d.chef_id, d.order_id]);
        } else { await db.run('UPDATE orders SET status = ? WHERE id = ?', [d.status, d.order_id]); }
        const updated = await db.get('SELECT o.*, c.name as chef_name FROM orders o LEFT JOIN chefs c ON o.assigned_chef_id = c.id WHERE o.id = ?', [d.order_id]);
        io.to(`restaurant_${d.restaurant_id}`).emit('order_pipeline_update', updated);
    });

    socket.on('call_waiter', (d) => { io.to(`restaurant_${d.restaurant_id}`).emit('waiter_alert', d); });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => { console.log(`Mesa 3D Enterprise System running on web port: ${PORT}`); });