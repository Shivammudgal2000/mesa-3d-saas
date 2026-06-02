// =================================================================================
//                 MESA 3D ENTERPRISE SaaS BACKEND MOTOR ENGINE
// =================================================================================
// Author: Software Engineer Shivam Mudgal
// Architecture: Node.js, Express, SQLite3 Relational Database, Real-Time Socket.io
// Sandbox Rule: Demo Tenant Account (#999) resets automatically every 30 minutes!
// =================================================================================

import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import multer from 'multer';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import QRCode from 'qrcode';

// Setup absolute path variables for the Linux production environment
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" } // Enable global cross-origin resource sharing for mobile devices
});

// MIDDLEWARE SETUPS
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Serve the 'public' directory as the main static file root
app.use(express.static(path.join(__dirname, 'public')));
// Explicitly expose the root 'uploads' folder so images display in the UI cleanly
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// CRITICAL: Ensure upload destination paths exist on Render's container upon engine boot
const dirs = [
    path.join(__dirname, 'public', 'models3d'),
    path.join(__dirname, 'uploads', 'chefs')
];
dirs.forEach(dir => {
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }
});

// MULTER STORAGE BLOCK FOR DISH 3D MODELS AND CHEF IMAGES
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        // Sort files dynamically depending on the input field name
        if (file.fieldname === "glb_model") {
            cb(null, path.join(__dirname, 'public', 'models3d'));
        } else if (file.fieldname === "chef_image") {
            cb(null, path.join(__dirname, 'uploads', 'chefs'));
        } else {
            cb(null, path.join(__dirname, 'uploads'));
        }
    },
    filename: (req, file, cb) => {
        // Append timestamps to file names to prevent system overwriting collisions
        const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
        cb(null, file.fieldname + '-' + uniqueSuffix + path.extname(file.originalname));
    }
});
const upload = multer({ storage: storage });

// SQLITE DATABASE INIZIALIZATION & SEEDING (Using standard compatible sqlite3 driver)
const db = new sqlite3.Database(path.join(__dirname, 'restaurant.db'), (err) => {
    if (err) console.error("Database connection failure:", err.message);
    else console.log("SQLite database linked successfully.");
});

db.serialize(() => {
    // Menu Table: Tracks 3D locations, standard pricing, offers, and text-based allergy flags
    db.run(`CREATE TABLE IF NOT EXISTS menu (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price REAL,
        offer_price REAL,
        is_offer INTEGER DEFAULT 0,
        allergies TEXT,
        image TEXT,
        glb_model TEXT
    )`);

    // Chefs Table: Stores metadata parameters and image references for the chef view component
    db.run(`CREATE TABLE IF NOT EXISTS chefs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        specialty TEXT,
        image TEXT
    )`);

    // AUTO-SEEDER: Auto-inject items if the tables initialize completely empty on Render
    db.get("SELECT COUNT(*) as count FROM menu", [], (err, row) => {
        if (row && row.count === 0) {
            db.run(`INSERT INTO menu (name, price, offer_price, is_offer, allergies, image, glb_model) VALUES 
                ('Premium Burger Stack', 399, 299, 1, 'Dairy,Gluten', '/images/burger.png', '/models3d/model3d-1780370807859-914884064.glb')`);
            console.log("Database seeded with sample 3D dish profiles!");
        }
    });
});

// CLEAN EXPRESS ROUTING LAYER FOR MULTI-PANEL NAVIGATION
app.get('/admin', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin', 'admin.html'));
});

app.get('/customer', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'customer', 'index.html'));
});

// DYNAMIC QR GENERATOR ENDPOINT (Bypasses hardcoded local configurations)
app.get('/api/generate-qr', async (req, res) => {
    try {
        const host = req.get('host'); // Reads incoming address environment dynamically (e.g., 'mesa-3d.onrender.com')
        const protocol = req.protocol;
        const targetCustomerUrl = `${protocol}://${host}/customer`;
        
        const qrImageBytes = await QRCode.toDataURL(targetCustomerUrl);
        res.json({ qrCode: qrImageBytes, url: targetCustomerUrl });
    } catch (err) {
        res.status(500).json({ error: "Failed to generate server-mapped QR matrix" });
    }
});

// MENU HANDLING ROUTES WITH ALLERGY STRINGS AND 3D MODELS
app.get('/api/menu', (req, res) => {
    db.all("SELECT * FROM menu", [], (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(rows);
    });
});

app.post('/api/menu', upload.single('glb_model'), (req, res) => {
    const { name, price, allergies } = req.body;
    // Set file path referencing the client folder access directly if file exists
    const glb_path = req.file ? `/models3d/${req.file.filename}` : '';
    const image_placeholder = '/images/default-dish.png';

    db.run(`INSERT INTO menu (name, price, allergies, image, glb_model) VALUES (?, ?, ?, ?, ?)`,
        [name, price, allergies || '', image_placeholder, glb_path],
        function(err) {
            if (err) return res.status(500).json({ error: err.message });
            io.emit('menu-updated'); // Notify all consumer clients to safely reload structures
            res.json({ success: true, id: this.lastID });
        }
    );
});

// OFFERS ROUTE: Toggles promotion rates globally
app.post('/api/menu/offer', (req, res) => {
    const { id, offer_price, is_offer } = req.body;
    db.run(`UPDATE menu SET offer_price = ?, is_offer = ? WHERE id = ?`, [offer_price, is_offer, id], (err) => {
        if (err) return res.status(500).json({ error: err.message });
        io.emit('offers-updated'); // Broadcast to open phones that pricing drops are live
        res.json({ success: true });
    });
});

// CHEFS DATA API IMPLEMENTATION
app.get('/api/chefs', (req, res) => {
    db.all("SELECT * FROM chefs", [], (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(rows);
    });
});

app.post('/api/chefs', upload.single('chef_image'), (req, res) => {
    const { name, specialty } = req.body;
    const img_path = req.file ? `/uploads/chefs/${req.file.filename}` : '/uploads/default-chef.png';

    db.run(`INSERT INTO chefs (name, specialty, image) VALUES (?, ?, ?)`, [name, specialty, img_path], function(err) {
        if (err) return res.status(500).json({ error: err.message });
        io.emit('chefs-updated');
        res.json({ success: true, id: this.lastID });
    });
});

// WEBSOCKET HANDSHAKE PROTOCOLS FOR LIVE REAL-TIME SERVICES
io.on('connection', (socket) => {
    console.log(`Connected browser node: ${socket.id}`);

    // Waiter Calling Routine: Passes request payload arrays to the Admin Monitor
    socket.on('call-waiter', (data) => {
        io.emit('notify-admin-waiter', {
            table: data.table || "Table 1",
            request: data.request,
            note: data.note || "",
            timestamp: new Date().toLocaleTimeString()
        });
    });

    socket.on('disconnect', () => {
        console.log(`Disconnected link node: ${socket.id}`);
    });
});

// ACTIVATE ENGINE MOTOR
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Mesa 3D Engine running on web port: ${PORT}`);
});
