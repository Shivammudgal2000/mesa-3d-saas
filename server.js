// =================================================================================
//                 MESA 3D ENTERPRISE SaaS BACKEND MOTOR ENGINE
// =================================================================================
// Author: Software Engineer Shivam Mudgal
// Architecture: Node.js, Express, SQLite3 Relational Database, Real-Time Socket.io
// Sandbox Rule: Demo Tenant Account (#999) resets automatically every 30 minutes!
// =================================================================================

// =================================================================================
//                 MESA 3D ENTERPRISE SaaS BACKEND CORE MOTOR ENGINE
// =================================================================================
// Author: Software Engineer Shivam Mudgal
// Architecture: Node.js, Express, SQLite3 Relational Database, Real-Time Socket.io
// Operational Modules: Hardware 3D Ingestion, PWA Registration, QR Matrix Engine
// =================================================================================

// --- SYSTEM DEPENDENCY MODULE INGESTION LAYER ---
import express from 'express';          // Enterprise web micro-framework to manage HTTP routing workflows
import { createServer } from 'http';     // Native Node.js HTTP server wrapper instance to bundle network layers
import { Server } from 'socket.io';      // Real-time bidirectional event engine for immediate client synchronization
import sqlite3 from 'sqlite3';           // Low-level database engine driver to execute operations directly on local disk files
import { open } from 'sqlite';           // Modern promise-based wrapper to execute cleaner async/await database code lines
import path from 'path';                 // Core utility module to resolve system absolute file pathway directory strings safely
import { fileURLToPath } from 'url';     // Helper framework to convert ECMA module URLs into valid local platform folder locations
import multer from 'multer';             // Specialized middleware engine to intercept and parse multipart/form-data multi-stream uploads
import QRCode from 'qrcode';             // Official production package tool to generate high-resolution matrix graphic QR streams
import fs from 'fs';               // 🟩 FIX: Native file-system manager to verify and deploy server directory folders on boot

// --- SYSTEM DIRECTORY STRUCTURAL RESOLUTIONS ---
const __filename = fileURLToPath(import.meta.url); // Translate standard file module URLs into a string file route path
const __dirname = path.dirname(__filename);       // Isolate the parent working directory location context parameter from the file route path

// --- APP CORE SETUPS & NETWORK LAYERS BINDINGS ---
const app = express();                     // Instantiating the global Express web application routing pipe handler instance
const server = createServer(app);          // Mounting the Express container onto a native Node HTTP network server shell layer
const io = new Server(server, {            // Initializing real-time WebSockets and defining open Cross-Origin Resource Sharing (CORS) rules
    cors: { origin: "*" }                  // Permits secure multi-panel cross-domain communication loops safely across platforms
});

// --- GLOBAL LEVEL ROUTING CONTROLLERS MIDDLEWARE ---
app.use(express.json({ limit: '50mb' }));                  // Forces incoming JSON payloads to automatically parse into accessible request body maps
app.use(express.urlencoded({ limit: '50mb', extended: true })); // Handles standard URL-encoded form submissions from classic browser input types
app.use(express.static(path.join(__dirname, 'public'))); // Maps the 'public' folder root to expose client assets (manifest.json, sw.js, html, css, js)

// 🟩 FIX: Fail-Safe Deployment Folder Builder
const directoriesToCreate = [
    path.join(__dirname, 'public/uploads/models3d'),
    path.join(__dirname, 'public/uploads/chefs')
];
directoriesToCreate.forEach(dirPath => {
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true });
        console.log(`[System Storage] Created missing production path: ${dirPath}`);
    }
});

// --- CONFIGURING MULTER DYNAMIC DISK STORAGE SPECIFICATIONS ---
const storageDiskConfiguration = multer.diskStorage({
    destination: (req, file, cb) => {
        // Evaluate internal field tokens to determine the exact asset destination sub-folder location on the disk drive
        if (file.fieldname === 'dish3DFile') {
            cb(null, path.join(__dirname, 'public/uploads/models3d')); // Route physical 3D model files (.gltf / .glb) into the 3D models directory
        } else {
            cb(null, path.join(__dirname, 'public/uploads/chefs'));    // Route physical line cook profile pictures into the crew directory folder
        }
    },
    filename: (req, file, cb) => {
        // Generate cryptographic random suffix integers using timestamps to prevent duplicate file naming collisions on disk write
        const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
        // Pre-pend explicit descriptive identification string headers according to incoming file field classifications
        const prefix = file.fieldname === 'dish3DFile' ? 'model3d-' : 'chef-';
        // Commit file writing operation with its original extension syntax signature intact
        cb(null, prefix + uniqueSuffix + path.extname(file.originalname));
    }
});
// I Ingests multipart chunks with stream compression limits configuration
const uploadProcessor = multer({
    storage: storageDiskConfiguration,
    limits: {
        fileSize: 100 * 1024 * 1024, // Expanded to 100MB margin to protect high-density complex 3D meshes
        fields: 100,                 // Increased threshold limit to allow structural staging array strings data through
        files: 1                     // Enforces exactly 1 file upload per network call block
    }
});

// Declare relational database connection token container point globally
let db;

// =================================================================================
//        RELATIONAL SQLITE SCHEMA INITIALIZATION ISOLATION BLOCK (ASYNC IMMED)
// =================================================================================
(async () => {
    try {
        // Open the local persistent SQLite relational database storage pool file matrix connection thread
        db = await open({
            filename: './restaurant.db',   // Database destination reference file on disk storage
            driver: sqlite3.Database       // Direct active driver selection mapping link bindings
        });

        // Schema 1: Multi-Tenant Workspace Configuration Records Table Setup
        await db.exec(`
            CREATE TABLE IF NOT EXISTS restaurants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                kitchen_passcode TEXT NOT NULL DEFAULT '1234',
                is_active INTEGER DEFAULT 1,
                subscription_expires TEXT
            )
        `);

        // Schema 2: Line Crew Personnel Roster Registration Information Matrix Table
        await db.exec(`
            CREATE TABLE IF NOT EXISTS chefs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                restaurant_id INTEGER,
                name TEXT NOT NULL,
                specialties TEXT,
                experience TEXT,
                photo_url TEXT,
                FOREIGN KEY (restaurant_id) REFERENCES restaurants(id)
            )
        `);

        // Schema 3: Volumetric 3D Dish Product Catalog Assets Registry Table Map Setup
        // Column 'image_url' maps the direct local file path link to the uploaded .gltf / .glb model asset file
        // Column 'is_promo' functions as the boolean bit flag to control visibility inside the Carousel Spotlight section
        // Column 'specialties' functions as the tracking string index storing numeric percentage discount integers
        await db.exec(`
            CREATE TABLE IF NOT EXISTS dishes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                restaurant_id INTEGER,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                image_url TEXT,
                is_promo INTEGER DEFAULT 0,
                dietary_type TEXT DEFAULT 'veg',
                allergens TEXT DEFAULT 'none',
                specialties TEXT DEFAULT '0', 
                FOREIGN KEY (restaurant_id) REFERENCES restaurants(id)
            )
        `);

        // Schema 4: High-Performance Live Pipeline Transaction Record Matrix Table
        // Column 'items' captures the nested customer order selections array converted into a flat JSON string data structure
        await db.exec(`
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                restaurant_id INTEGER,
                location_context TEXT NOT NULL, 
                location_identifier TEXT NOT NULL, 
                items TEXT NOT NULL, 
                total_price REAL NOT NULL,
                status TEXT DEFAULT 'New', 
                estimated_prep_time INTEGER DEFAULT 15,
                assigned_chef_id INTEGER,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (restaurant_id) REFERENCES restaurants(id),
                FOREIGN KEY (assigned_chef_id) REFERENCES chefs(id)
            )
        `);

        // Schema 5: Customer Closed-Loop Evaluation Feedback Review Logs Table
        await db.exec(`
            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                restaurant_id INTEGER,
                order_id INTEGER,
                food_rating INTEGER,
                service_rating INTEGER,
                comments TEXT,
                chef_id INTEGER,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (restaurant_id) REFERENCES restaurants(id),
                FOREIGN KEY (order_id) REFERENCES orders(id),
                FOREIGN KEY (chef_id) REFERENCES chefs(id)
            )
        `);

        console.log('Mesa 3D Engine: Database architecture tables verified and fully operational.');
    } catch (dbError) {
        console.error('Critical Server Crash: Relational Database Initialization Failure:', dbError);
    }
})();

// =================================================================================
//                      HTTP REST API ROUTING ARCHITECTURE LAYER
// =================================================================================

// Guard endpoint to instantly short-circuit unnecessary icon search requests from clean browser clients
app.get('/favicon.ico', (req, res) => res.status(204).end());

// API Endpoint 1: Fetches the entire active catalog matrix matching tenant parameters filtering criteria
app.get('/api/dishes', async (req, res) => {
    try {
        // Enforce the tracking query value to always read as a true base-10 integer!
        const rid = parseInt(req.query.restaurant_id) || 1;

        const items = await db.all('SELECT id, restaurant_id, name, price, image_url, is_promo, dietary_type, allergens, specialties AS offer_percentage FROM dishes WHERE restaurant_id = ?', [rid]);
        res.json(items);
    } catch (err) {
        res.status(500).json([]);
    }
});

// API Endpoint 2: Core multi-part file ingestion pipeline that maps physical 3D files to the database records
app.post('/api/dishes/add', uploadProcessor.single('dish3DFile'), async (req, res) => {
    try {
        // De-structure alphanumeric string input parameter values packed into the multi-part request block body container
        const { restaurant_id, name, price, dietary_type, allergens, offer_percentage } = req.body;

        // Define default fallback 3D model geometry path string if file upload stream parameters are missing
        let finalized3DModelPath = '/uploads/models3d/default-cube.gltf';
        if (req.file) {
            finalized3DModelPath = `/uploads/models3d/${req.file.filename}`; // Update destination locator with real dynamic file name tokens
        }

        console.log(`[3D Model Ingestion Engine] Registering asset item: "${name}" to database layout mapping rows...`);

        // Execute operational query string statement injection directly to include active data rows within the dishes schema table
        await db.run(
            'INSERT INTO dishes (restaurant_id, name, price, image_url, is_promo, dietary_type, allergens, specialties) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            [restaurant_id || 1, name, parseFloat(price), finalized3DModelPath, 0, dietary_type || 'veg', allergens || 'none', offer_percentage || '0']
        );
        res.json({ success: true }); // Return positive confirmation validation token back to admin panel view interface
    } catch (err) {
        console.error('SQL Execution Error inside /api/dishes/add endpoint module routing pipes:', err.message);
        res.status(500).json({ success: false, error: err.message }); // Send fault message trace response package directly
    }
});

// API Endpoint 3: Pulls current crew line cook profile rosters list parameters matching targeted tenant workspace context
app.get('/api/chefs', async (req, res) => {
    try {
        const rid = req.query.restaurant_id || 1;
        const staff = await db.all('SELECT * FROM chefs WHERE restaurant_id = ?', [rid]);
        res.json(staff);
    } catch (err) {
        res.status(500).json([]);
    }
});

// API Endpoint 4: Processes multi-part form parameters containing profile pictures and registers staff entities
app.post('/api/chefs/add', uploadProcessor.single('chefPhotoFile'), async (req, res) => {
    try {
        const { restaurant_id, name, specialties, experience } = req.body;
        const rid = restaurant_id || 1;

        // Captures the local relative path string where Multer saved the file
        const photoUrl = req.file ? `/uploads/chefs/${req.file.filename}` : null;

        await db.run(
            'INSERT INTO chefs (restaurant_id, name, specialties, experience, photo_url) VALUES (?, ?, ?, ?, ?)',
            [parseInt(rid), name, specialties, experience, photoUrl]
        );
        res.json({ success: true });
    } catch (err) {
        console.error('File Upload Registry Processing Error inside /api/chefs/add module:', err.message);
        res.status(500).json({ success: false, error: err.message });
    }
});

// API Endpoint 5: Purges a targeted dish row completely from the catalog matching unique auto-increment row IDs
app.post('/api/dishes/delete', async (req, res) => {
    try {
        const id = req.query.id || req.body.id;
        const restaurant_id = req.query.restaurant_id || req.body.restaurant_id || 1;

        await db.run('DELETE FROM dishes WHERE id = ? AND restaurant_id = ?', [id, restaurant_id]);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// API Endpoint 6: Intelligence Business Analytics Node - Computes most frequent choices from dynamic order transaction history
app.get('/api/analytics/popular', async (req, res) => {
    try {
        const rid = req.query.restaurant_id || 1;
        // Fetch out raw serialized items orders matrix strings data directly matching workspace profiles target identifiers
        const rows = await db.all(
            "SELECT items FROM orders WHERE restaurant_id = ? AND DATE(timestamp) = DATE('now', 'localtime')",
            [rid]
        );

        const itemCountsContainerMap = {}; // Setup data map memory dictionary tracker allocation block

        // Loop structural logs data to strip arrays and compute string instance repetitions variables loops
        rows.forEach(row => {
            try {
                // Safeguard check parsing nested layout parameters arrays safely into actionable objects fields models
                const parsedItemsList = typeof row.items === 'string' ? JSON.parse(row.items) : row.items;
                if (Array.isArray(parsedItemsList)) {
                    parsedItemsList.forEach(item => {
                        const targetDishTitleKey = item.name;
                        // Accumulate count iterations values onto the dictionary object keys index map configurations
                        itemCountsContainerMap[targetDishTitleKey] = (itemCountsContainerMap[targetDishTitleKey] || 0) + 1;
                    });
                }
            } catch (e) { /* Catch block intercepts malformed historical JSON text fragments and ignores failures gracefully */ }
        });

        // Convert the tracking map data structure into an ordered collection sorted by highest frequency
        const sortedPopularityRankedCollection = Object.keys(itemCountsContainerMap).map(name => ({
            name: name,
            count: itemCountsContainerMap[name]
        })).sort((a, b) => b.count - a.count).slice(0, 5); // Isolate and return only the top 5 ranking menu items

        res.json(sortedPopularityRankedCollection); // Dispatch clean statistical matrix package back to admin analytics drawer dashboard
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// API Endpoint 7: Highlight Carousel Controller Logic - Toggles visibility flags to instantly push items into the main showcase slider
app.post('/api/dishes/toggle-highlight', async (req, res) => {
    try {
        const id = req.query.id || req.body.id;
        // Intercept targeted boolean control bit value states (1 for enabled visibility spotlight, 0 to demote)
        const is_promo = req.query.is_promo !== undefined ? parseInt(req.query.is_promo) : 0;
        const restaurant_id = req.query.restaurant_id || req.body.restaurant_id || 1;

        console.log(`[Admin Override Panel] Mutation requested for Item ID: #${id}. Setting highlight bit to: ${is_promo}`);

        // Mutate the row context to alter promo visibility metrics on client devices immediately
        await db.run('UPDATE dishes SET is_promo = ? WHERE id = ? AND restaurant_id = ?', [is_promo, id, restaurant_id]);
        res.json({ success: true }); // Return verification loop confirmation token package signature to front controller components
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// API Endpoint 8: Percentage Promotional Value Ingestion - Modifies numeric discount parameters mapping columns states
app.post('/api/dishes/toggle-offer', async (req, res) => {
    try {
        const id = req.query.id || req.body.id;
        const is_promo = req.query.is_promo !== undefined ? parseInt(req.query.is_promo) : 0;
        const offer_percentage = req.query.percentage !== undefined ? req.query.percentage : '0';
        const restaurant_id = req.query.restaurant_id || req.body.restaurant_id || 1;

        console.log(`[Database Mutation Request] Modifying Promo Offers for Dish ID: #${id} to target index level: ${offer_percentage}% Off`);
        // Map the string value percentage reduction directly down onto the 'specialties' record column matrix mapping configurations
        await db.run('UPDATE dishes SET is_promo = ?, specialties = ? WHERE id = ? AND restaurant_id = ?', [is_promo, String(offer_percentage), id, restaurant_id]);
        res.json({ success: true });
    } catch (err) {
        console.error('Core Offer Settings Modification Execution Failed inside system routing layers:', err.message);
        res.status(500).json({ success: false, error: err.message });
    }
});

// 🖨️ API ENDPOINT 9: NATIVE APP PWA LAUNCHER QR CODE FILE STREAM GENERATOR ENGINE
// Intercepts structural target URLs from admin panel and converts parameters into printable attachments
app.get('/api/inventory/qr', async (req, res) => {
    try {
        // Pull target deep link url and text label metadata tokens from request parameters
        const destinationURLText = req.query.url;
        const identifierLabelTag = req.query.label || 'Mesa 3D Node';

        if (!destinationURLText) {
            return res.status(400).send('Fault Error: Missing destination URL target code parameter lines.');
        }

        console.log(`[QR Matrix Matrix Engine] Computing printable scan vector file for: "${identifierLabelTag}"`);
        console.log(`[QR Target Link Path] Compiling path maps to: ${destinationURLText}`);

        // Set high density graphic render options to enforce clean edge scaling on thermal receipt printers
        const qrGenerationOptions = {
            errorCorrectionLevel: 'H',      // Use high error correction margin tolerance block settings
            type: 'image/png',             // Request raw absolute binary image format structure type
            margin: 3,                     // Framed clean border isolation space margin
            color: {
                dark: '#0B0F19',           // High contrast dark palette value matching system hex profiles
                light: '#FFFFFF'           // Clean pure background space values layout tracks
            }
        };

        // Stream generated graphic buffer data straight out onto browser HTTP response pipelines channels headers
        res.setHeader('Content-Type', 'image/png');
        res.setHeader('Content-Disposition', `attachment; filename="Mesa3D-QR-${identifierLabelTag.replace(/\s+/g, '-')}.png"`);

        // Execute official encoder to construct visual binary stream straight onto out response threads
        await QRCode.toFileStream(res, destinationURLText, qrGenerationOptions);

    } catch (qrGenerationFaultException) {
        console.error('Critical Failure within QR Matrix Generator microservice pipeline wrapper:', qrGenerationFaultException.message);
        res.status(500).send('Internal Engine Routing Failure.');
    }
});

// API Endpoint 10: Purges kitchen crew roster entities matching specific employee indexing markers parameters
app.post('/api/chefs/delete', async (req, res) => {
    try {
        const id = req.query.id || req.body.id;
        const restaurant_id = req.query.restaurant_id || req.body.restaurant_id || 1;

        await db.run('DELETE FROM chefs WHERE id = ? AND restaurant_id = ?', [id, restaurant_id]);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// 🟩 SYSTEM FIX: Self-contained referencing loop preventing Express runtime crashes
app.post('/api/feedback', async (req, res) => {
    try {
        const { restaurant_id, order_id, food_rating, service_rating, comments, chef_id } = req.body;
        const rid = restaurant_id || 1;

        const result = await db.run(
            'INSERT INTO feedback (restaurant_id, order_id, food_rating, service_rating, comments, chef_id) VALUES (?, ?, ?, ?, ?, ?)',
            [rid, order_id, food_rating, service_rating, comments, chef_id]
        );

        const feedbackPayload = {
            id: result.lastID,
            order_id: parseInt(order_id),
            food_rating: parseInt(food_rating),
            service_rating: parseInt(service_rating),
            comments: comments,
            chef_id: chef_id
        };

        // Uses the active server reference attached securely to the application context layer
        const socketEngine = req.app.get('io') || io;
        if (socketEngine) {
            socketEngine.to(`workspace_${rid}`).emit('incoming_feedback', feedbackPayload);
        }

        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// Pass the instance mapping configuration right below your Server instantiation (Line 38)
app.set('io', io);

// API Endpoint 12: System Tenants Index Resolver Map List Node - Dispatches baseline system listings data
app.get('/api/tenants', async (req, res) => {
    const listings = await db.all('SELECT id, name FROM restaurants');
    res.json(listings);
});


// =================================================================================
//            REAL-TIME WEBSOCKET (SOCKET.IO) CHANNEL COMMUNICATION CHANNELS
// =================================================================================
io.on('connection', (socket) => {

    // Channel Action 1: Synchronizes panel view terminals to an isolated workspace room container partition
    socket.on('join_workspace', ({ restaurantId }) => {
        const rid = restaurantId || 1;
        // 🟩 FIXED: Changed room name string pattern to 'workspace_' to sync with frontend listeners
        socket.join(`workspace_${rid}`);
        console.log(`[Socket Matrix] Terminal successfully synchronized to pipeline room: workspace_${rid}`);
    });

    // Channel Action 2: Processes incoming check-out transaction rows packages and streams to monitors
    socket.on('place_order', async (orderData) => {
        try {
            const { restaurant_id, location_context, location_identifier, items, total_price } = orderData;
            const rid = restaurant_id || 1;

            // Commit purchase array block data to order transactional table logs rows after flattening to JSON data strings
            const result = await db.run(
                'INSERT INTO orders (restaurant_id, location_context, location_identifier, items, total_price) VALUES (?, ?, ?, ?, ?)',
                [rid, location_context, location_identifier, JSON.stringify(items), total_price]
            );

            // Fetch absolute server localized generation timestamp parameters context
            const serverDateInstance = new Date();

            // 🟩 FIXED ISSUE 5: Enforces a clean calendar timeline check block packet
            const payload = {
                id: result.lastID,
                ...orderData,
                status: 'New',
                estimated_prep_time: 15,
                timestamp: serverDateInstance.toISOString() // Formats real system execution hour maps
            };


            // 🟩 FIXED: Switched room target destination channel address identifier string cleanly
            io.to(`workspace_${rid}`).emit('new_order_alert', payload);
        } catch (err) {
            console.error('Order tracking transaction pipeline stream breakdown failure event trace:', err.message);
        }
    });

    // Channel Action 3: Overrides cooking status indices along production tracks and updates guests devices tracking states
    socket.on('update_order_status', async ({ restaurant_id, order_id, status, chef_id, prep_time }) => {
        try {
            const rid = restaurant_id || 1;

            // Conditional state evaluator checks if the order tracking ticket lifecycle has shifted into active kitchen cooking status
            if (status === 'Cooking') {
                // Link assigned staff cooks tracking tokens and lock temporary down calculated production timeline countdown metrics values
                await db.run('UPDATE orders SET status = ?, assigned_chef_id = ?, estimated_prep_time = ? WHERE id = ?', [status, chef_id, prep_time || 15, order_id]);
            } else {
                // Standard mutation tracking override path parameters logic loop for general state indicators updates
                await db.run('UPDATE orders SET status = ? WHERE id = ?', [status, order_id]);
            }

            // Extract fully compiled relational ticket properties to supply comprehensive analytics records back to client tracking boards
            const completeOrder = await db.get(`
                SELECT o.*, c.name as chef_name, c.specialties as chef_specialties, c.photo_url as chef_photo
                FROM orders o LEFT JOIN chefs c ON o.assigned_chef_id = c.id WHERE o.id = ?
            `, [order_id]);

            // 🟩 FIXED: Synchronize status tracker shifts down onto administrative channels matching front dashboard parameters context
            io.to(`workspace_${rid}`).emit('order_pipeline_update', completeOrder);
        } catch (err) {
            console.error('Status override state tracking transformation failure event log trace error:', err.message);
        }
    });

    // Channel Action 4: Real-time Multi-Checklist Service Bell Runner Dispatch Module Pipeline Hub
    socket.on('call_waiter', ({ restaurant_id, location_context, location_identifier }) => {
        const rid = restaurant_id || 1;

        // 🟩 FIXED: Dispatches staff pager notifications cleanly to the active workspace channel partition
        io.to(`workspace_${rid}`).emit('waiter_alert', {
            location_context,
            location_identifier,
            timestamp: new Date(),
            id: Date.now() // Added generation ID signature token to assist task node lifecycle deletion routines
        });
    });
});


// =================================================================================
//                        CORE SERVER BOOTSTRAPPING ENGINE BLOCK
// =================================================================================
// Bind application runtime engine pipelines to evaluate ambient infrastructure cloud environment port matrices parameters
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`=================================================================================`);
    console.log(` MESA 3D ENGINE CORE MODULE: Infrastructure listening effectively on web port: ${PORT}`);
    console.log(`=================================================================================`);
});
