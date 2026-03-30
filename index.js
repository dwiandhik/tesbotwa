const { //aa
    default: makeWASocket, 
    useMultiFileAuthState, 
    DisconnectReason,
    fetchLatestBaileysVersion,
    jidNormalizedUser 
} = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const session = require('express-session');
const bodyParser = require('body-parser');
const QRCode = require('qrcode');
const pino = require('pino');
const fs = require('fs-extra');
const path = require('path');
const multer = require('multer');
const { GoogleSpreadsheet } = require('google-spreadsheet');
const { JWT } = require('google-auth-library');
const cron = require('node-cron');

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const port = 3777;

// --- CONFIGURATION ---
const ADMIN_USER = "admin";
const ADMIN_PASS = "admin123"; 
const OWNER_NUMBER = "6285293958886"; // Ganti dengan nomor WhatsApp Anda (format 62, tanpa + atau spasi)
const ID_GRUP_REMINDER = "120363405900078596@g.us"; 

const SPREADSHEET_ID_1 = '1ydTqcG1Vg9EZFJ2boOjCekL2Xw4RYOe-haHCXxgmlX0'; // XL
const SPREADSHEET_ID_2 = '1BLFz1ogR_bbo-Vulfu9US9Jtte2b9pfGJd8dlRyA4c8'; // TELKOMSEL

const GOOGLE_EMAIL = 'bot-wa-sheets@unique-caldron-480115-f4.iam.gserviceaccount.com';
const GOOGLE_KEY = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCr0bhMd/YduL5h\nhP9f1Iswn12LXBE+eWIw2JWJoiJf5FIETiUldzIRF+7/gLRzenvvrdwT6pixHav5\nqad/YQ3XVnXoia1VTaNeGvKZ4Ruo7/9Ew2QkA9E9M0arDKGjYngI5x6APfLdNl+L\nIYhYl+Gui2mcVlK1/a4PrlCzy14NecUA8IFLL+a9Lk1fjCsNOwO3s0kApStdZ3pv\nTtFZa+rkdQAdLeI1o8l2s36FjDgWK9j5wMaNjb3flwIV9mrPS9j/BTKmL8L5UYTq\nNKAYcjg/iXnJTnB/y4JDxceIzzeVUCPcfiUgOFsfclJotPZu3YbM86NZKkwy2u+i\nQcIdDmGLAgMBAAECggEABWUFSKlaOd6rY5EdxNrTL/NLqEImFx8P3SwVAzLq/2Bs\nzs52o3GASexwHZI4mDOX9pOFMxQ3jRVlvpCmvfzEcXwOdv7znpexxxTzhFn+rf7d\ntev96z+Pb0DP5DOg/j9A446rdNKnxcLyRwJbhI89j7xi1CdwW18b+vQPJptTraVA\nQ4SAOFNzq2XB2o2lBRVQemlzeFfNv/VsNF08YkpJxxojYRTH9MauIfMaG4V8cBwG\n+f6o8bAg6SFgpR3pjY5sUMLaJIJp30tsrdx6f2zxHKD+QG6UWReiMi49YVRZE6Kp\n6mxLNiY+8p1bdWB99qujjSC8moDkDfGHQVfRuH0pQQKBgQDZeRqbbEZJpJDwLeTJ\nYs2fHP1MOtteVed28fX/OSDWRRFoSc/LJQClpyK0rn8Yau2t28j1ZH/G46yEB5sF\ngphQ5MjZxb8aqGahla2thK0xQ6BwBGyhnYTQEkjKh2IixWDYmXBQRtqOAgMSayp0\nkBAMkHjf6R88lV5ZZCNOI+Ul4QKBgQDKQhxZI9YqExZi63bp491LbJADw+3ztbKr\nuOLI0Ave0+bUgwS3MqWuVAk2f843peBu9fPNTT/ne7GKEAeKhg79sOuS5Bpu9OXq\nLcYQKG3+81qu77N4lQ9BSoYgBObp7vIg3iuxyuCr32XJFbmI0TVLB6a27M8mJ43F\nyMnYK5Ac6wKBgCJ76ZQrnxmeVr0/CcBFQoWwexnTW2WiCYn65B8MWACiAxieW6zC\nuU+LKR2tbcnZasbeywbeYMSQ5ZIqApLlGnH5VT/y81Ku0Vdd/KQ/HZdqOc6JZRBb\gevcel5jCVSqJ6kw63ZPReiuuP36sEi7b1AOuJwIw0NADC3wiwig+H0BAoGBALdY\noLwJqW2wqd14F+7EGenyesZ/CSigsFvMmQBy4B/ZtWk1b8PmTJywHz3hM33sh3vu\nx1h3S5O65GEEUOG4zsQYaiRZVMD6jaTwY5hoHfY8ghsMvYN0lESamuVRrEWpzqIO\n5EsiXvJO68USRYMKKyZdxDoUh1/OAU2my5qDOvuLAoGBAIyCqWN6q1o1j+fEFBL+\noJYtZFCOAQdJ+YQ3b2xtPVpQBc3gvaCdLfs0SOQNEincGeSXdcMiFKXT0uCzjvOq\nRRlhqZ8SHikUrS8PkFU6lNbH5WWZSVeU8XGYQ1lFRmRgDvtkaje19847cpGhNP9j\nk/PyEYQGn/D3vu3Utw7Z2RtA\n-----END PRIVATE KEY-----\n";

let botStatus = "Offline";
let botSock = null;
let taskXL = null;
let taskTsel = null;
let taskMasterList = null; // Tambahkan variabel untuk cron Master List
const activeBroadcasts = new Map(); // Map untuk menyimpan AbortController broadcast aktif
const cooldowns = new Map();

// --- DATABASE & SETTINGS ---
const settingsPath = './settings.json';
const defaultSettings = {
    timeXL: "08:00",
    timeTsel: "09:00",
    timeMasterList: "07:00"
};

if (!fs.existsSync(settingsPath)) {
    fs.writeFileSync(settingsPath, JSON.stringify(defaultSettings, null, 2));
}
const getSettings = () => {
    try {
        const savedSettings = JSON.parse(fs.readFileSync(settingsPath, 'utf-8'));
        // Gabungkan dengan pengaturan default untuk memastikan semua kunci ada
        return { ...defaultSettings, ...savedSettings };
    } catch (e) {
        console.error("Gagal membaca settings.json, menggunakan pengaturan default.", e);
        return defaultSettings;
    }
};

const dbPath = './database.json';
if (!fs.existsSync(dbPath)) fs.writeFileSync(dbPath, '[]');
const getDB = () => JSON.parse(fs.readFileSync(dbPath, 'utf-8'));
const saveDB = (data) => fs.writeFileSync(dbPath, JSON.stringify(data, null, 2));

const ownerDbPath = './owner_database.json';
if (!fs.existsSync(ownerDbPath)) fs.writeFileSync(ownerDbPath, '[]');
const getOwnerDB = () => JSON.parse(fs.readFileSync(ownerDbPath, 'utf-8'));
const saveOwnerDB = (data) => fs.writeFileSync(ownerDbPath, JSON.stringify(data, null, 2));


const logPath = './activity_log.json';
if (!fs.existsSync(logPath)) fs.writeFileSync(logPath, '[]');
function logActivity(logEntry) {
    const logs = JSON.parse(fs.readFileSync(logPath, 'utf-8'));
    logs.unshift({ ...logEntry, timestamp: new Date().toISOString() });
    if (logs.length > 500) { // Keep the log to the last 500 entries
        logs.splice(500);
    }
    fs.writeFileSync(logPath, JSON.stringify(logs, null, 2));
}

const broadcastHistoryPath = './broadcast_history.json';
if (!fs.existsSync(broadcastHistoryPath)) fs.writeFileSync(broadcastHistoryPath, '[]');
const getBroadcastHistory = () => JSON.parse(fs.readFileSync(broadcastHistoryPath, 'utf-8'));
const saveBroadcastHistory = (data) => fs.writeFileSync(broadcastHistoryPath, JSON.stringify(data, null, 2));




const serviceAccountAuth = new JWT({
    email: GOOGLE_EMAIL,
    key: GOOGLE_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});

// Helper to parse various date formats from sheets
function parseSheetDate(dateString) {
    if (!dateString || typeof dateString !== 'string') return null;
    // Supports DD/MM/YYYY, D/M/YYYY, and separators -, .
    const match = dateString.trim().match(/^(\d{1,2})([\/\-.])(\d{1,2})\2(\d{4})$/);
    if (!match) return null;

    const day = parseInt(match[1], 10);
    const month = parseInt(match[3], 10);
    const year = parseInt(match[4], 10);

    const date = new Date(year, month - 1, day);
    // Verify it's a valid date (e.g., not 30/02/2026)
    if (date.getFullYear() === year && date.getMonth() === month - 1 && date.getDate() === day) {
        date.setHours(0, 0, 0, 0);
        return date;
    }
    return null;
}

// --- SCAN LOGIC ---
async function scanProvider(id, sheetName, label, dateKey) {
    let list = [];
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    try {
        const doc = new GoogleSpreadsheet(id, serviceAccountAuth);
        await doc.loadInfo();
        const sheet = doc.sheetsByTitle[sheetName];
        if (sheet) {
            const rows = await sheet.getRows();
            rows.forEach(row => {
                const expDateString = row.get(dateKey);
                const expDate = parseSheetDate(expDateString);
                
                if (expDate && expDate.getTime() === today.getTime()) {
                    list.push({ 
                        nama: row.get('Nama'), 
                        nomor: row.get('Nomor'), 
                        exp: expDateString, 
                        ket: row.get('Keterangan') || '-', 
                        provider: label 
                    });
                }
            });
        } else {
            console.log(`[Reminder Scan] Sheet "${sheetName}" tidak ditemukan di spreadsheet ${label}.`);
        }
    } catch (e) { 
        console.error(`[Reminder Scan] Error saat memindai ${label} (${sheetName}):`, e.message); 
        if (botSock) {
            try {
                // Notify group about the scan failure to avoid silent errors
                await botSock.sendMessage(ID_GRUP_REMINDER, { text: `⚠️ Gagal menjalankan reminder untuk ${label}.\n\nError: ${e.message}` });
            } catch (sendError) {
                console.error(`[Reminder Scan] Gagal mengirim notifikasi error ke grup:`, sendError.message);
            }
        }
    }
    return list;
}

// --- HELPER for UPDATE DATA ---
function columnToLetter(column) {
	let temp;
	let letter = "";
	let col = column;
	while (col > 0) {
		temp = (col - 1) % 26;
		letter = String.fromCharCode(temp + 65) + letter;
		col = (col - temp - 1) / 26;
	}
	return letter;
}

// --- HELPER for BROADCAST ---
async function getNumbersFromSheet(sheetType) {
    const config = sheetUpdateConfig[sheetType];
    if (!config) return [];

    const { spreadsheetId, sheetName } = config;
    const doc = new GoogleSpreadsheet(spreadsheetId, serviceAccountAuth);
    try {
        await doc.loadInfo();
        const sheet = doc.sheetsByTitle[sheetName];
        if (!sheet) {
            console.error(`[Broadcast] Sheet "${sheetName}" tidak ditemukan.`);
            return [];
        }
        const rows = await sheet.getRows();
        // Assuming the column with numbers is named 'Nomor'
        return rows.map(row => row.get('Nomor')).filter(Boolean);
    } catch (e) {
        console.error(`[Broadcast] Gagal mengambil nomor dari sheet ${sheetName}:`, e.message);
        return [];
    }
}

// --- UPDATE DATA LOGIC ---
const sheetUpdateConfig = {
    'MASTER_LIST': {
        spreadsheetId: SPREADSHEET_ID_1,
        sheetName: 'Master List Update',
        dateKey: 'EXP',
        label: 'Master List Update'
    },
    'AKRAB': {
        spreadsheetId: SPREADSHEET_ID_1,
        sheetName: 'Akrab List Update',
        dateKey: 'EXP',
        label: 'Akrab List Update'
    },
    'TSEL_UPDATE': {
        spreadsheetId: SPREADSHEET_ID_2,
        sheetName: 'Tsel Update',
        dateKey: 'Exp', // Menggunakan 'Exp' sesuai dengan format di sheet Telkomsel
        label: 'Telkomsel (Tsel Update)'
    }
};

async function updateSheetData(sheetType, updates, io, socketId) {
    const config = sheetUpdateConfig[sheetType];
    if (!config) throw new Error(`Tipe sheet tidak valid: ${sheetType}`);
    const { spreadsheetId, sheetName, dateKey } = config;
    const doc = new GoogleSpreadsheet(spreadsheetId, serviceAccountAuth);
    try {
        await doc.loadInfo();
    } catch (e) {
        console.error(`Error loading spreadsheet info for ID ${spreadsheetId}:`, e);
        throw new Error(`Gagal memuat spreadsheet: ${e.message}`);
    }
    const trimmedSheetName = sheetName.trim();
    const sheet = doc.sheetsByTitle[trimmedSheetName];

    if (!sheet) {
        const errorMsg = `Sheet "${trimmedSheetName}" tidak ditemukan. Sheet yang tersedia: ${Object.keys(doc.sheetsByTitle).join(', ')}`;
        console.error(errorMsg);
        throw new Error(errorMsg);
    }
    let rows;
    try {
        rows = await sheet.getRows();
    } catch (e) {
        console.error(`Error getting rows from sheet "${sheetName}":`, e);
        throw new Error(`Gagal mendapatkan baris dari sheet: ${e.message}`);
    }

    const results = [];

    const dateKeyColumnIndex = sheet.headerValues.indexOf(dateKey);
    if (dateKeyColumnIndex === -1) {
        const errorMsg = `Kolom header "${dateKey}" tidak ditemukan di sheet "${sheetName}".`;
        console.error(errorMsg);
        throw new Error(errorMsg);
    }
    const dateKeyColumnLetter = columnToLetter(dateKeyColumnIndex + 1);

    for (const update of updates) {
        const { phoneNumber, newExpDate } = update;
        // The date is already in DD/MM/YYYY format from the frontend
        const formattedDate = newExpDate;
        const rowToUpdate = rows.find(row => row.get('Nomor') === phoneNumber);
        let status = 'Gagal (Tidak Ditemukan)';
        let error = null;

        if (rowToUpdate) {
            try {
                // Bypassing row.save() due to a potential library bug causing data mismatch.
                // Instead, we update the specific cell directly using the API.
                const cellA1 = `${dateKeyColumnLetter}${rowToUpdate.rowNumber}`;
                const range = `'${sheet.title}'!${cellA1}`;

                await doc.sheetsApi.put(`values/${encodeURIComponent(range)}`, {
                    searchParams: {
                        valueInputOption: 'USER_ENTERED'
                    },
                    json: {
                        range: range,
                        majorDimension: 'ROWS',
                        values: [[formattedDate]]
                    }
                });
                status = 'Berhasil';
            } catch (e) {
                console.error(`Failed to update row for ${phoneNumber}:`, e);
                // Berikan status yang lebih spesifik jika error izin
                if (e.message && (e.message.includes('403') || e.message.toLowerCase().includes('permission'))) {
                    status = 'Gagal (Izin Ditolak)';
                } else {
                    status = 'Gagal (Error Simpan)';
                }
                error = e.message;
            }
        }
        
        if (io && socketId) {
            io.to(socketId).emit('update_progress', {
                message: `Memproses ${results.length + 1} dari ${updates.length}...<br>Nomor: ${phoneNumber}, Status: ${status}`
            });
        }
        results.push({ phoneNumber, status, error });
    }

    return results;
}

// --- CRON SETUP ---
function setupReminders() {
    if (taskXL) taskXL.stop();
    if (taskTsel) taskTsel.stop();
    if (taskMasterList) taskMasterList.stop(); // Hentikan task Master List yang mungkin sudah ada
    
    const s = getSettings();
    
    // Master List Cron
    const [h0, m0] = s.timeMasterList.split(':');
    taskMasterList = cron.schedule(`${m0} ${h0} * * *`, async () => {
        const data = await scanProvider(SPREADSHEET_ID_1, 'Master List Update', 'Master List', 'EXP');
        if (data.length > 0 && botSock) sendWA(data, "Master List");
    }, { scheduled: true, timezone: "Asia/Jakarta" });

    // XL Cron
    const [h1, m1] = s.timeXL.split(':');
    taskXL = cron.schedule(`${m1} ${h1} * * *`, async () => {
        const data = await scanProvider(SPREADSHEET_ID_1, 'Akrab List Update', 'XL', 'EXP');
        if (data.length > 0 && botSock) sendWA(data, "XL (Akrab List Update)");
    }, { scheduled: true, timezone: "Asia/Jakarta" });

    // Tsel Cron
    const [h2, m2] = s.timeTsel.split(':');
    taskTsel = cron.schedule(`${m2} ${h2} * * *`, async () => {
        const data = await scanProvider(SPREADSHEET_ID_2, 'Tsel Update', 'TELKOMSEL (Tsel Update)', 'Exp');
        if (data.length > 0 && botSock) sendWA(data, "TELKOMSEL (Tsel Update)");
    }, { scheduled: true, timezone: "Asia/Jakarta" });
    
    console.log(`Cron Updated: Master List(${s.timeMasterList}), XL(${s.timeXL}), Tsel(${s.timeTsel})`);
}

async function sendWA(data, label) {
    let msg = `⏰ *REMINDER ${label} HARI INI*\n\n`;
    data.forEach((item, i) => {
        msg += `${i+1}. *${item.nama}*\n   📱 ${item.nomor}\n   🗓️ Exp: ${item.exp}\n   📝 Ket: ${item.ket}\n\n`;
    });
    await botSock.sendMessage(ID_GRUP_REMINDER, { text: msg });
}
setupReminders();

// --- BAILEYS CORE ---
async function startBot() {
    const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');
    const { version } = await fetchLatestBaileysVersion();
    botSock = makeWASocket({ 
        version, 
        auth: state, 
        logger: pino({ level: 'error' }), 
        browser: ["DW Cellkom", "Chrome", "1.0.0"] 
    });
    
    botSock.ev.on('creds.update', saveCreds);
    
    botSock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;
        if (qr) io.emit('qr_update', { qr: await QRCode.toDataURL(qr), status: "Menunggu Scan..." });
        if (connection === 'open') { 
            botStatus = "Online (Terhubung)"; 
            io.emit('status_update', { status: botStatus, connected: true }); 
        } else if (connection === 'close') { 
            const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
            const shouldReconnect = reason !== DisconnectReason.loggedOut && reason !== DisconnectReason.connectionReplaced;

            botStatus = "Offline";
            io.emit('status_update', { status: botStatus, connected: false });

            if (shouldReconnect) {
                startBot();
            } else if (reason === DisconnectReason.loggedOut) {
                stopBot();
            }
        }
    });

    botSock.ev.on('messages.upsert', async m => {
        const msg = m.messages[0];
        if (!msg.message || msg.key.fromMe) return;

        const from = jidNormalizedUser(msg.key.remoteJid);
        if (from.endsWith('@g.us')) return; // Abaikan pesan dari grup untuk auto-reply

        const isFromOwner = from.startsWith(OWNER_NUMBER);
        const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || "").toLowerCase().trim();
        const userWords = text.split(/\s+/);

        console.log(`[Pesan Masuk] Dari: ${from}, Apakah Owner: ${isFromOwner}, Teks: "${text}"`);
        console.log(`[Pesan Masuk] Kata-kata di pesan: ${JSON.stringify(userWords)}`);


        if (text === '.cek') {
            if (!isFromOwner) return;
            const d0 = await scanProvider(SPREADSHEET_ID_1, 'Master List Update', 'Master List', 'EXP');
            const d1 = await scanProvider(SPREADSHEET_ID_1, 'Akrab List Update', 'XL', 'EXP'); 
            const d2 = await scanProvider(SPREADSHEET_ID_2, 'Tsel Update', 'TELKOMSEL (Tsel Update)', 'Exp');
            const all = [...d0, ...d1, ...d2];
            if (all.length === 0) return await botSock.sendMessage(from, { text: "✅ Aman, tidak ada data habis hari ini." });
            let res = `📢 *LAPORAN CEK MANUAL*\n\n`;
            all.forEach((i, idx) => res += `${idx+1}. [${i.provider}] ${i.nama} - ${i.ket}\n`);
            await botSock.sendMessage(from, { text: res });
            return;
        }
        
        let findKey = null;

        // 1. Cari di database publik
        findKey = getDB().find(item => {
            const keywords = item.key.split(',').map(k => k.trim().toLowerCase()).filter(Boolean);
            const match = keywords.some(kw => userWords.includes(kw));
            if (match) console.log(`[Pencarian] Cocok di DB Publik dengan kata kunci: "${item.key}"`);
            return match;
        });

        // 2. Jika tidak ketemu dan pengirim adalah owner, cari di database owner
        if (!findKey && isFromOwner) {
            console.log('[Pencarian] Tidak cocok di DB Publik, mencoba DB Owner...');
            findKey = getOwnerDB().find(item => {
                const keywords = item.key.split(',').map(k => k.trim().toLowerCase()).filter(Boolean);
                const match = keywords.some(kw => userWords.includes(kw));
                if (match) console.log(`[Pencarian] Cocok di DB Owner dengan kata kunci: "${item.key}"`);
                return match;
            });
        }

        if (findKey) {
            const now = Date.now();
            if (cooldowns.has(from) && (now < cooldowns.get(from) + 3000)) return;
            cooldowns.set(from, now);
            console.log(`[Balasan] Mengirim balasan untuk kata kunci: "${findKey.key}"`);
            if (findKey.image && fs.existsSync(findKey.image)) {
                await botSock.sendMessage(from, { image: { url: findKey.image }, caption: findKey.response });
            } else { await botSock.sendMessage(from, { text: findKey.response }); }
        }
    });
}

function stopBot() {
    if (botSock) { botSock.end(); botSock = null; }
    botStatus = "Offline";
    io.emit('status_update', { status: botStatus, connected: false });
    if (fs.existsSync('./auth_info_baileys')) fs.removeSync('./auth_info_baileys');
}

// --- EXPRESS SERVER ---
const storage = multer.diskStorage({
    destination: (req, file, cb) => { if (!fs.existsSync('./uploads')) fs.mkdirSync('./uploads'); cb(null, './uploads'); },
    filename: (req, file, cb) => cb(null, Date.now() + path.extname(file.originalname))
});
const upload = multer({ storage });

app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.json()); // Tambahkan ini untuk mengurai JSON request body
app.use(session({ secret: 'dw-secret-key', resave: false, saveUninitialized: true }));
app.use('/uploads', express.static('uploads'));

const checkAuth = (req, res, next) => { if (req.session.loggedIn) next(); else res.redirect('/login'); };

app.get('/login', (req, res) => res.sendFile(path.join(__dirname, 'login.html')));
app.post('/login', (req, res) => {
    if (req.body.user === ADMIN_USER && req.body.pass === ADMIN_PASS) { req.session.loggedIn = true; res.redirect('/'); }
    else res.send("Akses Ditolak.");
});

app.get('/', checkAuth, (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/owner-keywords', checkAuth, (req, res) => res.sendFile(path.join(__dirname, 'owner-keywords.html')));
app.get('/reminder', checkAuth, (req, res) => res.sendFile(path.join(__dirname, 'reminder.html')));
app.get('/update', checkAuth, (req, res) => res.sendFile(path.join(__dirname, 'update.html')));
app.get('/broadcast', checkAuth, (req, res) => res.sendFile(path.join(__dirname, 'broadcast.html')));
app.get('/logs', checkAuth, (req, res) => res.sendFile(path.join(__dirname, 'logs.html')));

app.get('/api/status', (req, res) => res.json({ status: botStatus, connected: botStatus === "Online (Terhubung)", settings: getSettings() }));
app.get('/api/keywords', checkAuth, (req, res) => res.json(getDB()));
app.get('/api/owner-keywords', checkAuth, (req, res) => res.json(getOwnerDB()));

app.get('/api/owner-keyword/:index', checkAuth, (req, res) => {
    const db = getOwnerDB();
    const keyword = db[req.params.index];
    if (keyword) {
        res.json(keyword);
    } else {
        res.status(404).json({ message: 'Keyword not found' });
    }
});

app.get('/api/keyword/:index', checkAuth, (req, res) => {
    const db = getDB();
    const keyword = db[req.params.index];
    if (keyword) {
        res.json(keyword);
    } else {
        res.status(404).json({ message: 'Keyword not found' });
    }
});

app.get('/api/logs', checkAuth, (req, res) => {
    const logs = JSON.parse(fs.readFileSync(logPath, 'utf-8'));
    res.json(logs);
});

app.post('/api/settings/reminder', checkAuth, (req, res) => {
    const s = getSettings();
    s.timeXL = req.body.timeXL || s.timeXL;
    s.timeMasterList = req.body.timeMasterList || s.timeMasterList; // Simpan waktu reminder Master List
    s.timeTsel = req.body.timeTsel || s.timeTsel;
    fs.writeFileSync(settingsPath, JSON.stringify(s));
    setupReminders();
    res.redirect('/reminder');
});

app.get('/api/check-now', checkAuth, async (req, res) => {
    const d0 = await scanProvider(SPREADSHEET_ID_1, 'Master List Update', 'Master List', 'EXP'); // Tambahkan scan Master List
    const d1 = await scanProvider(SPREADSHEET_ID_1, 'Akrab List Update', 'XL', 'EXP'); 
    const d2 = await scanProvider(SPREADSHEET_ID_2, 'Tsel Update', 'TELKOMSEL (Tsel Update)', 'Exp');
    res.json([...d0, ...d1, ...d2]); // Gabungkan semua hasil scan
});

app.post('/api/update-data', checkAuth, async (req, res) => {
    const { updates, sheetType, socketId } = req.body;
    if (!Array.isArray(updates) || updates.length === 0) {
        return res.status(400).json({ success: false, message: 'Data update tidak valid.' });
    }
    if (!socketId) {
        return res.status(400).json({ success: false, message: 'Koneksi sesi tidak ditemukan.' });
    }

    // Immediately respond to the HTTP request
    res.status(202).json({ success: true, message: 'Proses update diterima. Progres akan ditampilkan...' });

    // Perform the long-running task in the background
    (async () => {
        try {
            const updateResults = await updateSheetData(sheetType, updates, io, socketId);

            updateResults.forEach(result => {
                const originalUpdate = updates.find(u => u.phoneNumber === result.phoneNumber);
                if (!originalUpdate) return;
                const formattedDate = originalUpdate.newExpDate;
                logActivity({
                    type: 'update',
                    phoneNumber: result.phoneNumber,
                    sheetType: sheetType,
                    newExpDate: formattedDate,
                    status: result.status,
                    error: result.error || undefined
                });
            });
            const successfulUpdates = updateResults.filter(r => r.status === 'Berhasil');
            const failedUpdates = updateResults.filter(r => r.status !== 'Berhasil');

            let message = '';
            if (successfulUpdates.length > 0) {
                const sheetLabel = sheetUpdateConfig[sheetType]?.label || sheetType;
                message += `Berhasil memperbarui ${successfulUpdates.length} nomor di sheet "${sheetLabel}".`;
            }
            if (failedUpdates.length > 0) {
                if (message) message += ' ';
                const failedMessages = failedUpdates.map(f => `${f.phoneNumber} (${f.status})`).join(', ');
                message += `Gagal memperbarui ${failedUpdates.length} nomor: ${failedMessages}.`;
            }
            if (message === '') message = 'Tidak ada nomor yang berhasil diperbarui atau ditemukan.';
            
            io.to(socketId).emit('update_complete', { success: true, allSucceeded: failedUpdates.length === 0, message });
        } catch (error) {
            console.error('Error in /api/update-data background task:', error);
            logActivity({ type: 'update', status: 'Error Server', message: error.message, sheetType });
            io.to(socketId).emit('update_complete', { success: false, allSucceeded: false, message: `Terjadi kesalahan pada server: ${error.message}` });
        }
    })();
});

app.post('/api/broadcast', checkAuth, upload.single('image'), async (req, res) => {
    const { message, target, manualNumbers, delay, socketId, sendToNewOnly } = req.body;
    const imagePath = req.file ? req.file.path : null; // Dapatkan path gambar jika diunggah

    if ((!message && !imagePath) || !delay || !socketId) {
        return res.status(400).json({ success: false, message: 'Parameter tidak lengkap.' });
    }
    if (!botSock) {
        if (imagePath && fs.existsSync(imagePath)) fs.removeSync(imagePath); // Clean up uploaded image if bot is offline
        return res.status(400).json({ success: false, message: 'WhatsApp tidak terhubung. Silakan hubungkan bot terlebih dahulu.' });
    }

    // Immediately respond to the HTTP request
    res.status(202).json({ success: true, message: 'Proses broadcast diterima.' });

    // Perform the long-running task in the background
    (async () => {
        const abortController = new AbortController();
        activeBroadcasts.set(socketId, abortController); // Simpan controller untuk socket ini
        const signal = abortController.signal;

        try {
            const broadcastHistory = new Set(getBroadcastHistory());
            let targetNumbers = [];

            // 1. Get numbers from manual input
            if (manualNumbers) {
                const manualList = manualNumbers.split('\n').map(n => n.trim()).filter(Boolean);
                targetNumbers.push(...manualList);
            }

            // 2. Get numbers from Google Sheet if a target is selected
            if (target !== 'manual') {
                io.to(socketId).emit('broadcast_progress', { message: `Mengambil nomor dari sheet "${sheetUpdateConfig[target]?.label || target}"...` });
                const sheetNumbers = await getNumbersFromSheet(target);
                targetNumbers.push(...sheetNumbers);
            }

            // 3. Deduplicate numbers
            let uniqueNumbers = [...new Set(targetNumbers)];

            // 4. Filter for new numbers if sendToNewOnly is true
            if (sendToNewOnly === 'true') {
                io.to(socketId).emit('broadcast_progress', { message: `Memfilter nomor yang belum pernah dihubungi...` });
                uniqueNumbers = uniqueNumbers.filter(num => !broadcastHistory.has(num.replace(/\D/g, '')));
            }

            if (uniqueNumbers.length === 0) {
                io.to(socketId).emit('broadcast_complete', { success: false, message: 'Tidak ada nomor target yang valid ditemukan.' });
                return;
            }

            io.to(socketId).emit('broadcast_progress', { message: `Ditemukan ${uniqueNumbers.length} nomor unik. Memulai pengiriman...` });

            let successCount = 0;
            let failCount = 0;
            const failedNumbers = [];
            let isCancelled = false;

            if (signal.aborted) { // Check if cancelled before starting the loop
                isCancelled = true;
            }

            for (let i = 0; i < uniqueNumbers.length; i++) {
                const number = uniqueNumbers[i];
                const jid = `${number.replace(/\D/g, '')}@s.whatsapp.net`;
                let result;

                try {
                    if (signal.aborted) { // Check for cancellation inside the loop
                        isCancelled = true;
                        io.to(socketId).emit('broadcast_progress', { message: `\n<b>Broadcast dibatalkan oleh pengguna.</b>` });
                        break; // Exit the loop
                    }
                    [result] = await botSock.onWhatsApp(jid);
                    if (result?.exists) {
                        if (imagePath && fs.existsSync(imagePath)) {
                            await botSock.sendMessage(jid, { image: { url: imagePath }, caption: message || '' }); // Kirim gambar dengan caption
                        } else {

                            await botSock.sendMessage(jid, { text: message });
                        }
                        successCount++;
                        io.to(socketId).emit('broadcast_progress', { message: `(${i + 1}/${uniqueNumbers.length}) Berhasil: ${number}` });
                    } else {
                        failCount++;
                        failedNumbers.push({ number, reason: 'No WA' });
                        io.to(socketId).emit('broadcast_progress', { message: `(${i + 1}/${uniqueNumbers.length}) Gagal (No WA): ${number}` });

                    }
                } catch (error) {
                    failCount++;
                    failedNumbers.push({ number, reason: 'Error' });
                    console.error(`[Broadcast] Error mengirim ke ${number}:`, error);
                    io.to(socketId).emit('broadcast_progress', { message: `(${i + 1}/${uniqueNumbers.length}) Gagal (Error): ${number}` });
                }

                if (i < uniqueNumbers.length - 1) {
                    await new Promise(resolve => setTimeout(resolve, delay * 1000));
                }

                // Add successfully sent numbers to history
                if (result?.exists) {
                    broadcastHistory.add(number.replace(/\D/g, ''));
                    saveBroadcastHistory([...broadcastHistory]);
                }
            }

            let summaryMessage;
            if (isCancelled) {
                summaryMessage = `Broadcast Dibatalkan. Berhasil: ${successCount}, Gagal: ${failCount}.`;
                io.to(socketId).emit('broadcast_complete', { success: false, message: summaryMessage }); // Emit as complete but with cancelled status
            } else {
                summaryMessage = `Broadcast Selesai. Berhasil: ${successCount}, Gagal: ${failCount}.`;
                io.to(socketId).emit('broadcast_complete', { success: true, message: summaryMessage });
            }

            // Log the broadcast activity
            logActivity({
                type: 'broadcast',
                message: message ? (message.substring(0, 50) + (message.length > 50 ? '...' : '')) : '[Hanya Gambar]',
                image: !!imagePath,
                target: target,
                manualCount: manualNumbers ? manualNumbers.split('\n').map(n => n.trim()).filter(Boolean).length : 0,
                total: uniqueNumbers.length,
                success: successCount,
                failed: failCount,
                status: summaryMessage,
                failedDetails: failedNumbers.map(f => `${f.number} (${f.reason})`).join(', ')
            });
        } catch (error) {
            console.error('Error in /api/broadcast background task:', error);
            io.to(socketId).emit('broadcast_complete', { success: false, message: `Terjadi kesalahan pada server: ${error.message}` });
            // Also log this error
            logActivity({ type: 'broadcast', status: 'Error Server', message: `Terjadi kesalahan pada server: ${error.message}` });
        }
    })();
});

app.post('/api/keywords', checkAuth, upload.single('image'), (req, res) => {
    const db = getDB();
    db.push({ 
        key: req.body.key.toLowerCase(), 
        response: req.body.response, 
        image: req.file ? req.file.path : null,
    });
    saveDB(db); res.redirect('/');
});

app.post('/api/owner-keywords', checkAuth, upload.single('image'), (req, res) => {
    const db = getOwnerDB();
    db.push({ 
        key: req.body.key.toLowerCase(), 
        response: req.body.response, 
        image: req.file ? req.file.path : null
    });
    saveOwnerDB(db); res.redirect('/owner-keywords');});

app.post('/api/keywords/edit/:index', checkAuth, upload.single('image'), (req, res) => {
    const db = getDB();
    const index = req.params.index;
    const keyword = db[index];

    if (keyword) {
        keyword.key = req.body.key.toLowerCase();
        keyword.response = req.body.response;
        if (req.file) {
            if (keyword.image && fs.existsSync(keyword.image)) {
                fs.removeSync(keyword.image);
            }
            keyword.image = req.file.path;
        }
        db[index] = keyword;
        saveDB(db);
    }
    res.redirect('/');
});

app.post('/api/owner-keywords/edit/:index', checkAuth, upload.single('image'), (req, res) => {
    const db = getOwnerDB();
    const index = req.params.index;
    const keyword = db[index];

    if (keyword) {
        keyword.key = req.body.key.toLowerCase();
        keyword.response = req.body.response;
        if (req.file) {
            if (keyword.image && fs.existsSync(keyword.image)) {
                fs.removeSync(keyword.image);
            }
            keyword.image = req.file.path;
        }
        db[index] = keyword;
        saveOwnerDB(db);
    }
    res.redirect('/owner-keywords');
});
app.get('/api/keywords/delete/:index', checkAuth, (req, res) => {
    const db = getDB();
    const toDelete = db[req.params.index];
    if (toDelete?.image && fs.existsSync(toDelete.image)) {
        fs.removeSync(toDelete.image);
    }
    db.splice(req.params.index, 1);
    saveDB(db); res.redirect('/');
});

app.get('/api/owner-keywords/delete/:index', checkAuth, (req, res) => {
    const db = getOwnerDB();
    const toDelete = db[req.params.index];
    if (toDelete?.image && fs.existsSync(toDelete.image)) {
        fs.removeSync(toDelete.image);
    }
    db.splice(req.params.index, 1);
    saveOwnerDB(db); res.redirect('/owner-keywords');
});

app.get('/start', checkAuth, (req, res) => { startBot(); res.redirect('/'); });
app.get('/stop', checkAuth, (req, res) => { stopBot(); res.redirect('/'); });

if (fs.existsSync('./auth_info_baileys')) startBot();
server.listen(port, '0.0.0.0', () => console.log(`🚀 Server on port ${port}`));
