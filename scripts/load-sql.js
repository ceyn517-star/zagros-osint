/**
 * scripts/load-sql.js
 *
 * Railway deployment sırasında Google Drive'dan SQL dosyalarını indirir,
 * gzip ile sıkıştırır ve MySQL'e yükler.
 *
 * Kullanım:
 *   node scripts/load-sql.js
 *
 * Gerekli ortam değişkenleri:
 *   MYSQL_URL veya (MYSQLHOST, MYSQLPORT, MYSQLUSER, MYSQLPASSWORD, MYSQLDATABASE)
 *   RAILWAY_VOLUME_MOUNT_PATH  (Railway volume mount noktası, varsayılan: /data)
 */

import fs from 'node:fs';
import path from 'node:path';
import { createGzip, createGunzip } from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';
import { execSync } from 'node:child_process';

import axios from 'axios';
import mysql from 'mysql2/promise';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ─── Yapılandırma ────────────────────────────────────────────────────────────

const DATA_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH
  ? process.env.RAILWAY_VOLUME_MOUNT_PATH
  : path.resolve(__dirname, '..', 'data');

// Google Drive dosya tanımları (server.js ile aynı linkler)
const SQL_FILES = [
  {
    name: 'za.sql',
    url : 'https://drive.google.com/uc?export=download&id=12GAV9hjm1JwqJYejeFGatqud-88Vsace',
  },
  {
    name: 'zagros.sql',
    url : 'https://drive.google.com/uc?export=download&id=1SUoLWqm-SsbL6tDgdaP-Tc68v6B72vuZ',
  },
  {
    name: 'zagrs.sql',
    url : 'https://drive.google.com/uc?export=download&id=1KmjL89fGLCaeeQv4soJ2SnI7DaZS8qjA',
  },
];

const TXT_FILES = [
  {
    name: 'data.txt',
    url : 'https://drive.google.com/uc?export=download&id=1KltBo15k2VkswKM8flAKPZYij1wbKcWZ',
  },
];

// İndirme zaman aşımı: 30 dakika (büyük dosyalar için)
const DOWNLOAD_TIMEOUT_MS = 30 * 60 * 1000;

// ─── Yardımcı fonksiyonlar ───────────────────────────────────────────────────

function log(tag, msg) {
  console.log(`[${new Date().toISOString()}] [${tag}] ${msg}`);
}

function err(tag, msg, error) {
  console.error(`[${new Date().toISOString()}] [${tag}] HATA: ${msg}`, error?.message ?? '');
}

/** Klasörü oluştur (yoksa) */
function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    log('DIR', `Klasör oluşturuldu: ${dir}`);
  }
}

/** Dosya boyutunu okunabilir formatta döndür */
function humanSize(bytes) {
  if (bytes < 1024)        return `${bytes} B`;
  if (bytes < 1024 ** 2)   return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3)   return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
}

// ─── Google Drive indirme ────────────────────────────────────────────────────

/**
 * Google Drive büyük dosyaları için onay token'ını yakalar ve
 * gerçek indirme URL'sini döndürür.
 */
async function resolveDriveUrl(url) {
  try {
    // İlk istek — Drive bazen onay sayfası döner
    const head = await axios.get(url, {
      maxRedirects: 5,
      timeout: 30_000,
      validateStatus: () => true,
      responseType: 'text',
    });

    // Onay formu var mı? (büyük dosyalar için)
    const match = head.data?.match(/confirm=([0-9A-Za-z_-]+)/);
    if (match) {
      const confirm = match[1];
      const separator = url.includes('?') ? '&' : '?';
      return `${url}${separator}confirm=${confirm}`;
    }
  } catch {
    // Sessizce devam et, orijinal URL'yi kullan
  }
  return url;
}

/**
 * Dosyayı Google Drive'dan indirir.
 * Zaten varsa atlar.
 */
async function downloadFile(name, url, destPath) {
  if (fs.existsSync(destPath)) {
    const size = fs.statSync(destPath).size;
    log('DOWNLOAD', `${name} zaten mevcut (${humanSize(size)}), atlanıyor`);
    return true;
  }

  log('DOWNLOAD', `${name} indiriliyor → ${destPath}`);
  const resolvedUrl = await resolveDriveUrl(url);

  const tmpPath = `${destPath}.tmp`;
  try {
    const response = await axios.get(resolvedUrl, {
      responseType: 'stream',
      timeout: DOWNLOAD_TIMEOUT_MS,
      maxRedirects: 10,
    });

    const writer = fs.createWriteStream(tmpPath);
    let downloaded = 0;
    let lastLog = Date.now();

    response.data.on('data', (chunk) => {
      downloaded += chunk.length;
      const now = Date.now();
      if (now - lastLog > 10_000) {          // Her 10 saniyede bir ilerleme
        log('DOWNLOAD', `${name} — ${humanSize(downloaded)} indirildi`);
        lastLog = now;
      }
    });

    await pipeline(response.data, writer);

    fs.renameSync(tmpPath, destPath);
    const finalSize = fs.statSync(destPath).size;
    log('DOWNLOAD', `${name} tamamlandı (${humanSize(finalSize)})`);
    return true;
  } catch (error) {
    err('DOWNLOAD', `${name} indirilemedi`, error);
    // Yarım kalan geçici dosyayı temizle
    if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
    return false;
  }
}

// ─── Gzip sıkıştırma ─────────────────────────────────────────────────────────

/**
 * Dosyayı gzip ile sıkıştırır.
 * Çıktı: <dosya>.gz
 * Orijinal dosya silinmez.
 */
async function compressFile(srcPath) {
  const gzPath = `${srcPath}.gz`;

  if (fs.existsSync(gzPath)) {
    log('GZIP', `${path.basename(gzPath)} zaten var, atlanıyor`);
    return gzPath;
  }

  log('GZIP', `${path.basename(srcPath)} sıkıştırılıyor…`);
  const src  = fs.createReadStream(srcPath);
  const gzip = createGzip({ level: 6 });
  const dest = fs.createWriteStream(gzPath);

  try {
    await pipeline(src, gzip, dest);
    const origSize = fs.statSync(srcPath).size;
    const gzSize   = fs.statSync(gzPath).size;
    const ratio    = ((1 - gzSize / origSize) * 100).toFixed(1);
    log('GZIP', `${path.basename(gzPath)} hazır — ${humanSize(origSize)} → ${humanSize(gzSize)} (%${ratio} tasarruf)`);
    return gzPath;
  } catch (error) {
    err('GZIP', `${path.basename(srcPath)} sıkıştırılamadı`, error);
    if (fs.existsSync(gzPath)) fs.unlinkSync(gzPath);
    return null;
  }
}

// ─── MySQL bağlantısı ────────────────────────────────────────────────────────

function buildMysqlConfig() {
  // Railway, MYSQL_URL veya ayrı değişkenler sağlayabilir
  if (process.env.MYSQL_URL) {
    return { uri: process.env.MYSQL_URL };
  }

  return {
    host    : process.env.MYSQLHOST     || process.env.DB_HOST     || '127.0.0.1',
    port    : Number(process.env.MYSQLPORT || process.env.DB_PORT  || 3306),
    user    : process.env.MYSQLUSER     || process.env.DB_USER     || 'root',
    password: process.env.MYSQLPASSWORD || process.env.DB_PASSWORD || '',
    database: process.env.MYSQLDATABASE || process.env.DB_NAME     || 'railway',
    multipleStatements: true,
    connectTimeout: 30_000,
  };
}

async function createConnection() {
  const config = buildMysqlConfig();
  log('MYSQL', `Bağlanıyor… (host: ${config.host ?? 'URL ile'})`);
  const conn = await mysql.createConnection(config);
  log('MYSQL', 'Bağlantı başarılı');
  return conn;
}

// ─── SQL yükleme ─────────────────────────────────────────────────────────────

/**
 * SQL dosyasını MySQL'e yükler.
 * Büyük dosyalar için mysql CLI kullanır (bellek dostu).
 */
async function loadSqlFile(sqlPath, conn) {
  const name = path.basename(sqlPath);
  log('MYSQL', `${name} yükleniyor…`);

  // mysql CLI mevcut mu?
  let hasCli = false;
  try {
    execSync('mysql --version', { stdio: 'ignore' });
    hasCli = true;
  } catch { /* CLI yok */ }

  if (hasCli) {
    // CLI yolu: bellek dostu, büyük dosyalar için ideal
    const cfg = buildMysqlConfig();
    const host = cfg.host ?? '127.0.0.1';
    const port = cfg.port ?? 3306;
    const user = cfg.user ?? 'root';
    const pass = cfg.password ?? '';
    const db   = cfg.database ?? 'railway';

    const cmd = `mysql -h ${host} -P ${port} -u ${user} ${pass ? `-p${pass}` : ''} ${db} < "${sqlPath}"`;
    try {
      execSync(cmd, { stdio: ['pipe', 'pipe', 'pipe'], timeout: 60 * 60 * 1000 });
      log('MYSQL', `${name} CLI ile yüklendi`);
      return true;
    } catch (error) {
      err('MYSQL', `${name} CLI yüklemesi başarısız`, error);
      // Fallback: JS yolu
    }
  }

  // JS yolu: dosyayı satır satır oku ve çalıştır
  log('MYSQL', `${name} JS stream ile yükleniyor (bu uzun sürebilir)…`);
  return new Promise((resolve) => {
    const stream = fs.createReadStream(sqlPath, { encoding: 'utf8' });
    let buffer   = '';
    let executed = 0;
    let errors   = 0;

    stream.on('data', async (chunk) => {
      stream.pause();
      buffer += chunk;

      // Noktalı virgülle biten ifadeleri çalıştır
      const statements = buffer.split(/;\s*\n/);
      buffer = statements.pop() ?? '';   // Son parça henüz tamamlanmamış olabilir

      for (const stmt of statements) {
        const trimmed = stmt.trim();
        if (!trimmed || trimmed.startsWith('--') || trimmed.startsWith('/*')) continue;
        try {
          await conn.query(trimmed + ';');
          executed++;
          if (executed % 1000 === 0) log('MYSQL', `${name} — ${executed} sorgu çalıştırıldı`);
        } catch (e) {
          errors++;
          if (errors <= 10) err('MYSQL', `Sorgu hatası (${name})`, e);
        }
      }

      stream.resume();
    });

    stream.on('end', async () => {
      // Kalan buffer'ı işle
      if (buffer.trim()) {
        try {
          await conn.query(buffer.trim());
          executed++;
        } catch (e) {
          errors++;
        }
      }
      log('MYSQL', `${name} tamamlandı — ${executed} sorgu, ${errors} hata`);
      resolve(errors === 0);
    });

    stream.on('error', (e) => {
      err('MYSQL', `${name} okuma hatası`, e);
      resolve(false);
    });
  });
}

// ─── Ana akış ────────────────────────────────────────────────────────────────

async function main() {
  log('MAIN', '=== SQL Yükleme Başlıyor ===');
  ensureDir(DATA_DIR);

  // 1. Dosyaları indir
  log('MAIN', '--- Adım 1: Dosyaları İndir ---');
  const allFiles = [...SQL_FILES, ...TXT_FILES];
  const downloadResults = [];

  for (const { name, url } of allFiles) {
    const destPath = path.join(DATA_DIR, name);
    const ok = await downloadFile(name, url, destPath);
    downloadResults.push({ name, destPath, ok });
  }

  const failed = downloadResults.filter(r => !r.ok);
  if (failed.length > 0) {
    err('MAIN', `${failed.length} dosya indirilemedi: ${failed.map(f => f.name).join(', ')}`);
  }

  // 2. SQL dosyalarını sıkıştır
  log('MAIN', '--- Adım 2: Gzip Sıkıştırma ---');
  for (const { name, destPath, ok } of downloadResults) {
    if (!ok || !name.endsWith('.sql')) continue;
    await compressFile(destPath);
  }

  // 3. MySQL'e yükle
  log('MAIN', '--- Adım 3: MySQL Yükleme ---');

  let conn;
  try {
    conn = await createConnection();
  } catch (error) {
    err('MAIN', 'MySQL bağlantısı kurulamadı — SQL yükleme atlanıyor', error);
    log('MAIN', 'Dosyalar /data klasöründe mevcut, bağlantı sağlandığında tekrar çalıştırın.');
    process.exit(1);
  }

  let loadOk = 0;
  let loadFail = 0;

  for (const { name, destPath, ok } of downloadResults) {
    if (!ok || !name.endsWith('.sql')) continue;
    const success = await loadSqlFile(destPath, conn);
    if (success) loadOk++;
    else loadFail++;
  }

  await conn.end();

  log('MAIN', '=== Tamamlandı ===');
  log('MAIN', `SQL yükleme: ${loadOk} başarılı, ${loadFail} başarısız`);

  if (loadFail > 0) process.exit(1);
}

main().catch((error) => {
  err('MAIN', 'Beklenmeyen hata', error);
  process.exit(1);
});
