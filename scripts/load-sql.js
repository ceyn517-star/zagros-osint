/**
 * scripts/load-sql.js
 *
 * Google Drive'dan SQL dosyalarını indirir, gzip ile sıkıştırır,
 * ardından MySQL'e yükler.
 *
 * Kullanım:
 *   node scripts/load-sql.js
 *
 * Gerekli ortam değişkenleri:
 *   MYSQL_URL  veya  MYSQLHOST, MYSQLPORT, MYSQLUSER, MYSQLPASSWORD, MYSQLDATABASE
 *   RAILWAY_VOLUME_MOUNT_PATH  (Railway volume yolu, yoksa proje kökü kullanılır)
 */

import fs from 'node:fs';
import path from 'node:path';
import { createGzip, createGunzip } from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

import axios from 'axios';
import mysql from 'mysql2/promise';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ─── Dizin yapılandırması ────────────────────────────────────────────────────
const DATA_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH
  ? process.env.RAILWAY_VOLUME_MOUNT_PATH
  : path.resolve(__dirname, '..', 'data');

fs.mkdirSync(DATA_DIR, { recursive: true });

// ─── İndirilecek dosyalar (server.js ile aynı Drive linkleri) ───────────────
const SQL_FILES = [
  {
    name: 'za.sql',
    url:  'https://drive.google.com/uc?export=download&id=12GAV9hjm1JwqJYejeFGatqud-88Vsace',
  },
  {
    name: 'zagros.sql',
    url:  'https://drive.google.com/uc?export=download&id=1SUoLWqm-SsbL6tDgdaP-Tc68v6B72vuZ',
  },
  {
    name: 'zagrs.sql',
    url:  'https://drive.google.com/uc?export=download&id=1KmjL89fGLCaeeQv4soJ2SnI7DaZS8qjA',
  },
];

const TXT_FILES = [
  {
    name: 'data.txt',
    url:  'https://drive.google.com/uc?export=download&id=1KltBo15k2VkswKM8flAKPZYij1wbKcWZ',
  },
];

// ─── Yardımcı: boyutu okunabilir formata çevir ──────────────────────────────
function humanSize(bytes) {
  if (bytes < 1024)       return `${bytes} B`;
  if (bytes < 1024 ** 2)  return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3)  return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
}

// ─── Google Drive büyük dosya indirme (confirm token desteği) ───────────────
async function downloadFile(url, destPath, label) {
  console.log(`[Download] ⬇  ${label} indiriliyor → ${destPath}`);

  // İlk istek – Drive bazen onay sayfası döner
  let response = await axios.get(url, {
    responseType:  'stream',
    timeout:       600_000, // 10 dakika
    maxRedirects:  10,
  });

  // Drive "virus scan warning" sayfasını geç
  const contentType = response.headers['content-type'] || '';
  if (contentType.includes('text/html')) {
    // Confirm token'ı cookie'den veya body'den al
    let confirmToken = null;
    const setCookie = response.headers['set-cookie'] || [];
    for (const c of setCookie) {
      const m = c.match(/download_warning[^=]*=([^;]+)/);
      if (m) { confirmToken = m[1]; break; }
    }

    if (confirmToken) {
      const confirmUrl = `${url}&confirm=${confirmToken}`;
      console.log(`[Download] 🔑 Onay token'ı alındı, yeniden deneniyor…`);
      response = await axios.get(confirmUrl, {
        responseType: 'stream',
        timeout:      600_000,
        maxRedirects: 10,
      });
    } else {
      // Alternatif: export=download yerine uc?id= formatı dene
      const idMatch = url.match(/id=([^&]+)/);
      if (idMatch) {
        const altUrl = `https://drive.google.com/uc?export=download&confirm=t&id=${idMatch[1]}`;
        console.log(`[Download] 🔄 Alternatif URL deneniyor…`);
        response = await axios.get(altUrl, {
          responseType: 'stream',
          timeout:      600_000,
          maxRedirects: 10,
        });
      }
    }
  }

  const writer = fs.createWriteStream(destPath);
  let downloaded = 0;
  const total = parseInt(response.headers['content-length'] || '0', 10);

  response.data.on('data', (chunk) => {
    downloaded += chunk.length;
    if (total > 0) {
      const pct = ((downloaded / total) * 100).toFixed(1);
      process.stdout.write(`\r[Download] ${label}: ${humanSize(downloaded)} / ${humanSize(total)} (${pct}%)`);
    } else {
      process.stdout.write(`\r[Download] ${label}: ${humanSize(downloaded)}`);
    }
  });

  await pipeline(response.data, writer);
  process.stdout.write('\n');
  console.log(`[Download] ✅ ${label} tamamlandı (${humanSize(downloaded)})`);
}

// ─── Gzip sıkıştırma ────────────────────────────────────────────────────────
async function compressFile(srcPath, gzPath, label) {
  console.log(`[Compress] 🗜  ${label} sıkıştırılıyor → ${gzPath}`);
  const src  = fs.createReadStream(srcPath);
  const gzip = createGzip({ level: 6 });
  const dest = fs.createWriteStream(gzPath);
  await pipeline(src, gzip, dest);
  const origSize = fs.statSync(srcPath).size;
  const gzSize   = fs.statSync(gzPath).size;
  const ratio    = ((1 - gzSize / origSize) * 100).toFixed(1);
  console.log(`[Compress] ✅ ${label}: ${humanSize(origSize)} → ${humanSize(gzSize)} (%${ratio} küçüldü)`);
}

// ─── MySQL bağlantısı oluştur ────────────────────────────────────────────────
function buildMysqlConfig() {
  // Railway otomatik olarak MYSQL_URL veya ayrı değişkenler sağlar
  if (process.env.MYSQL_URL) {
    return { uri: process.env.MYSQL_URL, multipleStatements: true };
  }

  return {
    host:               process.env.MYSQLHOST     || process.env.DB_HOST     || '127.0.0.1',
    port:               parseInt(process.env.MYSQLPORT || process.env.DB_PORT || '3306', 10),
    user:               process.env.MYSQLUSER     || process.env.DB_USER     || 'root',
    password:           process.env.MYSQLPASSWORD || process.env.DB_PASSWORD || '',
    database:           process.env.MYSQLDATABASE || process.env.DB_NAME     || 'zagros',
    multipleStatements: true,
    connectTimeout:     30_000,
  };
}

// ─── SQL dosyasını MySQL'e yükle (stream ile, bellek dostu) ─────────────────
async function loadSqlFile(conn, filePath, label) {
  console.log(`[MySQL] 📂 ${label} yükleniyor…`);

  const fileSize = fs.statSync(filePath).size;
  const isGzip   = filePath.endsWith('.gz');

  // Dosyayı satır satır oku, statement'ları biriktir
  const { createInterface } = await import('node:readline');

  let readStream = fs.createReadStream(filePath);
  if (isGzip) readStream = readStream.pipe(createGunzip());

  const rl = createInterface({ input: readStream, crlfDelay: Infinity });

  let buffer      = '';
  let stmtCount   = 0;
  let errorCount  = 0;
  let bytesRead   = 0;

  for await (const line of rl) {
    bytesRead += Buffer.byteLength(line, 'utf8') + 1;

    // Yorum satırlarını ve boş satırları atla
    const trimmed = line.trim();
    if (
      trimmed === '' ||
      trimmed.startsWith('--') ||
      trimmed.startsWith('/*') ||
      trimmed.startsWith('#')
    ) continue;

    buffer += line + '\n';

    // Noktalı virgülle biten tam statement'ı çalıştır
    if (trimmed.endsWith(';')) {
      try {
        await conn.query(buffer);
        stmtCount++;
        if (stmtCount % 500 === 0) {
          const pct = fileSize > 0 ? ((bytesRead / fileSize) * 100).toFixed(1) : '?';
          console.log(`[MySQL] ${label}: ${stmtCount} statement çalıştırıldı (%${pct})`);
        }
      } catch (err) {
        errorCount++;
        if (errorCount <= 10) {
          console.warn(`[MySQL] ⚠  ${label} statement hatası: ${err.message}`);
        } else if (errorCount === 11) {
          console.warn(`[MySQL] ⚠  ${label}: çok fazla hata, bundan sonrası loglanmayacak`);
        }
      }
      buffer = '';
    }
  }

  // Kalan buffer varsa çalıştır
  if (buffer.trim()) {
    try {
      await conn.query(buffer);
      stmtCount++;
    } catch (err) {
      errorCount++;
      console.warn(`[MySQL] ⚠  ${label} son statement hatası: ${err.message}`);
    }
  }

  console.log(`[MySQL] ✅ ${label} tamamlandı — ${stmtCount} statement, ${errorCount} hata`);
  return { stmtCount, errorCount };
}

// ─── Ana akış ────────────────────────────────────────────────────────────────
async function main() {
  console.log('═══════════════════════════════════════════════════════');
  console.log('  Zagros SQL Loader — Railway Auto-Import');
  console.log(`  DATA_DIR : ${DATA_DIR}`);
  console.log('═══════════════════════════════════════════════════════\n');

  // 1. Dosyaları indir
  const allFiles = [...SQL_FILES, ...TXT_FILES];
  for (const { name, url } of allFiles) {
    const destPath = path.join(DATA_DIR, name);
    if (fs.existsSync(destPath)) {
      console.log(`[Download] ⏭  ${name} zaten mevcut, atlanıyor`);
      continue;
    }
    try {
      await downloadFile(url, destPath, name);
    } catch (err) {
      console.error(`[Download] ❌ ${name} indirilemedi: ${err.message}`);
    }
  }

  // 2. SQL dosyalarını gzip ile sıkıştır (orijinal dosya silinmez)
  for (const { name } of SQL_FILES) {
    const srcPath = path.join(DATA_DIR, name);
    const gzPath  = `${srcPath}.gz`;

    if (!fs.existsSync(srcPath)) {
      console.log(`[Compress] ⏭  ${name} bulunamadı, atlanıyor`);
      continue;
    }
    if (fs.existsSync(gzPath)) {
      console.log(`[Compress] ⏭  ${name}.gz zaten mevcut, atlanıyor`);
      continue;
    }

    try {
      await compressFile(srcPath, gzPath, name);
    } catch (err) {
      console.error(`[Compress] ❌ ${name} sıkıştırılamadı: ${err.message}`);
    }
  }

  // 3. MySQL'e bağlan ve SQL'leri yükle
  const sqlFilesToLoad = SQL_FILES
    .map(({ name }) => path.join(DATA_DIR, name))
    .filter((p) => fs.existsSync(p));

  if (sqlFilesToLoad.length === 0) {
    console.error('[MySQL] ❌ Yüklenecek SQL dosyası bulunamadı. Çıkılıyor.');
    process.exit(1);
  }

  const cfg = buildMysqlConfig();
  let conn;
  try {
    console.log('\n[MySQL] 🔌 Bağlanılıyor…');
    conn = cfg.uri
      ? await mysql.createConnection(cfg.uri)
      : await mysql.createConnection(cfg);
    console.log('[MySQL] ✅ Bağlantı kuruldu');
  } catch (err) {
    console.error(`[MySQL] ❌ Bağlantı hatası: ${err.message}`);
    process.exit(1);
  }

  let totalStmts  = 0;
  let totalErrors = 0;

  for (const filePath of sqlFilesToLoad) {
    const label = path.basename(filePath);
    try {
      const { stmtCount, errorCount } = await loadSqlFile(conn, filePath, label);
      totalStmts  += stmtCount;
      totalErrors += errorCount;
    } catch (err) {
      console.error(`[MySQL] ❌ ${label} yüklenirken kritik hata: ${err.message}`);
    }
  }

  await conn.end();

  console.log('\n═══════════════════════════════════════════════════════');
  console.log(`  Tamamlandı — Toplam ${totalStmts} statement, ${totalErrors} hata`);
  console.log('═══════════════════════════════════════════════════════');

  if (totalErrors > 0) process.exit(1);
}

main().catch((err) => {
  console.error('[Fatal]', err);
  process.exit(1);
});
