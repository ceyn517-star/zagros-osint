/**
 * scripts/decompress.js
 *
 * /data klasöründeki .gz dosyalarını decompress eder.
 * Belirli bir dosya adı argüman olarak verilebilir;
 * verilmezse /data içindeki tüm .gz dosyaları işlenir.
 *
 * Kullanım:
 *   node scripts/decompress.js                    # tüm .gz dosyaları
 *   node scripts/decompress.js za.sql.gz          # tek dosya
 *   node scripts/decompress.js /data/zagros.sql.gz  # tam yol
 */

import fs from 'node:fs';
import path from 'node:path';
import { createGunzip } from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ─── Yapılandırma ────────────────────────────────────────────────────────────

const DATA_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH
  ? process.env.RAILWAY_VOLUME_MOUNT_PATH
  : path.resolve(__dirname, '..', 'data');

// ─── Yardımcı fonksiyonlar ───────────────────────────────────────────────────

function log(tag, msg) {
  console.log(`[${new Date().toISOString()}] [${tag}] ${msg}`);
}

function err(tag, msg, error) {
  console.error(`[${new Date().toISOString()}] [${tag}] HATA: ${msg}`, error?.message ?? '');
}

function humanSize(bytes) {
  if (bytes < 1024)        return `${bytes} B`;
  if (bytes < 1024 ** 2)   return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3)   return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
}

// ─── Decompress ──────────────────────────────────────────────────────────────

/**
 * Tek bir .gz dosyasını decompress eder.
 * Çıktı dosyası aynı klasörde, .gz uzantısı kaldırılmış şekilde oluşturulur.
 *
 * @param {string} gzPath  - .gz dosyasının tam yolu
 * @returns {Promise<boolean>} - başarı durumu
 */
async function decompressFile(gzPath) {
  if (!gzPath.endsWith('.gz')) {
    err('DECOMPRESS', `${path.basename(gzPath)} .gz uzantılı değil, atlanıyor`);
    return false;
  }

  const outPath = gzPath.slice(0, -3); // .gz kaldır
  const name    = path.basename(gzPath);

  if (fs.existsSync(outPath)) {
    const size = fs.statSync(outPath).size;
    log('DECOMPRESS', `${path.basename(outPath)} zaten mevcut (${humanSize(size)}), atlanıyor`);
    return true;
  }

  if (!fs.existsSync(gzPath)) {
    err('DECOMPRESS', `Dosya bulunamadı: ${gzPath}`);
    return false;
  }

  const gzSize = fs.statSync(gzPath).size;
  log('DECOMPRESS', `${name} açılıyor (${humanSize(gzSize)})…`);

  const tmpPath = `${outPath}.tmp`;
  try {
    const src    = fs.createReadStream(gzPath);
    const gunzip = createGunzip();
    const dest   = fs.createWriteStream(tmpPath);

    await pipeline(src, gunzip, dest);

    fs.renameSync(tmpPath, outPath);

    const outSize = fs.statSync(outPath).size;
    log('DECOMPRESS', `${path.basename(outPath)} hazır (${humanSize(outSize)})`);
    return true;
  } catch (error) {
    err('DECOMPRESS', `${name} açılamadı`, error);
    if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
    return false;
  }
}

/**
 * /data klasöründeki tüm .gz dosyalarını listeler.
 */
function findGzFiles(dir) {
  try {
    return fs.readdirSync(dir)
      .filter(f => f.endsWith('.gz'))
      .map(f => path.join(dir, f));
  } catch (error) {
    err('DECOMPRESS', `${dir} okunamadı`, error);
    return [];
  }
}

// ─── Ana akış ────────────────────────────────────────────────────────────────

async function main() {
  log('MAIN', '=== Decompress Başlıyor ===');
  log('MAIN', `Veri klasörü: ${DATA_DIR}`);

  // Argüman verilmişse sadece o dosyayı işle
  const args = process.argv.slice(2);
  let targets = [];

  if (args.length > 0) {
    targets = args.map(arg => {
      // Tam yol mu yoksa sadece dosya adı mı?
      return path.isAbsolute(arg) ? arg : path.join(DATA_DIR, arg);
    });
    log('MAIN', `Hedef dosyalar: ${targets.map(t => path.basename(t)).join(', ')}`);
  } else {
    targets = findGzFiles(DATA_DIR);
    if (targets.length === 0) {
      log('MAIN', `${DATA_DIR} içinde .gz dosyası bulunamadı`);
      return;
    }
    log('MAIN', `${targets.length} .gz dosyası bulundu`);
  }

  let ok   = 0;
  let fail = 0;

  for (const gzPath of targets) {
    const success = await decompressFile(gzPath);
    if (success) ok++;
    else fail++;
  }

  log('MAIN', '=== Tamamlandı ===');
  log('MAIN', `${ok} başarılı, ${fail} başarısız`);

  if (fail > 0) process.exit(1);
}

main().catch((error) => {
  err('MAIN', 'Beklenmeyen hata', error);
  process.exit(1);
});
